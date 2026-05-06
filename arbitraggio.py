import asyncio
import json
from decimal import Decimal, getcontext
from itertools import permutations
from datetime import datetime
import requests
import aiohttp
import os
import time
from concurrent.futures import ProcessPoolExecutor
import logging
from math import ceil
import concurrent.futures

psutil_available = False # Forcibly disabled

# Import the new modules for automated trading
import config
from trading_executor import trading_worker_with_affinity

# --- Logging Configuration ---
# Remove default handlers to avoid duplicate logs
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Configure the main logger
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.StreamHandler()],
                    encoding='utf-8')
logger = logging.getLogger(__name__)

# --- Configuration Constants ---
SYMBOLS_PER_CONNECTION = 200  # Number of symbols per WebSocket connection
TRADING_FEE = Decimal("0.00075")      # Fee per trade (0.075% with BNB discount)
STARTING_ASSETS = {'USDT', 'USDC', 'FDUSD', 'DAI', 'TUSD', 'BTC', 'ETH', 'SOL'} # Starting assets for arbitrage analysis
OPPORTUNITY_COOLDOWN = 60  # Seconds before re-notifying the same triangle

# --- Log Files ---
PROFITS_FILE = "profitable_opportunities.txt"
ANOMALIES_FILE = "anomalies.txt"

# --- Global Variables ---
prices_cache = {}
symbol_info_map = {}
last_check_time = datetime.now()
profitable_opportunities_set = {}
total_profitable_opportunities_found = 0
total_low_profit_positive_found = 0

# Telegram configuration (loaded from environment variables or file)
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

BUFFER_SICUREZZA = 0.8  # 80% of available quantity

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] {msg}")

getcontext().prec = 28  # H1 fix: was 12 but main() reset to 15 and worker process default 28 → inconsistency

# Binance WebSocket URL
WS_URL = "wss://stream.binance.com:9443/stream"

# Real-time price cache
price_map = {}
msg_count = 0  # Global WebSocket message counter

# Function to send Telegram message
def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text,
        'parse_mode': 'Markdown'  # Enable Markdown formatting
    }
    try:
        response = requests.post(url, data=payload, timeout=5)
        if response.status_code == 200:
            log("[TELEGRAM] Notification sent successfully.")
        else:
            log(f"[TELEGRAM][ERROR] Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        log(f"[TELEGRAM][ERROR] {e}")

# Function to write opportunities to daily file
def save_opportunity_to_file(opp):
    today = datetime.now().strftime('%Y%m%d')
    filename = f"arbitrage_{today}.txt"

    # Convert string values to Decimal before formatting
    profit_dec = Decimal(opp['profit'])
    final_dec = Decimal(opp['final'])
    profit_usdt = profit_dec * Decimal('100')

    # Add the note
    note = opp.get('note', '')

    line = (f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} | "
            f"{opp['path']} | "
            f"Profit: {profit_dec:.6f} | "
            f"Final: {final_dec:.6f} | "
            f"USDT Gain (on 100): {profit_usdt:.4f} USDT | "
            f"NOTE: {note}\n")

    with open(filename, 'a', encoding='utf-8') as f:
        f.write(line)
    # Reduce logging to avoid console clutter
    # log(f"[FILE] Opportunity saved to {filename}")

def save_profitable_opportunity(opp):
    """Saves only profitable opportunities to a dedicated file."""
    filename = "profitable_opportunities.txt"

    # Convert profit from string to Decimal
    profit_dec = Decimal(opp['profit'])
    profit_usdt = profit_dec * Decimal('100')

    line = (f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} | "
            f"{opp['path']} | "
            f"Net Profit: {(profit_dec*100):.4f}% | "
            f"Estimated Gain (100 USDT): {profit_usdt:.4f} USDT\n")
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(line)
    log(f"[FILE] PROFITABLE opportunity saved to {filename}")

async def monitor_performance(process):
    """Monitors and logs system performance every 15 seconds (reduced from 5)."""
    global msg_count
    log("[LOG] Starting optimized performance monitoring...")
    while True:
        await asyncio.sleep(15)  # Increased from 5 to 15 seconds to reduce load

        total_cpu = 0
        total_ram = 0

        if psutil_available:
            try:
                # CPU and RAM of main process (simplified)
                main_cpu = process.cpu_percent(interval=0.1)  # Minimum interval for accuracy
                main_ram = process.memory_info().rss / (1024 * 1024)
                total_cpu += main_cpu
                total_ram += main_ram

                # Monitor only active child processes (reduced load)
                children = process.children(recursive=True)
                active_children = 0
                for child in children:
                    try:
                        if child.status() == psutil.STATUS_RUNNING:
                            child_cpu = child.cpu_percent(interval=0.1)
                            child_ram = child.memory_info().rss / (1024 * 1024)
                            total_cpu += child_cpu
                            total_ram += child_ram
                            active_children += 1
                    except psutil.NoSuchProcess:
                        continue

            except psutil.NoSuchProcess:
                log("[PERF][WARNING] Main process not found for monitoring.")
                continue

        # Safe copy to avoid race condition
        current_price_map = price_map.copy()

        cpu_display = f"{total_cpu:.1f}%" if psutil_available else "N/A"
        ram_display = f"{total_ram:.2f} MB" if psutil_available else "N/A"

        msgs = msg_count
        msg_count = 0  # reset for next cycle
        msg_rate = msgs/15  # Calculated over 15 seconds

        # Log only if significant activity
        if msgs > 0 or total_cpu > 10:
            log(f"[PERF] CPU: {cpu_display} | RAM: {ram_display} | Cache: {len(current_price_map)} | Msg/s: {msg_rate:.1f}")
        else:
            log(f"[PERF] CPU: {cpu_display} | RAM: {ram_display} | Cache: {len(current_price_map)} | State: Idle")

async def get_exchange_symbols():
    """Gets the symbols and their info, focusing on pairs related to the starting assets."""
    try:
        url = "https://api.binance.com/api/v3/exchangeInfo"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        trading_symbols = {s['symbol']: s for s in data['symbols'] if s['status'] == 'TRADING'}

        # Filter for currencies that have a direct pair with the starting assets to limit the field
        relevant_currencies = set(STARTING_ASSETS)
        for symbol, info in trading_symbols.items():
            if info['quoteAsset'] in STARTING_ASSETS:
                relevant_currencies.add(info['baseAsset'])
            if info['baseAsset'] in STARTING_ASSETS:
                relevant_currencies.add(info['quoteAsset'])

        symbols_to_subscribe = set()
        temp_symbol_info_map = {}
        for symbol, info in trading_symbols.items():
            if info['baseAsset'] in relevant_currencies and info['quoteAsset'] in relevant_currencies:
                symbols_to_subscribe.add(symbol)

                min_qty, min_notional, step_size = Decimal("0"), Decimal("0"), Decimal("0")
                for f in info['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        min_qty = Decimal(f['minQty'])
                        step_size = Decimal(f['stepSize'])
                    elif f['filterType'] == 'NOTIONAL' or f['filterType'] == 'MIN_NOTIONAL':
                        min_notional = Decimal(f.get('notional', f.get('minNotional', "0")))

                temp_symbol_info_map[symbol] = {
                    'base': info['baseAsset'], 'quote': info['quoteAsset'],
                    'minQty': min_qty, 'minNotional': min_notional, 'stepSize': step_size
                }

        formatted_symbols = [s.lower() + "@bookTicker" for s in sorted(list(symbols_to_subscribe))]
        logger.info(f"Obtained {len(formatted_symbols)} symbols for arbitrage (related to {', '.join(sorted(list(STARTING_ASSETS)))}).")
        return formatted_symbols, temp_symbol_info_map
    except Exception as e:
        logger.error(f"Unable to obtain symbols: {e}")
        return [], {}

async def handle_message(msg):
    global msg_count
    msg_count += 1

    # Combined stream format handling
    data = json.loads(msg)
    if 'data' in data:  # Combined stream
        data = data['data']

    symbol = data['s']
    prices_cache[symbol] = {
        'bid': Decimal(data['b']),
        'ask': Decimal(data['a']),
        'bid_qty': Decimal(data['B']),
        'ask_qty': Decimal(data['A'])
    }

def format_opportunity_message(opp, prices):
    """Formats an arbitrage opportunity into a readable Telegram message."""
    try:
        path = opp['path']
        steps = path.split('→')

        # Add a robustness check for invalid paths
        if len(steps) < 4:
            log(f"[MSG][WARN] Received invalid or incomplete path: '{path}'")
            return f"⚠️ *Anomalous data*\n\nPath: `{path}`. Cannot generate a valid example."

        # Safely convert received data (may be strings)
        profit = Decimal(str(opp['profit']))
        profit_percentage = profit * 100

        details = opp.get('details', {})
        if not details:
            return f"⚠️ *INCOMPLETE DATA*\n\nPath: `{path}`. Cannot generate example."

        # Safely convert details
        rates = tuple(Decimal(str(r)) for r in details['rates'])
        (rate1, rate2, rate3) = rates

        pairs = tuple(str(p) for p in details['pairs'])
        (pair1_str, pair2_str, pair3_str) = pairs

        prices_details = tuple(Decimal(str(p)) for p in details['prices'])
        (price_val1, price_val2, price_val3) = prices_details

        # --- SIMPLIFIED AND CORRECT LOGIC ---
        investimento_usdt = config.SIMULATION_BUDGET_USDT
        guadagno_usdt = investimento_usdt * profit
        finale_usdt = investimento_usdt + guadagno_usdt
        commissioni_usdt = investimento_usdt * (1 - (1 - TRADING_FEE)**3)

        message = f"⚡ *ARBITRAGE OPPORTUNITY*\n\n" \
                    f"🔄 *Path:* `{path}`\n" \
                    f"💰 *Estimated Net Profit:* `{profit_percentage:.4f}%`\n\n" \
                    f"💵 *Example on {investimento_usdt} USDT:*\n" \
                    f"• Investment: `{investimento_usdt:.2f} USDT`\n" \
                    f"• Estimated Final: `{finale_usdt:.4f} USDT`\n" \
                    f"• Net Gain: `{guadagno_usdt:.4f} USDT`\n" \
                    f"• Estimated Fees: `{commissioni_usdt:.4f} USDT`\n\n" \
                    f"📈 *Operations and Prices (used in calculation):*\n" \
                    f"1. `{steps[0]}→{steps[1]}` (`{pair1_str}` @ `{price_val1:.8f}`)\n" \
                    f"2. `{steps[1]}→{steps[2]}` (`{pair2_str}` @ `{price_val2:.8f}`)\n" \
                    f"3. `{steps[2]}→{steps[0]}` (`{pair3_str}` @ `{price_val3:.8f}`)\n\n" \
                    f"⏰ *Timestamp:* `{datetime.now().strftime('%H:%M:%S')}`"
        return message

    except Exception as e:
        log(f"[MSG][ERR] Critical formatting error: {e} for opp: {opp}")
        return f"🚨 Error formatting message for `{opp.get('path', 'N/A')}`"

def adjust_quantity_for_step_size(quantity, step_size):
    """Rounds down the quantity to comply with Binance stepSize."""
    if step_size > 0:
        return (quantity // step_size) * step_size
    return quantity

def get_budget_in_asset(start_asset, budget_usdt, prices, existing_pairs):
    """C2 fix: converts USDT budget to units of start_asset using current prices.

    Resolves the unit-mismatch bug: the simulation always started with
    SIMULATION_BUDGET_USDT (e.g. 22 USDT) as quantity of start_asset, even
    when start_asset was BTC/ETH/SOL. Result: profit calculated as
    difference of different units, systemic false negatives.

    Returns None if there is no way to price the asset in USDT/USDC/FDUSD.
    """
    if start_asset == 'USDT':
        return budget_usdt
    # Direction 1: pair <start_asset>/<stable> where start is base.
    # Bid price = how much stable you receive for 1 start_asset → budget_usdt / bid = start units.
    for stable in ('USDT', 'USDC', 'FDUSD'):
        if start_asset in existing_pairs and stable in existing_pairs[start_asset]:
            symbol = existing_pairs[start_asset][stable]
            book = prices.get(symbol)
            if book and book.get('bid') and book['bid'] > 0:
                return budget_usdt / book['bid']
    # Direction 2 (rare): pair <stable>/<start_asset> where start is quote.
    # Ask price = how much stable needed for 1 unit of other asset → budget_usdt / ask = start units.
    for stable in ('USDT', 'USDC', 'FDUSD'):
        if stable in existing_pairs and start_asset in existing_pairs[stable]:
            symbol = existing_pairs[stable][start_asset]
            book = prices.get(symbol)
            if book and book.get('ask') and book['ask'] > 0:
                return budget_usdt / book['ask']
    return None

def find_arbitrage_worker(prices, symbol_info_map_local, profit_threshold, trading_fee, currency_chunk, all_currencies, trade_graph):
    """Worker process that searches for arbitrage opportunities by navigating a pre-computed graph."""
    # H1 fix: ensures prec=28 also in worker processes (for explicit clarity).
    getcontext().prec = 28
    worker_pid = os.getpid()
    print(f"[WORKER][{worker_pid}] Starting analysis for {len(currency_chunk)} currencies")

    profitable_opportunities = []
    stats = {
        'total_triangles': 0,
        'non_priority_start': 0,
        'cannot_convert_budget': 0,
        'low_profit': {'negative': 0, 'positive': 0},
        'simulation_failures': {
            'total': 0, 'FAIL_NO_DATA': 0, 'FAIL_STEP_SIZE': 0,
            'FAIL_MIN_QTY': 0, 'FAIL_LIQUIDITY': 0, 'FAIL_MIN_NOTIONAL': 0,
            'UNKNOWN': 0
        }
    }

    existing_pairs = {c: {} for c in all_currencies}
    for symbol, info in symbol_info_map_local.items():
        base, quote = info['base'], info['quote']
        if base not in existing_pairs: existing_pairs[base] = {}
        existing_pairs[base][quote] = symbol

    # Navigate the graph to find only valid paths
    for p_a in currency_chunk:
        if p_a not in trade_graph: continue

        for p_b in trade_graph[p_a]:
            if p_b not in trade_graph: continue
            for p_c in trade_graph[p_b]:
                if p_c == p_a: continue

                if p_c in trade_graph and p_a in trade_graph[p_c]:
                    stats['total_triangles'] += 1

                    if p_a not in STARTING_ASSETS:
                        stats['non_priority_start'] += 1
                        continue

                    try:
                        # C2 fix: convert USDT budget to units of start_asset using current prices.
                        # Previously 22 USDT was always passed as "quantity" of start_asset, wrong for BTC/ETH/SOL.
                        budget_in_asset = get_budget_in_asset(p_a, config.SIMULATION_BUDGET_USDT, prices, existing_pairs)
                        if budget_in_asset is None or budget_in_asset <= 0:
                            stats['cannot_convert_budget'] += 1
                            continue

                        status, result = simulate_trade(p_a, p_b, budget_in_asset, prices, symbol_info_map_local, existing_pairs)
                        if status != 'SUCCESS':
                            stats['simulation_failures']['total'] += 1
                            stats['simulation_failures'][status] = stats['simulation_failures'].get(status, 0) + 1
                            continue
                        rate1, amount1, pair1_str = result

                        amount1_after_fee = amount1 * (1 - trading_fee)

                        status, result = simulate_trade(p_b, p_c, amount1_after_fee, prices, symbol_info_map_local, existing_pairs)
                        if status != 'SUCCESS':
                            stats['simulation_failures']['total'] += 1
                            stats['simulation_failures'][status] = stats['simulation_failures'].get(status, 0) + 1
                            continue
                        rate2, amount2, pair2_str = result

                        amount2_after_fee = amount2 * (1 - trading_fee)

                        status, result = simulate_trade(p_c, p_a, amount2_after_fee, prices, symbol_info_map_local, existing_pairs)
                        if status != 'SUCCESS':
                            stats['simulation_failures']['total'] += 1
                            stats['simulation_failures'][status] = stats['simulation_failures'].get(status, 0) + 1
                            continue
                        rate3, amount3, pair3_str = result

                        final_amount = amount3 * (1 - trading_fee)
                        # Dimensionless profit ratio = (final - budget) / budget. Directly comparable to threshold.
                        profit_ratio = (final_amount - budget_in_asset) / budget_in_asset

                        if profit_ratio > profit_threshold:
                             profit_perc = profit_ratio * 100
                             profitable_opportunities.append({
                                'path': f"{p_a}→{p_b}→{p_c}→{p_a}",
                                'profit_perc': f"{profit_perc:.4f}",
                                'pairs': [pair1_str, pair2_str, pair3_str],
                                'details': {
                                    'pairs': (pair1_str, pair2_str, pair3_str),
                                    'rates': (str(rate1), str(rate2), str(rate3)),
                                    'prices': (str(prices.get(pair1_str,{}).get('ask' if p_a==symbol_info_map_local[pair1_str]['quote'] else 'bid')),
                                               str(prices.get(pair2_str,{}).get('ask' if p_b==symbol_info_map_local[pair2_str]['quote'] else 'bid')),
                                               str(prices.get(pair3_str,{}).get('ask' if p_c==symbol_info_map_local[pair3_str]['quote'] else 'bid')))
                                }
                            })
                        else:
                            if profit_ratio < 0:
                                stats['low_profit']['negative'] += 1
                            else:
                                stats['low_profit']['positive'] += 1

                    except Exception:
                        stats['simulation_failures']['total'] += 1
                        stats['simulation_failures']['UNKNOWN'] += 1
                        continue

    print(f"[WORKER][{worker_pid}] Analysis end: {stats['total_triangles']} triangles, {len(profitable_opportunities)} opportunities")
    return {'profitable': profitable_opportunities, 'stats': stats}

def simulate_trade(start_asset, end_asset, amount_in, prices, symbol_info, existing_pairs):
    """
    Simulates a single trade.
    Returns ('SUCCESS', (rate, amount_out, symbol)) or ('FAIL_REASON', None).
    """
    # Buy end_asset with start_asset (pair: end_asset/start_asset)
    if end_asset in existing_pairs and start_asset in existing_pairs[end_asset]:
        symbol = existing_pairs[end_asset][start_asset]
        info = symbol_info.get(symbol)
        book = prices.get(symbol)
        if not info or not book or book['ask'] == 0: return 'FAIL_NO_DATA', None

        price = book['ask']
        quantity_to_buy = adjust_quantity_for_step_size(amount_in / price, info['stepSize'])
        if quantity_to_buy == 0: return 'FAIL_STEP_SIZE', None

        notional_value = quantity_to_buy * price
        if quantity_to_buy < info['minQty']: return 'FAIL_MIN_QTY', None
        if quantity_to_buy > book['ask_qty']: return 'FAIL_LIQUIDITY', None
        if notional_value < info['minNotional']: return 'FAIL_MIN_NOTIONAL', None

        return 'SUCCESS', (Decimal(1) / price, quantity_to_buy, symbol)

    # Sell start_asset for end_asset (pair: start_asset/end_asset)
    elif start_asset in existing_pairs and end_asset in existing_pairs[start_asset]:
        symbol = existing_pairs[start_asset][end_asset]
        info = symbol_info.get(symbol)
        book = prices.get(symbol)
        if not info or not book or book['bid'] == 0: return 'FAIL_NO_DATA', None

        price = book['bid']
        quantity_to_sell = adjust_quantity_for_step_size(amount_in, info['stepSize'])
        if quantity_to_sell == 0: return 'FAIL_STEP_SIZE', None

        notional_value = quantity_to_sell * price
        if quantity_to_sell < info['minQty']: return 'FAIL_MIN_QTY', None
        if quantity_to_sell > book['bid_qty']: return 'FAIL_LIQUIDITY', None
        if notional_value < info['minNotional']: return 'FAIL_MIN_NOTIONAL', None

        return 'SUCCESS', (price, notional_value, symbol)

    return 'FAIL_NO_DATA', None # If the pair does not exist in any direction

def cpu_stress_test_worker(iterations):
    print(f"[STRESS][WORKER] PID: {os.getpid()} | Iterations: {iterations}")
    x = 0
    for i in range(iterations):
        x += i
    print(f"[STRESS][WORKER] PID: {os.getpid()} | Work end")
    return x

async def handle_trading_result(future):
    """Handles the asynchronous trading result"""
    try:
        # 30 second timeout for execution
        result = await asyncio.wait_for(future, timeout=config.TRADING_TIMEOUT)
        log(f"Trading completed: {result.get('status', 'Unknown')}")

        if result.get('status') == 'SUCCESS':
            profit_pct = result.get('profit_percentage', 0)
            log(f"✅ Profitable arbitrage: {profit_pct:.4f}%")
        elif result.get('status') == 'FAILED':
            log(f"❌ Arbitrage failed: {result.get('error', 'Unknown error')}")

    except asyncio.TimeoutError:
        log("⚠️ Trading timeout - process killed")
        # The process will be terminated automatically
    except Exception as e:
        log(f"❌ Trading result handling error: {e}")

async def main_loop(analysis_executor, trading_executor):
    """Main loop that coordinates workers and handles results (performance-optimized)."""
    global total_profitable_opportunities_found, total_low_profit_positive_found

    while True:
        await asyncio.sleep(config.ARBITRAGE_CHECK_INTERVAL)  # Use value from config
        if not symbol_info_map:
            logger.info("Symbol map not ready yet, waiting...")
            continue

        logger.info("Starting arbitrage opportunity check...")
        start_time = time.perf_counter()

        current_prices = dict(prices_cache)
        loop = asyncio.get_running_loop()

        all_currencies = sorted(list(set([info['base'] for info in symbol_info_map.values()] + [info['quote'] for info in symbol_info_map.values()])))

        # Limit the number of workers to reduce CPU load
        num_workers = min(config.MAX_CONCURRENT_ANALYSIS, analysis_executor._max_workers)
        chunk_size = (len(all_currencies) + num_workers - 1) // num_workers
        currency_chunks = [all_currencies[i:i + chunk_size] for i in range(0, len(all_currencies), chunk_size)]

        logger.info(f"[WORKER] Work distribution: {num_workers} workers, {len(all_currencies)} currencies, {chunk_size} currencies per worker")
        for i, chunk in enumerate(currency_chunks):
            logger.info(f"[WORKER] Worker {i+1}: {len(chunk)} currencies ({chunk[0]}...{chunk[-1]})")

        # --- Trading Graph Construction (optimized) ---
        trade_graph = {c: [] for c in all_currencies}
        for symbol, info in symbol_info_map.items():
            base, quote = info['base'], info['quote']
            if base in trade_graph and quote in trade_graph:
                trade_graph[base].append(quote)
                trade_graph[quote].append(base)
        # ------------------------------------

        futures = [loop.run_in_executor(analysis_executor, find_arbitrage_worker, current_prices, symbol_info_map, config.MIN_PROFIT_THRESHOLD, TRADING_FEE, chunk, all_currencies, trade_graph) for chunk in currency_chunks]

        aggregated_stats = {
            'total_triangles': 0,
            'non_priority_start': 0,
            'cannot_convert_budget': 0,
            'low_profit': {'negative': 0, 'positive': 0},
            'simulation_failures': {
                'total': 0, 'FAIL_NO_DATA': 0, 'FAIL_STEP_SIZE': 0,
                'FAIL_MIN_QTY': 0, 'FAIL_LIQUIDITY': 0, 'FAIL_MIN_NOTIONAL': 0,
                'UNKNOWN': 0
            }
        }
        total_profitable_found = 0

        # Process results with timeout to avoid blocks
        try:
            for future in asyncio.as_completed(futures, timeout=30):  # 30 second timeout
                try:
                    worker_result = await future
                    opportunities = worker_result.get('profitable', [])
                    worker_stats = worker_result.get('stats', {})

                    # Aggregate statistics
                    if worker_stats:
                        aggregated_stats['total_triangles'] += worker_stats.get('total_triangles', 0)
                        aggregated_stats['non_priority_start'] += worker_stats.get('non_priority_start', 0)
                        aggregated_stats['cannot_convert_budget'] += worker_stats.get('cannot_convert_budget', 0)

                        # Aggregate low_profit
                        low_profit_stats = worker_stats.get('low_profit', {})
                        aggregated_stats['low_profit']['negative'] += low_profit_stats.get('negative', 0)
                        aggregated_stats['low_profit']['positive'] += low_profit_stats.get('positive', 0)

                        # Aggregate simulation_failures
                        sim_fail_stats = worker_stats.get('simulation_failures', {})
                        for key, value in sim_fail_stats.items():
                            aggregated_stats['simulation_failures'][key] += value

                    if not opportunities: continue

                    total_profitable_found += len(opportunities)

                    for opp in opportunities:
                        path, profit_perc_str = opp.get('path'), opp.get('profit_perc')
                        if not path: continue

                        triangle_key = tuple(sorted(path.split('→')[:3]))
                        current_time = time.time()

                        if (current_time - profitable_opportunities_set.get(triangle_key, 0)) > OPPORTUNITY_COOLDOWN:
                            profitable_opportunities_set[triangle_key] = current_time
                            total_profitable_opportunities_found += 1 # Increment global counter

                            # --- LOG AND FILE: ALWAYS BEFORE NOTIFY ---
                            profit_perc_val = float(profit_perc_str)
                            guadagno_stimato = config.SIMULATION_BUDGET_USDT * (profit_perc_val / 100)
                            # Calculate optimal amount and volumes
                            try:
                                importo_ottimale, volumi = calcola_importo_ottimale_con_buffer(opp['pairs'], current_prices, symbol_info_map)
                            except Exception as e:
                                importo_ottimale, volumi = 0, []
                                logger.error(f"Optimal amount calculation error: {e}")
                            log_line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} | {path} | Net Profit: {profit_perc_val:.4f}% | Estimated Gain ({config.SIMULATION_BUDGET_USDT} USDT): {guadagno_stimato:.4f} USDT\n"
                            log_line += f"Optimal investable amount (buffer {int(BUFFER_SICUREZZA*100)}%): {importo_ottimale:.4f} USDT\n"
                            for v in volumi:
                                log_line += f"  - {v['pair']} {v['side']}_qty: {v['qty']:.4f}\n"
                            file_to_write = ANOMALIES_FILE if profit_perc_val > 50.0 else PROFITS_FILE
                            try:
                                with open(file_to_write, "a", encoding="utf-8") as f:
                                    f.write(log_line if profit_perc_val <= 50.0 else f"[ANOMALY] {log_line}")
                            except Exception as e:
                                logger.error(f"Opportunity file write error: {e}")
                            # Always also log to profitable file if above threshold
                            if profit_perc_val >= float(config.MIN_PROFIT_THRESHOLD) * 100:
                                try:
                                    save_profitable_opportunity(opp)
                                except Exception as e:
                                    logger.error(f"Profitable file write error: {e}")

                            # --- ROBUST TELEGRAM NOTIFICATION ---
                            try:
                                msg = format_opportunity_message(opp, current_prices)
                                await send_telegram_notification(msg)
                            except Exception as e:
                                logger.error(f"Telegram formatting or send error for {path}: {e}\nData: {opp}")

                except Exception as e:
                    logger.error(f"Worker result processing error: {e}")

        except asyncio.TimeoutError:
            logger.warning("⚠️ Worker analysis timeout (30s)")

        # Update global near-profitable counter
        total_low_profit_positive_found += aggregated_stats['low_profit']['positive']

        duration_ms = (time.perf_counter() - start_time) * 1000
        total_low_profit = aggregated_stats['low_profit']['negative'] + aggregated_stats['low_profit']['positive']
        total_sim_failures = aggregated_stats['simulation_failures']['total']

        # Detailed log but with reduced frequency for performance
        should_log_detailed = (
            total_profitable_found > 0 or
            duration_ms > 5000 or
            aggregated_stats['total_triangles'] > 10000  # Log if many triangles
        )

        if should_log_detailed:
            logger.info("--- Analysis Cycle Statistics ---")
            logger.info(f"Analysis Duration: {duration_ms:.2f} ms")
            logger.info(f"Valid triangles found: {aggregated_stats['total_triangles']:,}")
            logger.info(f"  - Discarded (non-priority start): {aggregated_stats['non_priority_start']:,}")
            logger.info(f"  - Discarded (budget not convertible to unit asset): {aggregated_stats['cannot_convert_budget']:,}")
            logger.info(f"  - Discarded (simulation failure): {total_sim_failures:,}")

            if total_sim_failures > 0:
                sim_failures = aggregated_stats['simulation_failures']
                logger.info(f"    - Insufficient liquidity: {sim_failures.get('FAIL_LIQUIDITY', 0):,}")
                logger.info(f"    - Minimum notional value: {sim_failures.get('FAIL_MIN_NOTIONAL', 0):,}")
                logger.info(f"    - Minimum quantity not reached: {sim_failures.get('FAIL_MIN_QTY', 0):,}")
                logger.info(f"    - Zero quantity for stepSize: {sim_failures.get('FAIL_STEP_SIZE', 0):,}")
                logger.info(f"    - Missing data/price: {sim_failures.get('FAIL_NO_DATA', 0):,}")
                if sim_failures.get('UNKNOWN', 0) > 0:
                     logger.info(f"    - Unknown/Other: {sim_failures.get('UNKNOWN', 0):,}")

            logger.info(f"  - Discarded (profit too low): {total_low_profit:,}")
            if total_low_profit > 0:
                logger.info(f"    - Negative (loss): {aggregated_stats['low_profit']['negative']:,}")
                logger.info(f"    - Positive (below threshold): {aggregated_stats['low_profit']['positive']:,}")
            logger.info(f"Profitable Opportunities Found: {total_profitable_found}")
            logger.info("------------------------------------")
        else:
            # Summary log for normal cycles
            logger.info(f"Analysis completed: {duration_ms:.1f}ms | Triangles: {aggregated_stats['total_triangles']:,} | Opportunities: {total_profitable_found}")

async def send_telegram_notification(message):
    """H5 fix: uses aiohttp instead of requests.post (sync, blocked the event loop)."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, data=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(f"Telegram send error: {resp.status} {text}")
    except Exception as e:
        logger.error(f"Telegram send exception: {e}")

async def websocket_manager(symbols):
    """Handles a single WebSocket connection with reconnection and optimizations."""
    url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(symbols)}"
    reconnect_delay = 5
    max_reconnect_delay = 60

    while True:
        try:
            # Import websockets only when needed
            import websockets

            async with websockets.connect(
                url,
                ping_interval=30,  # Increased from 20 to 30
                ping_timeout=60,
                close_timeout=10,
                max_size=2**20  # Limit message size
            ) as websocket:
                logger.info(f"WebSocket connection established for {len(symbols)} symbols.")
                reconnect_delay = 5  # Reset delay on success

                async for message in websocket:
                    await handle_message(message)

        except Exception as e:
            logger.error(f"WebSocket error ({len(symbols)} symbols): {e}. Reconnecting in {reconnect_delay}s.")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)  # Exponential backoff

async def hourly_summary_task(bot_start_time):
    """Sends an hourly summary on Telegram."""
    while True:
        await asyncio.sleep(3600) # Wait 1 hour

        uptime_seconds = time.time() - bot_start_time
        days = int(uptime_seconds // (24 * 3600))
        uptime_seconds %= (24 * 3600)
        hours = int(uptime_seconds // 3600)
        uptime_seconds %= 3600
        minutes = int(uptime_seconds // 60)

        uptime_str = f"{days}d {hours}h {minutes}m"

        summary_message = (
            f"🕒 *Hourly Summary*\n\n"
            f"✅ *Uptime:* `{uptime_str}`\n"
            f"💰 *Opportunities Found:* `{total_profitable_opportunities_found}`\n"
            f"🤏 *Near Profitable (below threshold):* `{total_low_profit_positive_found}`"
        )
        await send_telegram_notification(summary_message)

def calcola_importo_ottimale_con_buffer(pairs, prices, symbol_info_map):
    """
    Calculates the maximum investable amount for a triangle using only the best bid/ask and applying a safety buffer.
    Returns the optimal amount and the available volumes for each step.
    """
    importo_massimi = []
    volumi = []
    for i, pair in enumerate(pairs):
        info = symbol_info_map.get(pair)
        book = prices.get(pair)
        if not info or not book:
            importo_massimi.append(0)
            volumi.append({'pair': pair, 'qty': 0, 'side': 'N/A'})
            continue
        if i == 0:
            # First step: BUY (we use ask)
            qty_disp = book['ask_qty'] * BUFFER_SICUREZZA
            prezzo = book['ask']
            min_qty = info['minQty']
            min_notional = info['minNotional']
            step = info['stepSize']
            max_qty = max(min(qty_disp, qty_disp // step * step), 0)
            max_notional = max_qty * prezzo
            if max_qty < min_qty or max_notional < min_notional:
                max_qty = 0
            importo_massimi.append(max_qty * prezzo)
            volumi.append({'pair': pair, 'qty': qty_disp, 'side': 'ask'})
        else:
            # Second and third step: SELL (we use bid)
            qty_disp = book['bid_qty'] * BUFFER_SICUREZZA
            prezzo = book['bid']
            min_qty = info['minQty']
            min_notional = info['minNotional']
            step = info['stepSize']
            max_qty = max(min(qty_disp, qty_disp // step * step), 0)
            max_notional = max_qty * prezzo
            if max_qty < min_qty or max_notional < min_notional:
                max_qty = 0
            importo_massimi.append(max_qty * prezzo)
            volumi.append({'pair': pair, 'qty': qty_disp, 'side': 'bid'})
    importo_ottimale = min(importo_massimi)
    return importo_ottimale, volumi

async def main():
    global symbol_info_map

    # Print configuration at startup
    config.print_config_summary()

    # Configuration validation
    if config.AUTO_TRADE_ENABLED:
        errors = config.validate_config()
        if errors:
            logger.error("❌ Configuration errors detected:")
            for error in errors:
                logger.error(f"  - {error}")
            logger.error("The bot will continue with analysis only (trading disabled)")
            config.AUTO_TRADE_ENABLED = False

    # H1 fix: removed reset to 15. Precision is already 28 (Python default, set at module level).
    bot_start_time = time.time()

    logger.info("Starting Binance triangular arbitrage program...")
    await send_telegram_notification("🤖 Starting arbitrage bot...")

    symbols, symbol_info_map = await get_exchange_symbols()
    if not symbols:
        logger.error("No symbols obtained. Cannot proceed.")
        return

    symbol_groups = [symbols[i:i + SYMBOLS_PER_CONNECTION] for i in range(0, len(symbols), SYMBOLS_PER_CONNECTION)]

    # Separate executors for analysis and trading
    with ProcessPoolExecutor(max_workers=config.ANALYSIS_CORES) as analysis_executor:
        with ProcessPoolExecutor(max_workers=config.TRADING_CORES) as trading_executor:
            websocket_tasks = [websocket_manager(group) for group in symbol_groups]
            all_tasks = websocket_tasks + [
                main_loop(analysis_executor, trading_executor),
                hourly_summary_task(bot_start_time)
            ]
            await asyncio.gather(*all_tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program manually interrupted.")
    except Exception as e:
        logger.critical(f"Unhandled critical error in main: {e}", exc_info=True)
