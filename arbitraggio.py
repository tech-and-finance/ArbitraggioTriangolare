import asyncio
import json
import websockets
from decimal import Decimal, getcontext
from itertools import permutations
from datetime import datetime
import requests
import os
import time
from concurrent.futures import ProcessPoolExecutor
import logging
from math import ceil
import concurrent.futures
import psutil
try:
    import psutil
    psutil_available = True
except ImportError:
    psutil_available = False

# --- Configurazione del Logging ---
# Rimuove i gestori di default per evitare log duplicati
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Configura il logger principale
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.StreamHandler()],
                    encoding='utf-8')
logger = logging.getLogger(__name__)

# --- Costanti di Configurazione ---
SYMBOLS_PER_CONNECTION = 200  # Numero di simboli per connessione WebSocket
PROFIT_THRESHOLD = Decimal("0.001")  # Profitto minimo netto per notifica (0.1%)
TRADING_FEE = Decimal("0.00075")      # Commissione per ogni trade (0.075% con sconto BNB)
STARTING_CAPITAL_USDT = Decimal("100") # Capitale iniziale per la simulazione
ARBITRAGE_CHECK_INTERVAL = 120  # Secondi tra i controlli di arbitraggio (2 minuti)
STARTING_ASSETS = {'USDT', 'USDC', 'FDUSD', 'DAI', 'TUSD', 'BTC', 'ETH', 'SOL'} # Asset di partenza per l'analisi di arbitraggio
SIMULATION_CAPITAL = Decimal("100") # Capitale di simulazione in valuta di partenza
OPPORTUNITY_COOLDOWN = 60  # Secondi prima di notificare di nuovo lo stesso triangolo

# --- File di Log ---
PROFITS_FILE = "profitable_opportunities.txt"
ANOMALIES_FILE = "anomalies.txt"

# --- Variabili Globali ---
prices_cache = {}
symbol_info_map = {}
last_check_time = datetime.now()
profitable_opportunities_set = {}
total_profitable_opportunities_found = 0
total_low_profit_positive_found = 0

# Configurazione Telegram (caricata da variabili d'ambiente o file)
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8182228673:AAEwknPEkwI_vp8froD8rNEquaK88W3EukQ')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '279229754')

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] {msg}")

getcontext().prec = 12

# URL WebSocket Binance
WS_URL = "wss://stream.binance.com:9443/stream"

# Cache in tempo reale dei prezzi
price_map = {}
msg_count = 0  # Contatore globale dei messaggi WebSocket

# Funzione per inviare messaggio Telegram
def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text,
        'parse_mode': 'Markdown'  # Abilita la formattazione Markdown
    }
    try:
        response = requests.post(url, data=payload, timeout=5)
        if response.status_code == 200:
            log("[TELEGRAM] Notifica inviata con successo.")
        else:
            log(f"[TELEGRAM][ERRORE] Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        log(f"[TELEGRAM][ERRORE] {e}")

# Funzione per scrivere opportunità su file giornaliero
def save_opportunity_to_file(opp):
    today = datetime.now().strftime('%Y%m%d')
    filename = f"arbitrage_{today}.txt"
    
    # Converti i valori stringa in Decimal prima della formattazione
    profit_dec = Decimal(opp['profit'])
    final_dec = Decimal(opp['final'])
    profit_usdt = profit_dec * Decimal('100')
    
    # Aggiungi la nota
    note = opp.get('note', '')
    
    line = (f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} | "
            f"{opp['path']} | "
            f"Profitto: {profit_dec:.6f} | "
            f"Finale: {final_dec:.6f} | "
            f"Guadagno USDT (su 100): {profit_usdt:.4f} USDT | "
            f"NOTE: {note}\n")
            
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(line)
    # Riduciamo il logging per non intasare la console
    # log(f"[FILE] Opportunità salvata su {filename}")

def save_profitable_opportunity(opp):
    """Salva solo le opportunità profittevoli in un file dedicato."""
    filename = "profitable_opportunities.txt"
    
    # Converti profitto da stringa a Decimal
    profit_dec = Decimal(opp['profit'])
    profit_usdt = profit_dec * Decimal('100')
    
    line = (f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} | "
            f"{opp['path']} | "
            f"Profitto Netto: {(profit_dec*100):.4f}% | "
            f"Guadagno Stimato (100 USDT): {profit_usdt:.4f} USDT\n")
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(line)
    log(f"[FILE] Opportunità PROFITTEVOLE salvata su {filename}")

async def monitor_performance(process):
    """Monitora e registra le performance del sistema ogni 5 secondi."""
    global msg_count
    log("[LOG] Avvio monitoraggio performance...")
    while True:
        await asyncio.sleep(5)
        
        total_cpu = 0
        total_ram = 0

        if psutil_available:
            try:
                # CPU e RAM del processo principale
                main_cpu = process.cpu_percent(interval=None)
                main_ram = process.memory_info().rss / (1024 * 1024)
                total_cpu += main_cpu
                total_ram += main_ram
                
                # Aggiungi CPU e RAM dei processi figli (il ProcessPoolExecutor)
                children = process.children(recursive=True)
                for child in children:
                    try:
                        total_cpu += child.cpu_percent(interval=None)
                        total_ram += child.memory_info().rss / (1024 * 1024)
                    except psutil.NoSuchProcess:
                        continue # Il processo potrebbe essere terminato nel frattempo
            except psutil.NoSuchProcess:
                log("[PERF][ATTENZIONE] Processo principale non trovato per il monitoraggio.")
                continue

        # Copia sicura per evitare race condition
        current_price_map = price_map.copy()
        
        cpu_display = f"{total_cpu:.1f}%" if psutil_available else "N/A"
        ram_display = f"{total_ram:.2f} MB" if psutil_available else "N/A"
        
        msgs = msg_count
        msg_count = 0  # azzera per il prossimo ciclo
        log(f"[PERF] CPU: {cpu_display} | RAM: {ram_display} | Simboli cache: {len(current_price_map)} | Msg/s: {msgs/5:.0f}")

async def get_exchange_symbols():
    """Ottiene i simboli e le loro info, focalizzandosi sulle coppie legate agli asset di partenza."""
    try:
        url = "https://api.binance.com/api/v3/exchangeInfo"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        trading_symbols = {s['symbol']: s for s in data['symbols'] if s['status'] == 'TRADING'}

        # Filtra per le valute che hanno una coppia diretta con gli asset di partenza per limitare il campo
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
        logger.info(f"Ottenuti {len(formatted_symbols)} simboli per l'arbitraggio (legati a {', '.join(sorted(list(STARTING_ASSETS)))}).")
        return formatted_symbols, temp_symbol_info_map
    except Exception as e:
        logger.error(f"Impossibile ottenere i simboli: {e}")
        return [], {}

async def handle_message(msg):
    global msg_count
    msg_count += 1
    
    # Gestione del formato dello stream combinato
    data = json.loads(msg)
    if 'data' in data:  # Stream combinato
        data = data['data']
    
    symbol = data['s']
    prices_cache[symbol] = {
        'bid': Decimal(data['b']), 
        'ask': Decimal(data['a']),
        'bid_qty': Decimal(data['B']),
        'ask_qty': Decimal(data['A'])
    }

def format_opportunity_message(opp, prices):
    """Formatta un'opportunità di arbitraggio in un messaggio Telegram leggibile."""
    try:
        path = opp['path']
        steps = path.split('→')
        
        # Aggiungo un controllo di robustezza per percorsi non validi
        if len(steps) < 4:
            log(f"[MSG][WARN] Ricevuto percorso non valido o incompleto: '{path}'")
            return f"⚠️ *Dati anomali*\n\nPercorso: `{path}`. Impossibile generare un esempio valido."

        # Converte in modo sicuro i dati ricevuti (potrebbero essere stringhe)
        profit = Decimal(str(opp['profit']))
        profit_percentage = profit * 100
        
        details = opp.get('details', {})
        if not details:
            return f"⚠️ *DATI INCOMPLETI*\n\nPercorso: `{path}`. Impossibile generare esempio."

        # Converte in modo sicuro i dettagli
        rates = tuple(Decimal(str(r)) for r in details['rates'])
        (rate1, rate2, rate3) = rates
        
        pairs = tuple(str(p) for p in details['pairs'])
        (pair1_str, pair2_str, pair3_str) = pairs

        prices_details = tuple(Decimal(str(p)) for p in details['prices'])
        (price_val1, price_val2, price_val3) = prices_details

        # --- LOGICA SEMPLIFICATA E CORRETTA ---
        investimento_usdt = Decimal('100')
        guadagno_usdt = investimento_usdt * profit
        finale_usdt = investimento_usdt + guadagno_usdt
        commissioni_usdt = investimento_usdt * (1 - (1 - TRADING_FEE)**3)

        message = f"⚡ *OPPORTUNITÀ DI ARBITRAGGIO*\n\n" \
                    f"🔄 *Percorso:* `{path}`\n" \
                    f"💰 *Profitto Netto Stimato:* `{profit_percentage:.4f}%`\n\n" \
                    f"💵 *Esempio su 100 USDT:*\n" \
                    f"• Investimento: `{investimento_usdt:.2f} USDT`\n" \
                    f"• Finale Stimato: `{finale_usdt:.4f} USDT`\n" \
                    f"• Guadagno Netto: `{guadagno_usdt:.4f} USDT`\n" \
                    f"• Commissioni Stimate: `{commissioni_usdt:.4f} USDT`\n\n" \
                    f"📈 *Operazioni e Prezzi (usati nel calcolo):*\n" \
                    f"1. `{steps[0]}→{steps[1]}` (`{pair1_str}` @ `{price_val1:.8f}`)\n" \
                    f"2. `{steps[1]}→{steps[2]}` (`{pair2_str}` @ `{price_val2:.8f}`)\n" \
                    f"3. `{steps[2]}→{steps[0]}` (`{pair3_str}` @ `{price_val3:.8f}`)\n\n" \
                    f"⏰ *Timestamp:* `{datetime.now().strftime('%H:%M:%S')}`"
        return message

    except Exception as e:
        log(f"[MSG][ERR] Errore critico nella formattazione: {e} per opp: {opp}")
        return f"🚨 Errore nella formattazione del messaggio per `{opp.get('path', 'N/A')}`"

def adjust_quantity_for_step_size(quantity, step_size):
    """Arrotonda per difetto la quantità per conformarla allo stepSize di Binance."""
    if step_size > 0:
        return (quantity // step_size) * step_size
    return quantity

def find_arbitrage_worker(prices, symbol_info_map_local, profit_threshold, trading_fee, currency_chunk, all_currencies, trade_graph):
    """Processo worker che cerca opportunità di arbitraggio navigando un grafo pre-calcolato."""
    profitable_opportunities = []
    stats = {
        'total_triangles': 0,
        'non_priority_start': 0,
        'low_profit': {'negative': 0, 'positive': 0},
        'simulation_failures': {
            'total': 0,
            'FAIL_NO_DATA': 0,
            'FAIL_STEP_SIZE': 0,
            'FAIL_MIN_QTY': 0,
            'FAIL_LIQUIDITY': 0,
            'FAIL_MIN_NOTIONAL': 0,
            'UNKNOWN': 0
        }
    }
    
    existing_pairs = {c: {} for c in all_currencies}
    for symbol, info in symbol_info_map_local.items():
        base, quote = info['base'], info['quote']
        if base not in existing_pairs:
            existing_pairs[base] = {}
        existing_pairs[base][quote] = symbol

    # Naviga il grafo per trovare solo percorsi validi
    for p_a in currency_chunk:
        # p_a deve essere nel grafo
        if p_a not in trade_graph: continue
        
        # Livello 1: p_a -> p_b
        for p_b in trade_graph[p_a]:
            # Livello 2: p_b -> p_c
            if p_b not in trade_graph: continue
            for p_c in trade_graph[p_b]:
                # Non vogliamo loop semplici come A->B->A
                if p_c == p_a: continue
                
                # Livello 3: Controlla se p_c può tornare a p_a
                if p_c in trade_graph and p_a in trade_graph[p_c]:
                    stats['total_triangles'] += 1
                    
                    # Simula solo percorsi che iniziano con un asset prioritario
                    if p_a not in STARTING_ASSETS:
                        stats['non_priority_start'] += 1
                        continue

                    try:
                        status, result = simulate_trade(p_a, p_b, SIMULATION_CAPITAL, prices, symbol_info_map_local, existing_pairs)
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
                        profit = final_amount - SIMULATION_CAPITAL
                        
                        if profit > (SIMULATION_CAPITAL * profit_threshold / 100):
                             profit_perc = (profit / SIMULATION_CAPITAL) * 100
                             profitable_opportunities.append({
                                'path': f"{p_a}→{p_b}→{p_c}→{p_a}",
                                'profit_perc': f"{profit_perc:.4f}",
                                'pairs': [pair1_str, pair2_str, pair3_str]
                            })
                        else:
                            if profit < 0:
                                stats['low_profit']['negative'] += 1
                            else:
                                stats['low_profit']['positive'] += 1

                    except Exception:
                        stats['simulation_failures']['total'] += 1
                        stats['simulation_failures']['UNKNOWN'] += 1
                        continue
                        
    return {'profitable': profitable_opportunities, 'stats': stats}

def simulate_trade(start_asset, end_asset, amount_in, prices, symbol_info, existing_pairs):
    """
    Simula un singolo trade.
    Restituisce ('SUCCESS', (rate, amount_out, symbol)) o ('FAIL_REASON', None).
    """
    # Compra end_asset con start_asset (coppia: end_asset/start_asset)
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

    # Vendi start_asset per end_asset (coppia: start_asset/end_asset)
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

    return 'FAIL_NO_DATA', None # Se la coppia non esiste in nessuna direzione

def cpu_stress_test_worker(iterations):
    print(f"[STRESS][WORKER] PID: {os.getpid()} | Iterazioni: {iterations}")
    x = 0
    for i in range(iterations):
        x += i
    print(f"[STRESS][WORKER] PID: {os.getpid()} | Fine lavoro")
    return x

async def main_loop(executor):
    """Ciclo principale che coordina i worker e gestisce i risultati."""
    global total_profitable_opportunities_found, total_low_profit_positive_found
    
    while True:
        await asyncio.sleep(5) 
        if not symbol_info_map:
            logger.info("Mappa dei simboli non ancora pronta, attendo...")
            continue
        
        logger.info("Inizio controllo opportunità di arbitraggio...")
        start_time = time.perf_counter()

        current_prices = dict(prices_cache)
        loop = asyncio.get_running_loop()
        
        all_currencies = sorted(list(set([info['base'] for info in symbol_info_map.values()] + [info['quote'] for info in symbol_info_map.values()])))
        
        num_workers = executor._max_workers
        chunk_size = (len(all_currencies) + num_workers - 1) // num_workers
        currency_chunks = [all_currencies[i:i + chunk_size] for i in range(0, len(all_currencies), chunk_size)]
        
        # --- Costruzione del Grafo di Trading ---
        trade_graph = {c: [] for c in all_currencies}
        for symbol, info in symbol_info_map.items():
            base, quote = info['base'], info['quote']
            if base in trade_graph and quote in trade_graph:
                trade_graph[base].append(quote)
                trade_graph[quote].append(base)
        # ------------------------------------

        futures = [loop.run_in_executor(executor, find_arbitrage_worker, current_prices, symbol_info_map, PROFIT_THRESHOLD, TRADING_FEE, chunk, all_currencies, trade_graph) for chunk in currency_chunks]
        
        aggregated_stats = {
            'total_triangles': 0,
            'non_priority_start': 0,
            'low_profit': {'negative': 0, 'positive': 0},
            'simulation_failures': {
                'total': 0, 'FAIL_NO_DATA': 0, 'FAIL_STEP_SIZE': 0,
                'FAIL_MIN_QTY': 0, 'FAIL_LIQUIDITY': 0, 'FAIL_MIN_NOTIONAL': 0,
                'UNKNOWN': 0
            }
        }
        total_profitable_found = 0

        for future in asyncio.as_completed(futures):
            try:
                worker_result = await future
                opportunities = worker_result.get('profitable', [])
                worker_stats = worker_result.get('stats', {})

                # Aggrega le statistiche
                if worker_stats:
                    aggregated_stats['total_triangles'] += worker_stats.get('total_triangles', 0)
                    aggregated_stats['non_priority_start'] += worker_stats.get('non_priority_start', 0)
                    
                    # Aggrega low_profit
                    low_profit_stats = worker_stats.get('low_profit', {})
                    aggregated_stats['low_profit']['negative'] += low_profit_stats.get('negative', 0)
                    aggregated_stats['low_profit']['positive'] += low_profit_stats.get('positive', 0)

                    # Aggrega simulation_failures
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
                        total_profitable_opportunities_found += 1 # Incrementa il contatore globale
                        
                        await send_telegram_notification(format_opportunity_message(opp, current_prices))
                        
                        profit_perc_val = float(profit_perc_str)
                        guadagno_stimato = 100 * (profit_perc_val / 100)
                        log_line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} | {path} | Profitto Netto: {profit_perc_val:.4f}% | Guadagno Stimato (100 USDT): {guadagno_stimato:.4f} USDT\n"
                        
                        file_to_write = ANOMALIES_FILE if profit_perc_val > 50.0 else PROFITS_FILE
                        with open(file_to_write, "a", encoding="utf-8") as f:
                            f.write(log_line if profit_perc_val <= 50.0 else f"[ANOMALIA] {log_line}")

            except Exception as e:
                logger.error(f"Errore nel processare il risultato del worker: {e}")
        
        # Aggiorna il contatore globale dei quasi-profittevoli
        total_low_profit_positive_found += aggregated_stats['low_profit']['positive']

        duration_ms = (time.perf_counter() - start_time) * 1000
        total_low_profit = aggregated_stats['low_profit']['negative'] + aggregated_stats['low_profit']['positive']
        total_sim_failures = aggregated_stats['simulation_failures']['total']

        logger.info("--- Statistiche Ciclo di Analisi ---")
        logger.info(f"Durata Analisi: {duration_ms:.2f} ms")
        logger.info(f"Triangoli validi trovati: {aggregated_stats['total_triangles']:,}")
        logger.info(f"  - Scartati (partenza non prioritaria): {aggregated_stats['non_priority_start']:,}")
        logger.info(f"  - Scartati (fallimento simulazione): {total_sim_failures:,}")
        
        if total_sim_failures > 0:
            sim_failures = aggregated_stats['simulation_failures']
            logger.info(f"    - Liquidità insufficiente: {sim_failures.get('FAIL_LIQUIDITY', 0):,}")
            logger.info(f"    - Valore nozionale minimo: {sim_failures.get('FAIL_MIN_NOTIONAL', 0):,}")
            logger.info(f"    - Quantità minima non raggiunta: {sim_failures.get('FAIL_MIN_QTY', 0):,}")
            logger.info(f"    - Quantità zero per stepSize: {sim_failures.get('FAIL_STEP_SIZE', 0):,}")
            logger.info(f"    - Dati/Prezzo mancanti: {sim_failures.get('FAIL_NO_DATA', 0):,}")
            if sim_failures.get('UNKNOWN', 0) > 0:
                 logger.info(f"    - Sconosciuto/Altro: {sim_failures.get('UNKNOWN', 0):,}")

        logger.info(f"  - Scartati (profitto troppo basso): {total_low_profit:,}")
        if total_low_profit > 0:
            logger.info(f"    - Negativo (perdita): {aggregated_stats['low_profit']['negative']:,}")
            logger.info(f"    - Positivo (sotto soglia): {aggregated_stats['low_profit']['positive']:,}")
        logger.info(f"Opportunità Profittevoli Trovate: {total_profitable_found}")
        logger.info("------------------------------------")

async def send_telegram_notification(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        async with asyncio.timeout(10):
            response = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'})
            if response.status_code != 200:
                logger.warning(f"Errore invio Telegram: {response.status_code} {response.text}")
    except Exception as e:
        logger.error(f"Eccezione invio Telegram: {e}")

async def websocket_manager(symbols):
    """Gestisce una singola connessione WebSocket con riconnessione."""
    url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(symbols)}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=60) as websocket:
                logger.info(f"Connessione WebSocket stabilita per {len(symbols)} simboli.")
                async for message in websocket:
                    await handle_message(message)
        except Exception as e:
            logger.error(f"Errore WebSocket ({len(symbols)} simboli): {e}. Riconnessione tra 5s.")
            await asyncio.sleep(5)

async def monitor_performance_task():
    """Task per monitorare e loggare l'uso di CPU/RAM."""
    p = psutil.Process(os.getpid())
    while True:
        try:
            with p.oneshot():
                cpu = p.cpu_percent()
                ram_mb = p.memory_info().rss / (1024 * 1024)
                children = p.children(recursive=True)
                for child in children:
                    with child.oneshot():
                        cpu += child.cpu_percent()
                        ram_mb += child.memory_info().rss / (1024 * 1024)
            
            logger.info(f"PERF - CPU: {cpu:.1f}% | RAM: {ram_mb:.2f} MB | Cache Prezzi: {len(prices_cache)}")
        except psutil.NoSuchProcess:
            pass # Il processo potrebbe essere terminato
        await asyncio.sleep(5)

async def hourly_summary_task(bot_start_time):
    """Invia un riepilogo orario su Telegram."""
    while True:
        await asyncio.sleep(3600) # Attende 1 ora

        uptime_seconds = time.time() - bot_start_time
        days = int(uptime_seconds // (24 * 3600))
        uptime_seconds %= (24 * 3600)
        hours = int(uptime_seconds // 3600)
        uptime_seconds %= 3600
        minutes = int(uptime_seconds // 60)

        uptime_str = f"{days}g {hours}h {minutes}m"

        summary_message = (
            f"🕒 *Riepilogo Orario*\n\n"
            f"✅ *Uptime:* `{uptime_str}`\n"
            f"💰 *Opportunità Trovate:* `{total_profitable_opportunities_found}`\n"
            f"🤏 *Quasi Profittevoli (sotto soglia):* `{total_low_profit_positive_found}`"
        )
        await send_telegram_notification(summary_message)

async def main():
    global symbol_info_map
    getcontext().prec = 15
    bot_start_time = time.time()
    
    logger.info("Avvio programma di arbitraggio triangolare Binance...")
    await send_telegram_notification("🤖 Avvio del bot di arbitraggio...")

    symbols, symbol_info_map = await get_exchange_symbols()
    if not symbols:
        logger.error("Nessun simbolo ottenuto. Impossibile procedere.")
        return

    symbol_groups = [symbols[i:i + SYMBOLS_PER_CONNECTION] for i in range(0, len(symbols), SYMBOLS_PER_CONNECTION)]
    
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        websocket_tasks = [websocket_manager(group) for group in symbol_groups]
        all_tasks = websocket_tasks + [main_loop(executor), monitor_performance_task(), hourly_summary_task(bot_start_time)]
        await asyncio.gather(*all_tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Programma interrotto manualmente.")
    except Exception as e:
        logger.critical(f"Errore critico non gestito in main: {e}", exc_info=True)