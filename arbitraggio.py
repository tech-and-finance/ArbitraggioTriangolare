import asyncio
import json
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

psutil_available = False # Disabilitato forzatamente

# Importa i nuovi moduli per il trading automatico
import config
from trading_executor import trading_worker_with_affinity

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
TRADING_FEE = Decimal("0.00075")      # Commissione per ogni trade (0.075% con sconto BNB)
STARTING_ASSETS = {'USDT', 'USDC', 'FDUSD', 'DAI', 'TUSD', 'BTC', 'ETH', 'SOL'} # Asset di partenza per l'analisi di arbitraggio
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

BUFFER_SICUREZZA = 0.8  # 80% della quantità disponibile

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
    """Monitora e registra le performance del sistema ogni 15 secondi (ridotto da 5)."""
    global msg_count
    log("[LOG] Avvio monitoraggio performance ottimizzato...")
    while True:
        await asyncio.sleep(15)  # Aumentato da 5 a 15 secondi per ridurre carico
        
        total_cpu = 0
        total_ram = 0

        if psutil_available:
            try:
                # CPU e RAM del processo principale (semplificato)
                main_cpu = process.cpu_percent(interval=0.1)  # Intervallo minimo per accuratezza
                main_ram = process.memory_info().rss / (1024 * 1024)
                total_cpu += main_cpu
                total_ram += main_ram
                
                # Monitora solo i processi figli attivi (ridotto carico)
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
                log("[PERF][ATTENZIONE] Processo principale non trovato per il monitoraggio.")
                continue

        # Copia sicura per evitare race condition
        current_price_map = price_map.copy()
        
        cpu_display = f"{total_cpu:.1f}%" if psutil_available else "N/A"
        ram_display = f"{total_ram:.2f} MB" if psutil_available else "N/A"
        
        msgs = msg_count
        msg_count = 0  # azzera per il prossimo ciclo
        msg_rate = msgs/15  # Calcolato su 15 secondi
        
        # Log solo se ci sono attività significative
        if msgs > 0 or total_cpu > 10:
            log(f"[PERF] CPU: {cpu_display} | RAM: {ram_display} | Cache: {len(current_price_map)} | Msg/s: {msg_rate:.1f}")
        else:
            log(f"[PERF] CPU: {cpu_display} | RAM: {ram_display} | Cache: {len(current_price_map)} | Stato: Idle")

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
        investimento_usdt = config.SIMULATION_BUDGET_USDT
        guadagno_usdt = investimento_usdt * profit
        finale_usdt = investimento_usdt + guadagno_usdt
        commissioni_usdt = investimento_usdt * (1 - (1 - TRADING_FEE)**3)

        message = f"⚡ *OPPORTUNITÀ DI ARBITRAGGIO*\n\n" \
                    f"🔄 *Percorso:* `{path}`\n" \
                    f"💰 *Profitto Netto Stimato:* `{profit_percentage:.4f}%`\n\n" \
                    f"💵 *Esempio su {investimento_usdt} USDT:*\n" \
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

    # Naviga il grafo per trovare solo percorsi validi
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
                        # USA LA VARIABILE DI CONFIG CORRETTA QUI
                        status, result = simulate_trade(p_a, p_b, config.SIMULATION_BUDGET_USDT, prices, symbol_info_map_local, existing_pairs)
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
                        profit = final_amount - config.SIMULATION_BUDGET_USDT
                        
                        if profit > (config.SIMULATION_BUDGET_USDT * profit_threshold):
                             profit_perc = (profit / config.SIMULATION_BUDGET_USDT) * 100
                             profitable_opportunities.append({
                                'path': f"{p_a}→{p_b}→{p_c}→{p_a}",
                                'profit_perc': f"{profit_perc:.4f}",
                                'pairs': [pair1_str, pair2_str, pair3_str],
                                # Dettagli aggiuntivi per un logging migliore
                                'details': {
                                    'rates': (str(rate1), str(rate2), str(rate3)),
                                    'prices': (str(prices.get(pair1_str,{}).get('ask' if p_a==symbol_info_map_local[pair1_str]['quote'] else 'bid')), 
                                               str(prices.get(pair2_str,{}).get('ask' if p_b==symbol_info_map_local[pair2_str]['quote'] else 'bid')),
                                               str(prices.get(pair3_str,{}).get('ask' if p_c==symbol_info_map_local[pair3_str]['quote'] else 'bid')))
                                }
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

async def handle_trading_result(future):
    """Gestisce il risultato del trading asincrono"""
    try:
        # Timeout di 30 secondi per l'esecuzione
        result = await asyncio.wait_for(future, timeout=config.TRADING_TIMEOUT)
        log(f"Trading completato: {result.get('status', 'Unknown')}")
        
        if result.get('status') == 'SUCCESS':
            profit_pct = result.get('profit_percentage', 0)
            log(f"✅ Arbitraggio profittevole: {profit_pct:.4f}%")
        elif result.get('status') == 'FAILED':
            log(f"❌ Arbitraggio fallito: {result.get('error', 'Unknown error')}")
            
    except asyncio.TimeoutError:
        log("⚠️ Trading timeout - processo ucciso")
        # Il processo verrà terminato automaticamente
    except Exception as e:
        log(f"❌ Errore gestione risultato trading: {e}")

async def main_loop(analysis_executor, trading_executor):
    """Ciclo principale che coordina i worker e gestisce i risultati (ottimizzato per performance)."""
    global total_profitable_opportunities_found, total_low_profit_positive_found
    
    while True:
        await asyncio.sleep(config.ARBITRAGE_CHECK_INTERVAL)  # Usa il valore da config
        if not symbol_info_map:
            logger.info("Mappa dei simboli non ancora pronta, attendo...")
            continue
        
        logger.info("Inizio controllo opportunità di arbitraggio...")
        start_time = time.perf_counter()

        current_prices = dict(prices_cache)
        loop = asyncio.get_running_loop()
        
        all_currencies = sorted(list(set([info['base'] for info in symbol_info_map.values()] + [info['quote'] for info in symbol_info_map.values()])))
        
        # Limita il numero di worker per ridurre carico CPU
        num_workers = min(config.MAX_CONCURRENT_ANALYSIS, analysis_executor._max_workers)
        chunk_size = (len(all_currencies) + num_workers - 1) // num_workers
        currency_chunks = [all_currencies[i:i + chunk_size] for i in range(0, len(all_currencies), chunk_size)]
        
        # --- Costruzione del Grafo di Trading (ottimizzata) ---
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
            'low_profit': {'negative': 0, 'positive': 0},
            'simulation_failures': {
                'total': 0, 'FAIL_NO_DATA': 0, 'FAIL_STEP_SIZE': 0,
                'FAIL_MIN_QTY': 0, 'FAIL_LIQUIDITY': 0, 'FAIL_MIN_NOTIONAL': 0,
                'UNKNOWN': 0
            }
        }
        total_profitable_found = 0

        # Processa i risultati con timeout per evitare blocchi
        try:
            for future in asyncio.as_completed(futures, timeout=30):  # Timeout di 30 secondi
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

                            # --- LOG E FILE: SEMPRE PRIMA DI NOTIFICA ---
                            profit_perc_val = float(profit_perc_str)
                            guadagno_stimato = config.SIMULATION_BUDGET_USDT * (profit_perc_val / 100)
                            # Calcolo importo ottimale e volumi
                            try:
                                importo_ottimale, volumi = calcola_importo_ottimale_con_buffer(opp['pairs'], current_prices, symbol_info_map)
                            except Exception as e:
                                importo_ottimale, volumi = 0, []
                                logger.error(f"Errore calcolo importo ottimale: {e}")
                            log_line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} | {path} | Profitto Netto: {profit_perc_val:.4f}% | Guadagno Stimato ({config.SIMULATION_BUDGET_USDT} USDT): {guadagno_stimato:.4f} USDT\n"
                            log_line += f"Importo ottimale investibile (buffer {int(BUFFER_SICUREZZA*100)}%): {importo_ottimale:.4f} USDT\n"
                            for v in volumi:
                                log_line += f"  - {v['pair']} {v['side']}_qty: {v['qty']:.4f}\n"
                            file_to_write = ANOMALIES_FILE if profit_perc_val > 50.0 else PROFITS_FILE
                            try:
                                with open(file_to_write, "a", encoding="utf-8") as f:
                                    f.write(log_line if profit_perc_val <= 50.0 else f"[ANOMALIA] {log_line}")
                            except Exception as e:
                                logger.error(f"Errore scrittura file opportunità: {e}")
                            # Logga sempre anche nel file delle profittevoli se sopra soglia
                            if profit_perc_val >= float(config.MIN_PROFIT_THRESHOLD) * 100:
                                try:
                                    save_profitable_opportunity(opp)
                                except Exception as e:
                                    logger.error(f"Errore scrittura file profittevoli: {e}")

                            # --- NOTIFICA TELEGRAM ROBUSTA ---
                            try:
                                msg = format_opportunity_message(opp, current_prices)
                                await send_telegram_notification(msg)
                            except Exception as e:
                                logger.error(f"Errore nella formattazione o invio Telegram per {path}: {e}\nDati: {opp}")

                except Exception as e:
                    logger.error(f"Errore nel processare il risultato del worker: {e}")
                    
        except asyncio.TimeoutError:
            logger.warning("⚠️ Timeout nell'analisi dei worker (30s)")
        
        # Aggiorna il contatore globale dei quasi-profittevoli
        total_low_profit_positive_found += aggregated_stats['low_profit']['positive']

        duration_ms = (time.perf_counter() - start_time) * 1000
        total_low_profit = aggregated_stats['low_profit']['negative'] + aggregated_stats['low_profit']['positive']
        total_sim_failures = aggregated_stats['simulation_failures']['total']

        # Log completo ma con frequenza ridotta per performance
        should_log_detailed = (
            total_profitable_found > 0 or 
            duration_ms > 5000 or 
            aggregated_stats['total_triangles'] > 10000  # Log se molti triangoli
        )
        
        if should_log_detailed:
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
        else:
            # Log sintetico per cicli normali
            logger.info(f"Analisi completata: {duration_ms:.1f}ms | Triangoli: {aggregated_stats['total_triangles']:,} | Opportunità: {total_profitable_found}")

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
    """Gestisce una singola connessione WebSocket con riconnessione e ottimizzazioni."""
    url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(symbols)}"
    reconnect_delay = 5
    max_reconnect_delay = 60
    
    while True:
        try:
            # Importa websockets solo quando necessario
            import websockets
            
            async with websockets.connect(
                url, 
                ping_interval=30,  # Aumentato da 20 a 30
                ping_timeout=60,
                close_timeout=10,
                max_size=2**20  # Limita dimensione messaggi
            ) as websocket:
                logger.info(f"Connessione WebSocket stabilita per {len(symbols)} simboli.")
                reconnect_delay = 5  # Reset delay su successo
                
                async for message in websocket:
                    await handle_message(message)
                    
        except Exception as e:
            logger.error(f"Errore WebSocket ({len(symbols)} simboli): {e}. Riconnessione tra {reconnect_delay}s.")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)  # Backoff esponenziale

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

def calcola_importo_ottimale_con_buffer(pairs, prices, symbol_info_map):
    """
    Calcola l'importo massimo investibile per un triangolo usando solo il best bid/ask e applicando un buffer di sicurezza.
    Restituisce l'importo ottimale e i volumi disponibili per ogni step.
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
            # Primo step: BUY (usiamo ask)
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
            # Secondo e terzo step: SELL (usiamo bid)
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
    
    # Stampa configurazione all'avvio
    config.print_config_summary()
    
    # Validazione configurazione
    if config.AUTO_TRADE_ENABLED:
        errors = config.validate_config()
        if errors:
            logger.error("❌ Errori di configurazione rilevati:")
            for error in errors:
                logger.error(f"  - {error}")
            logger.error("Il bot continuerà solo con l'analisi (trading disabilitato)")
            config.AUTO_TRADE_ENABLED = False
    
    getcontext().prec = 15
    bot_start_time = time.time()
    
    logger.info("Avvio programma di arbitraggio triangolare Binance...")
    await send_telegram_notification("🤖 Avvio del bot di arbitraggio...")

    symbols, symbol_info_map = await get_exchange_symbols()
    if not symbols:
        logger.error("Nessun simbolo ottenuto. Impossibile procedere.")
        return

    symbol_groups = [symbols[i:i + SYMBOLS_PER_CONNECTION] for i in range(0, len(symbols), SYMBOLS_PER_CONNECTION)]
    
    # Executor separati per analisi e trading
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
        logger.info("Programma interrotto manualmente.")
    except Exception as e:
        logger.critical(f"Errore critico non gestito in main: {e}", exc_info=True)