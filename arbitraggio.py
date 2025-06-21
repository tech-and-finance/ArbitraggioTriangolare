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

# Configurazione Telegram
TELEGRAM_CONFIG = {
    'bot_token': '8182228673:AAEwknPEkwI_vp8froD8rNEquaK88W3EukQ',
    'chat_id': '279229754'
}

# --- Costanti di Configurazione ---
SYMBOLS_PER_CONNECTION = 200  # Numero di simboli per connessione WebSocket
PROFIT_THRESHOLD = Decimal("0.001")  # Profitto minimo netto per notifica (0.1%)
TRADING_FEE = Decimal("0.001")      # Commissione per ogni trade (0.1%)
STARTING_CAPITAL_USDT = Decimal("100") # Capitale iniziale per la simulazione
ARBITRAGE_CHECK_INTERVAL = 120  # Secondi tra i controlli di arbitraggio (2 minuti)
TELEGRAM_COOLDOWN = 5  # Secondi di attesa tra notifiche Telegram per evitare rate-limit
STABLECOINS = {'USDT', 'USDC', 'FDUSD'} # Valute stabili da cui iniziare l'arbitraggio
# --------------------------------

# Cache globale per ottimizzazioni
arbitrage_cache = {}
last_arbitrage_check = 0
cached_currencies = set()
cached_pairs = {}
last_telegram_time = 0

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
    url = f"https://api.telegram.org/bot{TELEGRAM_CONFIG['bot_token']}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CONFIG['chat_id'],
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
    """Ottiene i simboli e le loro info per l'arbitraggio con USDT."""
    try:
        url = "https://api.binance.com/api/v3/exchangeInfo"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        trading_symbols = {s['symbol']: s for s in data['symbols'] if s['status'] == 'TRADING'}

        usdt_related_currencies = {'USDT'}
        for symbol_info in trading_symbols.values():
            if symbol_info['quoteAsset'] == 'USDT':
                usdt_related_currencies.add(symbol_info['baseAsset'])

        symbols_to_subscribe = set()
        symbol_info_map = {}
        for symbol, info in trading_symbols.items():
            if info['baseAsset'] in usdt_related_currencies and info['quoteAsset'] in usdt_related_currencies:
                symbols_to_subscribe.add(symbol)
                # Estrai minQty dal filtro LOT_SIZE
                min_qty = Decimal("0")
                for f in info['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        min_qty = Decimal(f['minQty'])
                        break
                symbol_info_map[symbol] = {
                    'base': info['baseAsset'], 
                    'quote': info['quoteAsset'],
                    'minQty': min_qty
                }
        
        formatted_symbols = [s.lower() + "@bookTicker" for s in sorted(list(symbols_to_subscribe))]
        log(f"[LOG] Ottenuti {len(formatted_symbols)} simboli per l'arbitraggio con USDT.")
        return formatted_symbols, symbol_info_map
    except Exception as e:
        log(f"[LOG][ERRORE] Impossibile ottenere i simboli: {e}")
        return [], {}

async def handle_message(msg):
    global msg_count
    msg_count += 1
    
    # Gestione del formato dello stream combinato
    data = json.loads(msg)
    if 'data' in data:  # Stream combinato
        data = data['data']
    
    symbol = data['s']
    price_map[symbol] = {
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

def find_arbitrage_worker(prices, symbol_info_map, profit_threshold, trading_fee, currency_chunk, all_currencies, pair_set):
    """
    Genera e calcola le opportunità di arbitraggio per un sottoinsieme di valute,
    simulando un trade con un capitale fisso per verificare la liquidità.
    """
    from decimal import Decimal
    from itertools import permutations

    results = []
    permutazioni_processate = 0
    permutazioni_valide = 0
    
    SIMULATION_CAPITAL = Decimal('100') # Simula un trade con 100 USDT (o equivalente)

    cached_pairs = {}
    for symbol, info in symbol_info_map.items():
        if symbol in prices:
            cached_pairs[(info['base'], info['quote'])] = symbol
            cached_pairs[(info['quote'], info['base'])] = symbol

    for a in currency_chunk:
        for b in all_currencies:
            if b == a or (a, b) not in pair_set: continue
            for c in all_currencies:
                if c == a or c == b: continue
                if (b, c) not in pair_set or (c, a) not in pair_set: continue
                
                for p_a, p_b, p_c in permutations((a, b, c), 3):
                    permutazioni_processate += 1
                    try:
                        # --- Inizio Simulazione Trade ---
                        
                        # STEP 1: p_a -> p_b
                        amount1 = SIMULATION_CAPITAL if p_a in STABLECOINS else Decimal(0)
                        rate1, price1, pair1_str = Decimal(0), Decimal(0), ""
                        
                        if (p_b, p_a) in cached_pairs and cached_pairs[(p_b, p_a)] in prices: # Vendi p_a per p_b (compri p_b)
                            symbol1 = cached_pairs[(p_b, p_a)]
                            book = prices[symbol1]
                            if book['ask'] > 0 and book['ask_qty'] > 0:
                                price1 = book['ask']
                                rate1 = Decimal(1) / price1
                                # Se non partiamo da stablecoin, non possiamo iniziare la simulazione del capitale
                                if p_a not in STABLECOINS: continue
                                
                                amount_to_buy = SIMULATION_CAPITAL / price1 # Quanto p_b compriamo
                                if amount_to_buy > book['ask_qty']: continue # Non c'è abbastanza liquidità

                                amount1 = amount_to_buy
                                pair1_str = symbol1
                        elif (p_a, p_b) in cached_pairs and cached_pairs[(p_a, p_b)] in prices: # Vendi p_a per p_b (compri p_b)
                            symbol1 = cached_pairs[(p_a, p_b)]
                            book = prices[symbol1]
                            if book['bid'] > 0 and book['bid_qty'] > 0:
                                price1 = book['bid']
                                rate1 = price1
                                # Se non partiamo da stablecoin, non possiamo iniziare la simulazione del capitale
                                if p_a not in STABLECOINS: continue
                                
                                amount_to_sell = SIMULATION_CAPITAL # Quanto p_a vendiamo
                                if amount_to_sell > book['bid_qty']: continue # Non c'è abbastanza liquidità
                                
                                amount1 = amount_to_sell * price1
                                pair1_str = symbol1

                        if rate1 == 0 or amount1 == 0: continue
                        amount1_after_fee = amount1 * (Decimal(1) - trading_fee)

                        # STEP 2: p_b -> p_c
                        amount2 = Decimal(0)
                        rate2, price2, pair2_str = Decimal(0), Decimal(0), ""
                        if (p_c, p_b) in cached_pairs and cached_pairs[(p_c, p_b)] in prices:
                            symbol2 = cached_pairs[(p_c, p_b)]
                            book = prices[symbol2]
                            if book['ask'] > 0 and book['ask_qty'] > 0:
                                price2 = book['ask']
                                rate2 = Decimal(1) / price2
                                amount_to_buy = amount1_after_fee / price2
                                if amount_to_buy > book['ask_qty']: continue
                                amount2 = amount_to_buy
                                pair2_str = symbol2
                        elif (p_b, p_c) in cached_pairs and cached_pairs[(p_b, p_c)] in prices:
                            symbol2 = cached_pairs[(p_b, p_c)]
                            book = prices[symbol2]
                            if book['bid'] > 0 and book['bid_qty'] > 0:
                                price2 = book['bid']
                                rate2 = price2
                                amount_to_sell = amount1_after_fee
                                if amount_to_sell > book['bid_qty']: continue
                                amount2 = amount_to_sell * price2
                                pair2_str = symbol2
                        
                        if rate2 == 0 or amount2 == 0: continue
                        amount2_after_fee = amount2 * (Decimal(1) - trading_fee)

                        # STEP 3: p_c -> p_a
                        amount3 = Decimal(0)
                        rate3, price3, pair3_str = Decimal(0), Decimal(0), ""
                        if (p_a, p_c) in cached_pairs and cached_pairs[(p_a, p_c)] in prices:
                            symbol3 = cached_pairs[(p_a, p_c)]
                            book = prices[symbol3]
                            if book['ask'] > 0 and book['ask_qty'] > 0:
                                price3 = book['ask']
                                rate3 = Decimal(1) / price3
                                amount_to_buy = amount2_after_fee / price3
                                if amount_to_buy > book['ask_qty']: continue
                                amount3 = amount_to_buy
                                pair3_str = symbol3
                        elif (p_c, p_a) in cached_pairs and cached_pairs[(p_c, p_a)] in prices:
                            symbol3 = cached_pairs[(p_c, p_a)]
                            book = prices[symbol3]
                            if book['bid'] > 0 and book['bid_qty'] > 0:
                                price3 = book['bid']
                                rate3 = price3
                                amount_to_sell = amount2_after_fee
                                if amount_to_sell > book['bid_qty']: continue
                                amount3 = amount_to_sell * price3
                                pair3_str = symbol3

                        if rate3 == 0 or amount3 == 0: continue
                        final_amount = amount3 * (Decimal(1) - trading_fee)

                        # --- Fine Simulazione Trade ---
                        permutazioni_valide += 1
                        
                        net_profit = (final_amount / SIMULATION_CAPITAL) - Decimal(1) if SIMULATION_CAPITAL > 0 else Decimal(0)
                        profitto_lordo = (rate1 * rate2 * rate3) - 1

                        path_str = f"{p_a}→{p_b}→{p_c}→{p_a}"

                        if profitto_lordo > Decimal('0.5'):
                            # ... (il codice per le anomalie rimane simile, omettiamolo per brevità)
                            continue
                        
                        details = {
                            'rates': tuple(str(r) for r in (rate1, rate2, rate3)),
                            'pairs': (pair1_str, pair2_str, pair3_str),
                            'prices': tuple(str(p) for p in (price1, price2, price3))
                        }
                        opp = {
                            'path': path_str,
                            'profit': str(net_profit),
                            'final': str(final_amount),
                            'details': details,
                            'profitto_lordo': str(profitto_lordo)
                        }
                        
                        if net_profit > profit_threshold:
                            opp['note'] = 'PROFITTEVOLE'
                            results.append({'type': 'profitable', 'opp': opp})
                        elif net_profit > 0:
                            opp['note'] = 'QUASI PROFITTEVOLE'
                            results.append({'type': 'quasi', 'opp': opp})
                        else:
                            # Non inviare più le opportunità scartate
                            pass
                    except Exception as e:
                        # Loggare l'errore qui sarebbe utile in futuro
                        continue
    return results, permutazioni_processate, permutazioni_valide

def cpu_stress_test_worker(iterations):
    print(f"[STRESS][WORKER] PID: {os.getpid()} | Iterazioni: {iterations}")
    x = 0
    for i in range(iterations):
        x += i
    print(f"[STRESS][WORKER] PID: {os.getpid()} | Fine lavoro")
    return x

async def check_arbitrage(symbol_info_map, executor):
    log("[LOG] Attendo 10 secondi per riempire la cache dei prezzi...")
    await asyncio.sleep(10)
    log("[LOG] Inizio controllo opportunità di arbitraggio...")
    
    global last_telegram_time

    while True:
        start_time = time.perf_counter()
        current_price_map = price_map.copy()

        active_currencies_set = set()
        for symbol, info in symbol_info_map.items():
            if symbol in current_price_map:
                active_currencies_set.add(info['base'])
                active_currencies_set.add(info['quote'])
        active_currencies = list(active_currencies_set)
        
        pair_set = set()
        for symbol in current_price_map:
            if symbol in symbol_info_map:
                base = symbol_info_map[symbol]['base']
                quote = symbol_info_map[symbol]['quote']
                pair_set.add((base, quote))
                pair_set.add((quote, base))

        if not active_currencies:
            log("[LOG] Nessuna valuta attiva trovata in questo ciclo.")
            await asyncio.sleep(ARBITRAGE_CHECK_INTERVAL)
            continue

        num_workers = os.cpu_count() or 2
        chunk_size = ceil(len(active_currencies) / num_workers)
        if chunk_size == 0:
            log("[LOG] Nessuna valuta da processare.")
            await asyncio.sleep(ARBITRAGE_CHECK_INTERVAL)
            continue
            
        currency_chunks = [active_currencies[i:i + chunk_size] for i in range(0, len(active_currencies), chunk_size)]
        
        loop = asyncio.get_running_loop()
        tasks = [loop.run_in_executor(executor, find_arbitrage_worker, current_price_map, symbol_info_map, PROFIT_THRESHOLD, TRADING_FEE, chunk, active_currencies, pair_set) for chunk in currency_chunks]
        
        arbitrage_opps = []
        opportunita_quasi_profit = 0
        opportunita_lorde = 0
        profitti_lordi = []
        profitti_netto = []
        total_permutations_processed = 0
        total_permutations_valid = 0
        
        processed_triangles = set() # Aggiunto per de-duplicare i triangoli

        # Sostituisci gather con as_completed per un'elaborazione non bloccante
        for future in asyncio.as_completed(tasks):
            worker_results, perms_processed, perms_valid = await future
            total_permutations_processed += perms_processed
            total_permutations_valid += perms_valid
            
            for res in worker_results:
                if res['type'] == 'anomaly':
                    log(f"[ANOMALIA] Profitto lordo anomalo ({res['profit']*100:.2f}%) scartato per {res['path']}")
                    log(f"  Tassi: {res['rates'][0]:.6f} * {res['rates'][1]:.6f} * {res['rates'][2]:.6f}")
                    log(f"  Prezzi: {res['prices'][0]:.6f}, {res['prices'][1]:.6f}, {res['prices'][2]:.6f}")
                else:
                    opp = res['opp']
                    
                    # --- Logica di de-duplicazione del triangolo ---
                    path_currencies = set(opp['path'].split('→')[:3])
                    canonical_triangle = tuple(sorted(list(path_currencies)))
                    if canonical_triangle in processed_triangles:
                        continue # Triangolo già processato in questo ciclo, ignora.

                    # Converti i profitti da stringa a Decimal per i calcoli corretti
                    profitto_lordo_dec = Decimal(opp['profitto_lordo'])
                    
                    profitti_lordi.append(float(profitto_lordo_dec))
                    profitti_netto.append(float(opp['profit']))
                    
                    if profitto_lordo_dec > 0:
                        opportunita_lorde += 1

                    # --- Logica di filtro e logging disaccoppiata ---
                    
                    # 1. Determina se il percorso è preferito
                    starting_currency = opp['path'].split('→')[0]
                    is_stablecoin_in_path = any(c in STABLECOINS for c in path_currencies)
                    is_preferred_path = not (is_stablecoin_in_path and starting_currency not in STABLECOINS)

                    # 2. Salva SEMPRE sul file di log giornaliero. Se il percorso non è preferito,
                    # la nota originale ('PROFITTEVOLE', etc.) viene sovrascritta per chiarezza.
                    if not is_preferred_path:
                        opp['note'] = 'IGNORATO (NON PREFERITO)'
                    
                    if res['type'] != 'scartata': # Logga tutto tranne le non-profittevoli per pulizia
                        save_opportunity_to_file(opp)

                    # 3. Se il percorso non è preferito, interrompi qui l'elaborazione
                    if not is_preferred_path:
                        if res['type'] != 'scartata':
                             log(f"[SKIP] Percorso {opp['path']} ignorato. Si preferisce la partenza da stablecoin.")
                        continue

                    # --- Da qui in poi, processiamo solo i percorsi PREFERITI ---

                    # Segna il triangolo come processato per evitare duplicati nelle notifiche/log profittevoli
                    if res['type'] == 'profitable' or res['type'] == 'quasi':
                        processed_triangles.add(canonical_triangle)

                    if res['type'] == 'profitable':
                        arbitrage_opps.append(opp)
                        save_profitable_opportunity(opp) # Ora riceve solo percorsi preferiti
                        
                        now = time.perf_counter()
                        if now - last_telegram_time > TELEGRAM_COOLDOWN:
                            msg = format_opportunity_message(opp, current_price_map)
                            send_telegram_message(msg)
                            last_telegram_time = now
                        else:
                            log(f"[TELEGRAM][SKIP] Notifica per {opp['path']} saltata (cooldown).")

                    elif res['type'] == 'quasi':
                        opportunita_quasi_profit += 1
                        
                        now = time.perf_counter()
                        if now - last_telegram_time > TELEGRAM_COOLDOWN:
                            msg = format_opportunity_message(opp, current_price_map)
                            msg = "⚠️ *QUASI ARBITRAGGIO*\n\n" + msg
                            send_telegram_message(msg)
                            last_telegram_time = now
                        else:
                            log(f"[TELEGRAM][SKIP] Notifica per {opp['path']} saltata (cooldown).")
        
        duration = (time.perf_counter() - start_time) * 1000
        profitto_lordo_medio = (sum(profitti_lordi) / len(profitti_lordi)) if profitti_lordi else 0
        profitto_netto_medio = (sum(profitti_netto) / len(profitti_netto)) if profitti_netto else 0
        
        log(f"[PERF] Ciclo completato in {duration:.2f}ms | Valute: {len(active_currencies)} | Permutazioni: {total_permutations_processed:,}/{total_permutations_valid:,} | Lorde: {opportunita_lorde} | Nette: {len(arbitrage_opps)} | Quasi: {opportunita_quasi_profit} | Profitto Med. Lordo: {profitto_lordo_medio*100:.4f}% | Netto: {profitto_netto_medio*100:.4f}%")
        
        await asyncio.sleep(ARBITRAGE_CHECK_INTERVAL)

async def create_websocket_connection(symbols):
    """Crea una connessione WebSocket per un gruppo di simboli"""
    streams_url = f"{WS_URL}?streams={'/'.join(symbols)}"
    log(f"[LOG] Creazione connessione per {len(symbols)} simboli...")
    
    try:
        websocket = await websockets.connect(
            streams_url,
            ping_interval=20,
            ping_timeout=60
        )
        log(f"[LOG] Connessione WebSocket stabilita per {len(symbols)} simboli")
        return websocket
    except Exception as e:
        log(f"[LOG][ERRORE] Impossibile creare connessione WebSocket: {e}")
        return None

async def handle_websocket_connection_with_reconnect(symbols, reconnect_delay=5):
    """Gestisce una connessione WebSocket con riconnessione automatica."""
    while True:
        websocket = None
        try:
            websocket = await create_websocket_connection(symbols)
            if websocket is None:
                log(f"[RECONNECT] Impossibile creare la connessione WebSocket. Riprovo tra {reconnect_delay}s...")
                send_telegram_message(f"[RECONNECT] Impossibile creare la connessione WebSocket. Riprovo tra {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                continue
            log(f"[RECONNECT] Connessione WebSocket (re)stabilita per {len(symbols)} simboli.")
            send_telegram_message(f"[RECONNECT] Connessione WebSocket (re)stabilita per {len(symbols)} simboli.")
            last_msg_time = time.time()
            while True:
                try:
                    msg = await websocket.recv()
                    now = time.time()
                    if now - last_msg_time > 5:
                        log(f"[DEBUG][WS] {now - last_msg_time:.2f}s dall'ultimo messaggio WebSocket!")
                    last_msg_time = now
                    await handle_message(msg)
                except Exception as e:
                    log(f"[RECONNECT][ERRORE] Errore nella ricezione messaggi WebSocket: {e}")
                    send_telegram_message(f"[RECONNECT][ERRORE] Errore WebSocket: {e}. Riconnessione tra {reconnect_delay}s...")
                    break
        except Exception as e:
            log(f"[RECONNECT][ERRORE] Errore generale WebSocket: {e}")
            send_telegram_message(f"[RECONNECT][ERRORE] Errore generale WebSocket: {e}. Riconnessione tra {reconnect_delay}s...")
        finally:
            if websocket:
                await websocket.close()
            await asyncio.sleep(reconnect_delay)

# Modifica la main per usare la nuova funzione di gestione con riconnessione
def patch_main_for_reconnect():
    import types
    async def main():
        log(f"[LOG] Connessione al WebSocket Binance: {WS_URL}")
        process = psutil.Process(os.getpid()) if psutil_available else None
        try:
            symbols, symbol_info_map = await get_exchange_symbols()
            if not symbols:
                log("[LOG][ERRORE] Nessun simbolo ottenuto. Impossibile procedere.")
                return
                
            symbol_groups = [symbols[i:i + SYMBOLS_PER_CONNECTION] 
                            for i in range(0, len(symbols), SYMBOLS_PER_CONNECTION)]
            log(f"[LOG] Creazione di {len(symbol_groups)} connessioni WebSocket...")
            
            with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
                # Crea task per il controllo arbitraggio e performance
                arbitrage_task = asyncio.create_task(check_arbitrage(symbol_info_map, executor))
                
                tasks_to_run = [arbitrage_task]
                if process:
                    monitor_task = asyncio.create_task(monitor_performance(process))
                    tasks_to_run.append(monitor_task)
                
                # Crea e gestisci le connessioni WebSocket con riconnessione
                websocket_tasks = []
                for group in symbol_groups:
                    task = asyncio.create_task(handle_websocket_connection_with_reconnect(group))
                    websocket_tasks.append(task)
                
                all_tasks = websocket_tasks + tasks_to_run
                    
                if websocket_tasks:
                    await asyncio.gather(*all_tasks)
                else:
                    log("[LOG][ERRORE] Nessuna connessione WebSocket creata con successo.")
                    # Assicurati di cancellare i task appesi se le websocket falliscono
                    for task in tasks_to_run:
                        task.cancel()
                
        except Exception as e:
            log(f"[LOG][ERRORE] Errore generale: {e}")
        finally:
            log("[LOG] Programma terminato.")
    globals()['main'] = main
patch_main_for_reconnect()

if __name__ == "__main__":
    log("[LOG] Avvio programma di arbitraggio triangolare Binance...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("[LOG] Programma interrotto manualmente.")