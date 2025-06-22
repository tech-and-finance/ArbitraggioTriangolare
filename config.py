"""
Configurazione per il Bot di Arbitraggio Triangolare
Gestisce tutte le impostazioni per l'analisi e il trading automatico
"""

import os
from decimal import Decimal

# ============================================================================
# CONFIGURAZIONE TRADING AUTOMATICO
# ============================================================================

# FLAG DI SICUREZZA - DEVE ESSERE IMPOSTATO MANUALMENTE SU True
AUTO_TRADE_ENABLED = False

# MODALITÀ TEST - Non esegue trade reali, solo simulazioni
DRY_RUN_MODE = True

# BUDGET MASSIMO PER TRADE (in USDT)
TRADE_BUDGET_USDT = Decimal("10")

# TIMEOUT PER L'ESECUZIONE DEL TRADING (secondi)
TRADING_TIMEOUT = 30

# Configurazione WebSocket Trading
WEBSOCKET_TRADING_ENABLED = True  # Abilita trading via WebSocket
WEBSOCKET_TIMEOUT = 5.0  # Timeout per ordini WebSocket (secondi)
WEBSOCKET_MAX_FAILURES = 3  # Numero massimo fallimenti prima del fallback
WEBSOCKET_PING_INTERVAL = 20  # Intervallo ping WebSocket (secondi)

# Configurazione performance
MAX_EXECUTION_TIME = 10.0  # Tempo massimo di esecuzione per trade (secondi)
MIN_PROFIT_THRESHOLD = Decimal('0.0005')  # Profitto minimo per notifica/trade (0.05%)
ARBITRAGE_CHECK_INTERVAL = 5  # Secondi tra i cicli di analisi del mercato

# ============================================================================
# CONFIGURAZIONE SISTEMA E PERFORMANCE
# ============================================================================

# Configurazione CPU e processi
TOTAL_CORES = 16  # Numero totale di core CPU (impostato manualmente)
TRADING_CORES = 1  # Core per il trading
WEB_CORES = 1  # Core per il web server
ANALYSIS_CORES = 14 # Core per l'analisi (16 - 1 - 1)

# Ottimizzazioni performance per ridurre carico CPU
MAX_CONCURRENT_ANALYSIS = 2  # Limita analisi concorrenti
ANALYSIS_BATCH_SIZE = 200  # Dimensione batch per analisi
PRICE_CACHE_TTL = 5  # TTL cache prezzi (secondi)

# ============================================================================
# CONFIGURAZIONE BINANCE API
# ============================================================================

# Carica le chiavi API da variabili d'ambiente per sicurezza
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', '')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '')

# URL API Binance
BINANCE_API_URL = "https://api.binance.com"
BINANCE_TESTNET_URL = "https://testnet.binance.vision"

# Usa testnet se DRY_RUN_MODE è True
def get_binance_url():
    """Restituisce l'URL corretto per Binance in base alla modalità"""
    return BINANCE_TESTNET_URL if DRY_RUN_MODE else BINANCE_API_URL

# ============================================================================
# CONFIGURAZIONE LOGGING TRADING
# ============================================================================

# File di log per il trading
TRADING_LOG_FILE = "trading_log.txt"
TRADING_ERROR_LOG_FILE = "trading_errors.txt"

# ============================================================================
# CONFIGURAZIONE TELEGRAM
# ============================================================================

# Configurazione Telegram
TELEGRAM_BOT_TOKEN = 'YOUR_BOT_TOKEN'
TELEGRAM_CHAT_ID = 'YOUR_CHAT_ID'
TELEGRAM_COOLDOWN = 0  # Cooldown tra notifiche (secondi)

# ============================================================================
# VALIDAZIONE CONFIGURAZIONE
# ============================================================================

def validate_config():
    """Valida la configurazione e restituisce errori se presenti"""
    errors = []
    
    if AUTO_TRADE_ENABLED:
        if not BINANCE_API_KEY:
            errors.append("BINANCE_API_KEY non impostata")
        if not BINANCE_SECRET_KEY:
            errors.append("BINANCE_SECRET_KEY non impostata")
        if TRADE_BUDGET_USDT <= 0:
            errors.append("TRADE_BUDGET_USDT deve essere > 0")
    
    return errors

def print_config_summary():
    """Stampa un riepilogo della configurazione"""
    print("=== CONFIGURAZIONE TRADING AUTOMATICO ===")
    print(f"Trading Abilitato: {'✅ SÌ' if AUTO_TRADE_ENABLED else '❌ NO'}")
    print(f"Modalità Test: {'✅ SÌ' if DRY_RUN_MODE else '❌ NO'}")
    print(f"Budget per Trade: {TRADE_BUDGET_USDT} USDT")
    print(f"Timeout Trading: {TRADING_TIMEOUT} secondi")
    print(f"Core Totali: {TOTAL_CORES}")
    print(f"Core Analisi: {ANALYSIS_CORES}")
    print(f"Core Trading: {TRADING_CORES}")
    print("==========================================")
    
    if AUTO_TRADE_ENABLED:
        errors = validate_config()
        if errors:
            print("⚠️ ERRORI DI CONFIGURAZIONE:")
            for error in errors:
                print(f"  - {error}")
        else:
            print("✅ Configurazione valida")

# Configurazione Trading
AUTO_TRADE_ENABLED = False  # Abilita il trading automatico
DRY_RUN_MODE = True  # Modalità test (usa testnet Binance)
TRADE_BUDGET_USDT = Decimal('10')  # Budget per trade REALE in USDT
SIMULATION_BUDGET_USDT = Decimal('22') # Budget per la SIMULAZIONE e i calcoli di esempio 