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

# ============================================================================
# CONFIGURAZIONE CPU
# ============================================================================

import os
TOTAL_CORES = os.cpu_count()
TRADING_CORES = 1
ANALYSIS_CORES = max(1, TOTAL_CORES - TRADING_CORES)

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
TRADING_LOG_FILE = "trading_results.txt"
TRADING_ERROR_LOG_FILE = "trading_errors.txt"

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