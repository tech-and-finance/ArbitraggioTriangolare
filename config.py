"""
Configuration for the Triangular Arbitrage Bot
Manages all settings for analysis and automated trading
"""

import os
from decimal import Decimal

# ============================================================================
# AUTOMATED TRADING CONFIGURATION
# ============================================================================

# SAFETY FLAG - MUST BE MANUALLY SET TO True
AUTO_TRADE_ENABLED = False

# TEST MODE - Does not execute real trades, only simulations
DRY_RUN_MODE = True

# MAXIMUM BUDGET PER TRADE (in USDT)
TRADE_BUDGET_USDT = Decimal("10")
# Budget for SIMULATION and example calculations
SIMULATION_BUDGET_USDT = Decimal("22")

# TRADING EXECUTION TIMEOUT (seconds)
TRADING_TIMEOUT = 30

# WebSocket Trading configuration
WEBSOCKET_TRADING_ENABLED = True  # Enable trading via WebSocket
WEBSOCKET_TIMEOUT = 5.0  # Timeout for WebSocket orders (seconds)
WEBSOCKET_MAX_FAILURES = 3  # Maximum number of failures before fallback
WEBSOCKET_PING_INTERVAL = 20  # WebSocket ping interval (seconds)

# Performance configuration
MAX_EXECUTION_TIME = 10.0  # Maximum execution time per trade (seconds)
MIN_PROFIT_THRESHOLD = Decimal('0.0005')  # Minimum profit for notification/trade (0.05%)
# Override via env var for benchmark/tuning without redeploy.
ARBITRAGE_CHECK_INTERVAL = int(os.environ.get('ARBITRAGE_CHECK_INTERVAL_OVERRIDE', '5'))

# ============================================================================
# SYSTEM AND PERFORMANCE CONFIGURATION
# ============================================================================

# CPU and process configuration
TOTAL_CORES = 16  # Total number of CPU cores (manually set)
TRADING_CORES = 1  # Cores for trading
WEB_CORES = 1  # Cores for the web server
ANALYSIS_CORES = 14 # Cores for analysis (16 - 1 - 1)

# Performance optimizations to reduce CPU load
# Override via env var for runtime benchmark/tuning without redeploy.
# 2026-05-05 benchmark showed 4 workers = sweet spot (avg 90ms/cycle).
# 8 workers +28%, 14 workers +75% due to IPC overhead (pickle symbol_info_map per worker call).
# See Vault/Projects/arbitraggio-triangolare-deep-analysis-2026-05-05.md § Benchmark parallelismo.
MAX_CONCURRENT_ANALYSIS = int(os.environ.get('MAX_CONCURRENT_ANALYSIS_OVERRIDE', '4'))
ANALYSIS_BATCH_SIZE = 200  # Batch size for analysis
PRICE_CACHE_TTL = 5  # Price cache TTL (seconds)

# ============================================================================
# BINANCE API CONFIGURATION
# ============================================================================

# Load API keys from environment variables for security
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', '')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '')

# Binance API URLs
BINANCE_API_URL = "https://api.binance.com"
BINANCE_TESTNET_URL = "https://testnet.binance.vision"

# Use testnet if DRY_RUN_MODE is True
def get_binance_url():
    """Returns the correct Binance URL based on the mode"""
    return BINANCE_TESTNET_URL if DRY_RUN_MODE else BINANCE_API_URL

# ============================================================================
# TRADING LOGGING CONFIGURATION
# ============================================================================

# Log files for trading
TRADING_LOG_FILE = "trading_log.txt"
TRADING_ERROR_LOG_FILE = "trading_errors.txt"

# ============================================================================
# TELEGRAM CONFIGURATION
# ============================================================================

# Telegram configuration
TELEGRAM_BOT_TOKEN = 'YOUR_BOT_TOKEN'
TELEGRAM_CHAT_ID = 'YOUR_CHAT_ID'
TELEGRAM_COOLDOWN = 0  # Cooldown between notifications (seconds)

# ============================================================================
# CONFIGURATION VALIDATION
# ============================================================================

def validate_config():
    """Validates the configuration and returns errors if present"""
    errors = []

    if AUTO_TRADE_ENABLED:
        if not BINANCE_API_KEY:
            errors.append("BINANCE_API_KEY not set")
        if not BINANCE_SECRET_KEY:
            errors.append("BINANCE_SECRET_KEY not set")
        if TRADE_BUDGET_USDT <= 0:
            errors.append("TRADE_BUDGET_USDT must be > 0")

    return errors

def print_config_summary():
    """Prints a summary of the configuration"""
    print("=== AUTOMATED TRADING CONFIGURATION ===")
    print(f"Trading Enabled: {'✅ YES' if AUTO_TRADE_ENABLED else '❌ NO'}")
    print(f"Test Mode: {'✅ YES' if DRY_RUN_MODE else '❌ NO'}")
    print(f"Budget per Trade: {TRADE_BUDGET_USDT} USDT")
    print(f"Trading Timeout: {TRADING_TIMEOUT} seconds")
    print(f"Total Cores: {TOTAL_CORES}")
    print(f"Analysis Cores: {ANALYSIS_CORES}")
    print(f"Trading Cores: {TRADING_CORES}")
    print("==========================================")

    if AUTO_TRADE_ENABLED:
        errors = validate_config()
        if errors:
            print("⚠️ CONFIGURATION ERRORS:")
            for error in errors:
                print(f"  - {error}")
        else:
            print("✅ Configuration valid")

# H2 fix: removed duplicate block overwriting initial definitions (drift risk).
# All constants (AUTO_TRADE_ENABLED, DRY_RUN_MODE, TRADE_BUDGET_USDT, SIMULATION_BUDGET_USDT)
# are now defined ONCE in the main block at the top of the file.
