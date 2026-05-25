#!/usr/bin/env bash
# Pre-flight sanity check for the triangular arbitrage bot.
#
# Verifies AWS region, time sync, Binance connectivity, Python env,
# env vars, Telegram bot reachability, instance resources, and tmux.
# Run this after fresh provisioning (AWS EC2, VPS, or local) and before
# any long-running benchmark.
#
# Usage:
#   bash scripts/sanity-check.sh
#
# Exit code:
#   0  all checks passed (possibly with warnings)
#   1  at least one FAIL — resolve before running the bot
#
# Env vars expected to be exported (load from ~/.env or shell):
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BINANCE_API_KEY, BINANCE_SECRET_KEY
#   MAX_CONCURRENT_ANALYSIS_OVERRIDE (optional, defaults to 4)
#   ARBITRAGE_CHECK_INTERVAL_OVERRIDE (optional, defaults to 5)

set -u
FAILED=0
WARNED=0

if [ -t 1 ]; then
    GREEN=$(tput setaf 2); RED=$(tput setaf 1); YELLOW=$(tput setaf 3); BOLD=$(tput bold); RESET=$(tput sgr0)
else
    GREEN=""; RED=""; YELLOW=""; BOLD=""; RESET=""
fi

pass() { echo "${GREEN}[PASS]${RESET} $1"; }
fail() { echo "${RED}[FAIL]${RESET} $1"; FAILED=$((FAILED+1)); }
warn() { echo "${YELLOW}[WARN]${RESET} $1"; WARNED=$((WARNED+1)); }
section() { echo ""; echo "${BOLD}=== $1 ===${RESET}"; }

# A. AWS region (only when running on EC2 — silently skip otherwise)
section "A. AWS region"
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60" --max-time 2 2>/dev/null)
if [ -n "$TOKEN" ]; then
    REGION=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/region --max-time 2 2>/dev/null)
    AZ=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone --max-time 2 2>/dev/null)
    if [ "$REGION" = "ap-northeast-1" ]; then
        pass "Region: $REGION (AZ: $AZ)"
    else
        fail "Region: '$REGION' (expected ap-northeast-1) — instance is in the WRONG region for low-latency Binance access"
    fi
else
    warn "Not running on EC2 (or metadata service unreachable) — skipping region check"
fi

# B. Time sync (critical for Binance HMAC signature window)
section "B. Time sync"
if command -v timedatectl >/dev/null 2>&1; then
    SYNC=$(timedatectl show --property=NTPSynchronized --value 2>/dev/null)
    NTP=$(timedatectl show --property=NTP --value 2>/dev/null)
    if [ "$SYNC" = "yes" ] && [ "$NTP" = "yes" ]; then
        pass "NTP synchronized"
    else
        fail "NTP not synchronized (sync=$SYNC, ntp=$NTP) — sudo systemctl enable --now systemd-timesyncd"
    fi
else
    warn "timedatectl not available — skipping NTP check (verify clock manually)"
fi

# C. Binance connectivity + clock drift vs server
section "C. Binance connectivity + clock drift"
SERVER_TIME=$(curl -s https://api.binance.com/api/v3/time --max-time 5 2>/dev/null | grep -oE '"serverTime":[0-9]+' | cut -d: -f2)
# Use python for portable ms timestamp; `date +%s%3N` is unreliable across distros
# (Ubuntu 26.04 GNU coreutils returns nanoseconds with %3N as suffix instead of truncated ms).
LOCAL_TIME=$(python3 -c "import time; print(int(time.time() * 1000))" 2>/dev/null || date +%s%3N)
if [ -n "$SERVER_TIME" ]; then
    DRIFT=$((SERVER_TIME - LOCAL_TIME))
    ABS_DRIFT=${DRIFT#-}
    if [ "$ABS_DRIFT" -lt 100 ]; then
        pass "Binance REST reachable, clock drift: ${DRIFT}ms"
    elif [ "$ABS_DRIFT" -lt 1000 ]; then
        warn "Binance reachable, drift ${DRIFT}ms (>100ms but under signature window 1000ms)"
    else
        fail "Drift ${DRIFT}ms exceeds signature window — orders will fail"
    fi
else
    fail "Binance REST unreachable"
fi

if command -v nc >/dev/null 2>&1; then
    if nc -zw5 stream.binance.com 9443 >/dev/null 2>&1; then
        pass "Binance WebSocket port 9443 reachable"
    else
        fail "Binance WebSocket port 9443 unreachable"
    fi
else
    warn "nc not installed (apt install netcat-openbsd) — skipping WebSocket TCP check"
fi

if curl -sf https://testnet.binance.vision/api/v3/time --max-time 5 >/dev/null 2>&1; then
    pass "Binance testnet reachable (dry-run target)"
else
    fail "Binance testnet unreachable"
fi

# D. Python + dependencies
section "D. Python + dependencies"
if [ -f venv/bin/activate ]; then
    # shellcheck disable=SC1091
    source venv/bin/activate
    PY_VERSION=$(python --version 2>&1 | awk '{print $2}')
    if [[ "$PY_VERSION" == 3.11.* ]] || [[ "$PY_VERSION" == 3.12.* ]]; then
        pass "Python $PY_VERSION (venv active)"
    else
        warn "Python $PY_VERSION (expected 3.11.x or 3.12.x)"
    fi
    if python -c "import websockets, aiohttp, requests; from binance.client import Client; from decimal import Decimal" 2>/dev/null; then
        WS_VER=$(python -c "import websockets; print(websockets.__version__)")
        AIO_VER=$(python -c "import aiohttp; print(aiohttp.__version__)")
        pass "Imports OK (websockets=$WS_VER, aiohttp=$AIO_VER)"
    else
        fail "Import test failed — pip install -r requirements.txt"
    fi
else
    fail "venv not found in $(pwd)/venv/ — python3.11 -m venv venv && pip install -r requirements.txt"
fi

# E. Env vars (lengths only, never values)
section "E. Env vars (lengths only)"
check_var() {
    local name="$1"
    local min_len="$2"
    local val="${!name:-}"
    if [ -z "$val" ]; then
        fail "$name not set"
    elif [ "${#val}" -lt "$min_len" ]; then
        warn "$name length ${#val} (expected at least $min_len)"
    else
        pass "$name length ${#val}"
    fi
}
check_var TELEGRAM_BOT_TOKEN 40
check_var TELEGRAM_CHAT_ID 6
check_var BINANCE_API_KEY 60
check_var BINANCE_SECRET_KEY 60
[ -n "${MAX_CONCURRENT_ANALYSIS_OVERRIDE:-}" ] && pass "MAX_CONCURRENT_ANALYSIS_OVERRIDE=$MAX_CONCURRENT_ANALYSIS_OVERRIDE" || warn "MAX_CONCURRENT_ANALYSIS_OVERRIDE not set (default 4)"
[ -n "${ARBITRAGE_CHECK_INTERVAL_OVERRIDE:-}" ] && pass "ARBITRAGE_CHECK_INTERVAL_OVERRIDE=$ARBITRAGE_CHECK_INTERVAL_OVERRIDE" || warn "ARBITRAGE_CHECK_INTERVAL_OVERRIDE not set (default 5)"

# F. Telegram bot
section "F. Telegram bot"
if [ -n "${TELEGRAM_BOT_TOKEN:-}" ]; then
    TG_RESPONSE=$(curl -s "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getMe" --max-time 5 2>/dev/null)
    TG_OK=$(echo "$TG_RESPONSE" | grep -oE '"ok":(true|false)' | cut -d: -f2)
    if [ "$TG_OK" = "true" ]; then
        BOT_USER=$(echo "$TG_RESPONSE" | grep -oE '"username":"[^"]+"' | cut -d'"' -f4)
        pass "Telegram bot OK (@$BOT_USER)"
    else
        fail "Telegram getMe failed — token revoked or invalid"
    fi
else
    fail "TELEGRAM_BOT_TOKEN not set, skipping"
fi

# G. Instance resources
section "G. Instance resources"
DISK_PCT=$(df / | awk 'NR==2 {print $5}' | tr -d '%')
RAM_FREE_MB=$(free -m | awk 'NR==2 {print $7}')
LOAD=$(awk '{print $1}' /proc/loadavg)
ULIMIT_N=$(ulimit -n)

[ "$DISK_PCT" -lt 70 ] && pass "Disk: ${DISK_PCT}% used" || warn "Disk: ${DISK_PCT}% used (>70%)"
[ "$RAM_FREE_MB" -gt 300 ] && pass "RAM free: ${RAM_FREE_MB}MB" || warn "RAM free: ${RAM_FREE_MB}MB (<300MB)"
LOAD_INT=$(echo "$LOAD" | awk '{print int($1*100)}')
[ "$LOAD_INT" -lt 50 ] && pass "Load avg 1m: $LOAD" || warn "Load avg 1m: $LOAD (>0.5 — something is using CPU)"
[ "$ULIMIT_N" -ge 1024 ] && pass "ulimit -n: $ULIMIT_N" || fail "ulimit -n: $ULIMIT_N (<1024) — raise in /etc/security/limits.conf"

# I. tmux
section "I. tmux"
if command -v tmux >/dev/null 2>&1; then
    pass "tmux installed: $(tmux -V)"
else
    fail "tmux not installed — sudo apt install tmux"
fi

# Summary
echo ""
echo "${BOLD}=========================================${RESET}"
if [ "$FAILED" -eq 0 ] && [ "$WARNED" -eq 0 ]; then
    echo "${GREEN}${BOLD}ALL GREEN — ready for benchmark${RESET}"
elif [ "$FAILED" -eq 0 ]; then
    echo "${YELLOW}${BOLD}OK with $WARNED warning(s) — review before long run${RESET}"
else
    echo "${RED}${BOLD}$FAILED FAIL(s) — resolve before proceeding${RESET}"
    exit 1
fi
echo ""
echo "Manual checks (run from your local machine, not the instance):"
echo "  H. Verify your local public IP matches the security group inbound rule (curl https://checkip.amazonaws.com)"
echo "  J. Backup IAM credentials .csv to 1Password/gpg"
echo "  K. (optional) EBS snapshot pre-benchmark (aws ec2 create-snapshot ...)"
