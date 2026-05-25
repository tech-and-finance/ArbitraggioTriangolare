"""Smoke test isolato: verifica che la pipeline opp -> save_files funzioni
con dati realistici Decimal-based. Da eseguire su EC2 puntando al file
staging-event-driven prima di switch atomico.

Uso:  python smoke_test_pipeline.py
Exit 0 = OK (PROFITS_FILE scritto correttamente, schema chiavi consistente).
Exit 1 = FAIL (pipeline rotta - non lanciare run 24h).
"""
import importlib.util
import os
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

ARB_PATH = Path(__file__).parent / "arbitraggio.py"

def load_module(path):
    spec = importlib.util.spec_from_file_location("arb_under_test", str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["arb_under_test"] = mod
    os.environ.setdefault("BINANCE_API_KEY", "smoke")
    os.environ.setdefault("BINANCE_SECRET_KEY", "smoke")
    spec.loader.exec_module(mod)
    return mod

def build_mock_opp():
    profit_ratio = Decimal("0.001772")
    budget = Decimal("22")
    return {
        "path": "USDT->BTC->ETH->USDT",
        "profit": str(profit_ratio),
        "profit_perc": "0.1772",
        "final": str(budget * (Decimal("1") + profit_ratio)),
        "pairs": ["BTCUSDT", "ETHBTC", "ETHUSDT"],
        "details": {
            "pairs": ("BTCUSDT", "ETHBTC", "ETHUSDT"),
            "rates": ("0.0000099", "20.0", "5050.5"),
            "prices": ("100500.00", "0.0500", "5050.5"),
        },
    }

def build_mock_prices():
    return {
        "BTCUSDT": {"ask": Decimal("100500.00"), "bid": Decimal("100499.00"),
                    "ask_qty": Decimal("0.5"), "bid_qty": Decimal("0.5"), "ts": 0},
        "ETHBTC": {"ask": Decimal("0.0501"), "bid": Decimal("0.0500"),
                   "ask_qty": Decimal("10.0"), "bid_qty": Decimal("10.0"), "ts": 0},
        "ETHUSDT": {"ask": Decimal("5051.0"), "bid": Decimal("5050.5"),
                    "ask_qty": Decimal("5.0"), "bid_qty": Decimal("5.0"), "ts": 0},
    }

def build_mock_symbol_info():
    base = {
        "minQty": Decimal("0.0001"),
        "minNotional": Decimal("5"),
        "stepSize": Decimal("0.0001"),
        "base": "X", "quote": "Y",
    }
    out = {}
    for sym, b, q in [("BTCUSDT", "BTC", "USDT"), ("ETHBTC", "ETH", "BTC"), ("ETHUSDT", "ETH", "USDT")]:
        info = dict(base)
        info["base"] = b
        info["quote"] = q
        out[sym] = info
    return out

def main():
    if not ARB_PATH.exists():
        print(f"[FAIL] arbitraggio.py non trovato in {ARB_PATH}")
        return 1

    print(f"[INFO] Caricando {ARB_PATH}")
    arb = load_module(ARB_PATH)

    failures = []

    # 1. BUFFER_SICUREZZA must be Decimal
    if not isinstance(arb.BUFFER_SICUREZZA, Decimal):
        failures.append(f"BUFFER_SICUREZZA is {type(arb.BUFFER_SICUREZZA).__name__}, expected Decimal")
    else:
        print(f"[OK] BUFFER_SICUREZZA = {arb.BUFFER_SICUREZZA} (Decimal)")

    opp = build_mock_opp()
    prices = build_mock_prices()
    sinfo = build_mock_symbol_info()

    # 2. calcola_importo_ottimale_con_buffer must not raise on Decimal inputs
    try:
        importo, volumi = arb.calcola_importo_ottimale_con_buffer(opp["pairs"], prices, sinfo)
        print(f"[OK] calcola_importo_ottimale_con_buffer -> importo={importo}, volumi={len(volumi)}")
        if importo == 0:
            print("[WARN] importo_ottimale=0 (filtri minQty/minNotional). Controllare mock se inatteso.")
    except Exception as e:
        failures.append(f"calcola_importo_ottimale_con_buffer raised {type(e).__name__}: {e}")

    # 3. profit_perc_val cast deve essere Decimal-safe per moltiplicazione con SIMULATION_BUDGET_USDT
    try:
        from decimal import Decimal as D
        profit_perc_val = D(opp["profit_perc"])
        guadagno = arb.config.SIMULATION_BUDGET_USDT * profit_perc_val / D("100")
        print(f"[OK] guadagno_stimato calculation -> {guadagno}")
    except Exception as e:
        failures.append(f"profit_perc_val Decimal calculation raised {type(e).__name__}: {e}")

    # 4. save_profitable_opportunity must not crash on the opp schema (KeyError check)
    # Salviamo in tmp dir per non polluire cwd
    tmpdir = tempfile.mkdtemp(prefix="arb_smoke_")
    orig_cwd = os.getcwd()
    os.chdir(tmpdir)
    try:
        # Adatta opp se la funzione si aspetta key 'profit' (legacy)
        # Verifichiamo la coerenza schema vs cosa save_profitable_opportunity usa
        try:
            arb.save_profitable_opportunity(opp)
            written = Path(tmpdir) / "profitable_opportunities.txt"
            if written.exists() and written.stat().st_size > 0:
                content = written.read_text()
                print(f"[OK] save_profitable_opportunity -> file scritto ({written.stat().st_size}B)")
                print(f"     contenuto: {content.strip()[:200]}")
                if "USDT->BTC->ETH" not in content:
                    failures.append("save_profitable_opportunity: path NON presente nel file scritto")
            else:
                failures.append("save_profitable_opportunity: file NON creato o vuoto")
        except KeyError as ke:
            failures.append(f"save_profitable_opportunity: KeyError {ke} - schema inconsistente. opp ha keys={list(opp.keys())}")
        except Exception as e:
            failures.append(f"save_profitable_opportunity raised {type(e).__name__}: {e}")
    finally:
        os.chdir(orig_cwd)

    # 5. format_opportunity_message coerenza schema
    try:
        msg = arb.format_opportunity_message(opp, prices)
        if "INCOMPLETE DATA" in msg or "Anomalous data" in msg:
            failures.append(f"format_opportunity_message returned anomaly: {msg[:120]}")
        else:
            print(f"[OK] format_opportunity_message -> {msg[:80].splitlines()[0]}")
    except KeyError as ke:
        failures.append(f"format_opportunity_message KeyError {ke}")
    except Exception as e:
        failures.append(f"format_opportunity_message {type(e).__name__}: {e}")

    print("---")
    if failures:
        print(f"[FAIL] {len(failures)} probleM(i):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("[OK] Smoke test passed.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
