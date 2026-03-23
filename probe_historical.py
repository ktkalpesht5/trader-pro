"""
probe_historical.py
-------------------
Probes Delta Exchange India API to check whether historical OHLCV candle data
is available for expired BTC move options (straddles).
"""

import httpx
import time
from datetime import datetime, timezone
import pytz

BASE_URL = "https://api.india.delta.exchange"
IST = pytz.timezone("Asia/Kolkata")


def ts(ist_dt: datetime) -> int:
    """Convert IST datetime to UTC unix timestamp."""
    return int(ist_dt.astimezone(timezone.utc).timestamp())


def probe_candles(symbol: str, start: int, end: int, resolution: int = 300) -> dict:
    url = f"{BASE_URL}/v2/history/candles"
    params = {"symbol": symbol, "resolution": resolution, "start": start, "end": end}
    try:
        r = httpx.get(url, params=params, timeout=10)
        data = r.json()
        candles = data.get("result", [])
        return {
            "symbol": symbol,
            "status": r.status_code,
            "candle_count": len(candles) if isinstance(candles, list) else 0,
            "first_candle": candles[0] if candles else None,
            "last_candle": candles[-1] if candles else None,
            "raw_result": data.get("result") if not candles else "...",
        }
    except Exception as e:
        return {"symbol": symbol, "status": "ERROR", "error": str(e)}


def probe_ticker(symbol: str) -> dict:
    url = f"{BASE_URL}/v2/tickers/{symbol}"
    try:
        r = httpx.get(url, timeout=10)
        data = r.json()
        result = data.get("result", {})
        return {
            "symbol": symbol,
            "status": r.status_code,
            "mark_price": result.get("mark_price"),
            "settlement_price": result.get("settlement_price"),
            "close": result.get("close"),
        }
    except Exception as e:
        return {"symbol": symbol, "status": "ERROR", "error": str(e)}


def probe_products_expired() -> dict:
    """Check if /v2/products returns expired contracts."""
    url = f"{BASE_URL}/v2/products"
    try:
        r = httpx.get(url, params={
            "contract_types": "move_options",
            "underlying_asset_symbol": "BTC",
            "page_size": 50,
            "states": "expired",
        }, timeout=10)
        data = r.json()
        results = data.get("result", [])
        return {
            "status": r.status_code,
            "count": len(results) if isinstance(results, list) else 0,
            "sample": [p.get("symbol") for p in (results[:5] if isinstance(results, list) else [])],
        }
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}


if __name__ == "__main__":
    # Yesterday (2026-03-23) IST window: 11 AM → 4:30 PM
    yesterday = datetime(2026, 3, 23, tzinfo=IST)
    start_11am  = ts(yesterday.replace(hour=11, minute=0))
    end_430pm   = ts(yesterday.replace(hour=16, minute=30))
    # Full day window
    start_9am   = ts(yesterday.replace(hour=9,  minute=0))
    end_530pm   = ts(yesterday.replace(hour=17, minute=30))

    print("=" * 60)
    print("PROBE: Delta Exchange India — Expired Straddle Data")
    print("=" * 60)
    print(f"Window: 2026-03-23  11:00 AM → 4:30 PM IST")
    print(f"Timestamps: start={start_11am}  end={end_430pm}\n")

    # ── 1. Candles for known expired straddles ────────────────────────────
    print("── 1. Candle data for expired MV contracts ─────────────────")
    expired_symbols = [
        "MV-BTC-67800-230326",
        "MV-BTC-68000-230326",
        "MV-BTC-68200-230326",
        "MV-BTC-70000-230326",
        "MV-BTC-70600-200326",  # from trade #2
        "MV-BTC-74000-180326",  # from trade #1
    ]
    for sym in expired_symbols:
        result = probe_candles(sym, start_11am, end_430pm)
        count = result.get("candle_count", 0)
        status = result.get("status")
        if count > 0:
            first = result["first_candle"]
            last  = result["last_candle"]
            print(f"  ✅ {sym}: {count} candles | first={first} | last={last}")
        else:
            print(f"  ❌ {sym}: status={status} count={count}  raw={result.get('raw_result', result.get('error'))}")

    # ── 2. Candles with different resolutions ─────────────────────────────
    print("\n── 2. Try different resolutions for best known symbol ───────")
    test_sym = "MV-BTC-67800-230326"
    for res in [60, 300, 900, 3600]:
        result = probe_candles(test_sym, start_9am, end_530pm, resolution=res)
        print(f"  res={res:5d}s: status={result['status']} count={result['candle_count']}")

    # ── 3. Ticker endpoint for expired symbol ─────────────────────────────
    print("\n── 3. Ticker endpoint for expired symbols ───────────────────")
    for sym in ["MV-BTC-67800-230326", "MV-BTC-70600-200326"]:
        result = probe_ticker(sym)
        print(f"  {sym}: {result}")

    # ── 4. Products endpoint — does it expose expired contracts? ──────────
    print("\n── 4. Products endpoint with states=expired ─────────────────")
    result = probe_products_expired()
    print(f"  {result}")

    # ── 5. Try today's live straddle candles as a sanity check ─────────────
    print("\n── 5. Sanity check: live straddle candles (today) ───────────")
    now_ist = datetime.now(IST)
    today = now_ist.strftime("%d%m%y")
    print(f"  Today's expiry suffix: {today}")
    # Get today's products first
    r = httpx.get(f"{BASE_URL}/v2/products", params={
        "contract_types": "move_options",
        "underlying_asset_symbol": "BTC",
        "page_size": 10,
    }, timeout=10)
    products = r.json().get("result", [])
    today_syms = [p["symbol"] for p in products if p.get("symbol", "").endswith(today)]
    print(f"  Found today's contracts: {today_syms[:3]}")
    if today_syms:
        start_today = ts(now_ist.replace(hour=9, minute=0))
        end_today   = int(datetime.now(timezone.utc).timestamp())
        result = probe_candles(today_syms[0], start_today, end_today, resolution=300)
        count = result.get("candle_count", 0)
        print(f"  Live candles for {today_syms[0]}: status={result['status']} count={count}")
        if count > 0:
            print(f"    first={result['first_candle']}")
            print(f"    last= {result['last_candle']}")
