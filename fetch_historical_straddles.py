"""
fetch_historical_straddles.py
------------------------------
Fetches all expired BTC MV straddle (move_options) contracts from Delta Exchange
India API for a given date window and saves:

  data/straddles_meta_{tag}.json     — product metadata for every contract
  data/straddles_candles_{tag}.json  — OHLCV candles keyed by symbol
  data/straddles_combined_{tag}.csv  — flat CSV for quick analysis
  data/btc_candles_{tag}.json        — BTC/USD hourly spot candles (useful for
                                       both short-straddle and option-buying
                                       strategy analysis)

Usage:
    python fetch_historical_straddles.py                    # last 3 months
    python fetch_historical_straddles.py 2025-09-28 2025-12-27   # custom range
"""

import asyncio
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import httpx
import pytz

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL     = "https://api.india.delta.exchange"
IST          = pytz.timezone("Asia/Kolkata")
TODAY_IST    = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)

# Allow CLI date-range override: python script.py YYYY-MM-DD YYYY-MM-DD
if len(sys.argv) == 3:
    WINDOW_START_IST = IST.localize(datetime.strptime(sys.argv[1], "%Y-%m-%d"))
    WINDOW_END_IST   = IST.localize(datetime.strptime(sys.argv[2], "%Y-%m-%d"))
    TAG = f"{sys.argv[1]}_{sys.argv[2]}"
else:
    WINDOW_END_IST   = TODAY_IST
    WINDOW_START_IST = TODAY_IST - timedelta(days=90)
    TAG = WINDOW_START_IST.strftime("%Y-%m-%d") + "_" + WINDOW_END_IST.strftime("%Y-%m-%d")

THREE_MONTHS_AGO_IST = WINDOW_START_IST   # kept as alias for existing logic
DATA_DIR     = os.path.join(os.path.dirname(__file__), "data")

# ── HTTP helpers ──────────────────────────────────────────────────────────────

async def get(client: httpx.AsyncClient, path: str, params: dict = None) -> dict:
    for attempt in range(3):
        try:
            r = await client.get(path, params=params or {})
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                wait = 2 ** attempt
                print(f"  Rate limited — waiting {wait}s…")
                await asyncio.sleep(wait)
            else:
                raise
        except Exception as e:
            if attempt == 2:
                raise
            await asyncio.sleep(1)
    return {}


# ── Step 1: Collect all expired BTC MV straddle products ────────────────────

async def fetch_all_expired_straddles(client: httpx.AsyncClient) -> list[dict]:
    """
    Pages through /v2/products with states=expired,contract_types=move_options
    and returns all BTC straddles that settled within the past 3 months.
    """
    products = []
    after_cursor = None
    page = 0

    print("Fetching expired BTC straddle product list…")

    while True:
        page += 1
        params = {
            "contract_types":           "move_options",
            "underlying_asset_symbol":  "BTC",
            "states":                   "expired",
            "page_size":                100,
        }
        if after_cursor:
            params["after"] = after_cursor

        data  = await get(client, "/v2/products", params)
        batch = data.get("result", [])
        meta  = data.get("meta", {})

        if not batch:
            break

        for p in batch:
            symbol          = p.get("symbol", "")
            settlement_str  = p.get("settlement_time", "")

            if not symbol.startswith("MV-BTC-"):
                continue

            # Parse settlement time and convert to IST
            if not settlement_str:
                continue
            try:
                st_utc = datetime.fromisoformat(settlement_str.replace("Z", "+00:00"))
                st_ist = st_utc.astimezone(IST)
            except Exception:
                continue

            # Skip contracts that settled after our window end
            if st_ist > WINDOW_END_IST:
                continue

            # Products are newest-first; stop once we pass the window start
            if st_ist < WINDOW_START_IST:
                print(f"  Reached window start ({WINDOW_START_IST.date()}) at page {page}. Stopping.")
                return products

            # Parse strike from symbol: MV-BTC-70600-200326 → 70600
            parts = symbol.split("-")
            strike = int(parts[2]) if len(parts) >= 4 else 0

            products.append({
                "symbol":          symbol,
                "strike":          strike,
                "settlement_time_utc": settlement_str,
                "settlement_time_ist": st_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
                "settlement_date_ist": st_ist.strftime("%Y-%m-%d"),
                "product_id":      p.get("id"),
                "state":           p.get("state", "expired"),
                "description":     p.get("description", ""),
            })

        print(f"  Page {page}: {len(batch)} products fetched, {len(products)} BTC straddles so far")

        # Pagination
        after_cursor = meta.get("after")
        if not after_cursor:
            break

        await asyncio.sleep(0.3)  # gentle rate limiting

    return products


# ── Step 2: Fetch OHLCV candles for each contract ────────────────────────────

async def fetch_candles_for_symbol(
    client: httpx.AsyncClient,
    symbol: str,
    settlement_utc_str: str,
) -> list[dict]:
    """
    Fetches 1-hour OHLCV candles for the straddle's full lifetime.
    Daily BTC straddles list ~24h before expiry, so we fetch 26h back from settlement.
    """
    try:
        st_utc = datetime.fromisoformat(settlement_utc_str.replace("Z", "+00:00"))
    except Exception:
        return []

    # Straddle lifetime: ~24h. Fetch 26h before settlement to catch listing candle.
    end_ts   = int(st_utc.timestamp())
    start_ts = end_ts - (26 * 3600)

    data = await get(client, "/v2/history/candles", params={
        "symbol":     symbol,
        "resolution": "1h",           # hourly — balances detail vs. volume
        "start":      str(start_ts),
        "end":        str(end_ts),
    })

    raw = data.get("result", [])
    if not raw:
        return []

    candles = []
    for c in raw:
        if isinstance(c, dict):
            ts    = c.get("time", 0)
            open_ = float(c.get("open",   0) or 0)
            high  = float(c.get("high",   0) or 0)
            low   = float(c.get("low",    0) or 0)
            close = float(c.get("close",  0) or 0)
            vol   = float(c.get("volume", 0) or 0)
        else:
            ts, open_, high, low, close, vol = (
                c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])
            )

        # Convert candle timestamp to IST for readability
        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        dt_ist = dt_utc.astimezone(IST)

        candles.append({
            "timestamp_unix": ts,
            "datetime_ist":   dt_ist.strftime("%Y-%m-%d %H:%M IST"),
            "open":           open_,
            "high":           high,
            "low":            low,
            "close":          close,
            "volume":         vol,
        })

    candles.sort(key=lambda x: x["timestamp_unix"])
    return candles


# ── Step 3: Build summary row per product ────────────────────────────────────

def summarise(product: dict, candles: list[dict]) -> dict:
    """Extracts key stats from candles for the flat CSV row."""
    if not candles:
        return {
            **product,
            "open_price":      None,
            "close_price":     None,
            "high_price":      None,
            "low_price":       None,
            "total_volume":    None,
            "price_at_entry_window": None,  # candle closest to 12:00 PM IST
            "candle_count":    0,
        }

    opens  = [c["open"]   for c in candles if c["open"]  > 0]
    highs  = [c["high"]   for c in candles if c["high"]  > 0]
    lows   = [c["low"]    for c in candles if c["low"]   > 0]
    closes = [c["close"]  for c in candles if c["close"] > 0]
    vols   = [c["volume"] for c in candles]

    # Price at 12:00 PM IST (entry window open) — find nearest candle
    entry_window_price = None
    settlement_date = product["settlement_date_ist"]  # e.g. "2026-03-20"
    target_ts = int(
        IST.localize(datetime.strptime(f"{settlement_date} 12:00:00", "%Y-%m-%d %H:%M:%S"))
        .astimezone(timezone.utc)
        .timestamp()
    )
    nearest = min(candles, key=lambda c: abs(c["timestamp_unix"] - target_ts), default=None)
    if nearest:
        entry_window_price = nearest["close"]

    return {
        **product,
        "open_price":            opens[0]  if opens  else None,
        "close_price":           closes[-1] if closes else None,
        "high_price":            max(highs) if highs  else None,
        "low_price":             min(lows)  if lows   else None,
        "total_volume":          sum(vols),
        "price_at_entry_window": entry_window_price,
        "candle_count":          len(candles),
    }


# ── BTC Spot Candles ──────────────────────────────────────────────────────────

async def fetch_btc_spot_candles(
    client: httpx.AsyncClient,
    start_ist: datetime,
    end_ist: datetime,
) -> list[dict]:
    """
    Fetches BTC/USD hourly OHLCV candles for the given IST window.
    Saves both raw numeric fields and human-readable IST timestamps.
    Useful for: ATM determination, IV-RV calculation, option buying analysis.
    Fetches in 500-candle batches (500h ≈ 20 days).
    """
    start_ts = int(start_ist.astimezone(timezone.utc).timestamp())
    end_ts   = int(end_ist.astimezone(timezone.utc).timestamp())
    batch    = 500 * 3600

    all_candles: dict[int, dict] = {}
    cursor_end = end_ts

    while cursor_end > start_ts:
        cursor_start = max(cursor_end - batch, start_ts)
        data = await get(client, "/v2/history/candles", {
            "symbol": "BTCUSD", "resolution": "1h",
            "start": str(cursor_start), "end": str(cursor_end),
        })
        for c in data.get("result", []):
            if isinstance(c, dict):
                ts = c.get("time", 0)
                o, h, l, cl, v = (float(c.get(k, 0) or 0) for k in ("open","high","low","close","volume"))
            else:
                ts, o, h, l, cl, v = c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])
            if ts and cl:
                dt_ist = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)
                all_candles[ts] = {
                    "timestamp_unix": ts,
                    "datetime_ist":   dt_ist.strftime("%Y-%m-%d %H:%M IST"),
                    "open": o, "high": h, "low": l, "close": cl, "volume": v,
                }
        cursor_end = cursor_start
        await asyncio.sleep(0.2)

    return sorted(all_candles.values(), key=lambda x: x["timestamp_unix"])


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    async with httpx.AsyncClient(
        base_url=BASE_URL,
        timeout=20.0,
        headers={"Accept": "application/json"},
    ) as client:

        # ── 1. Fetch product list ────────────────────────────────────────────
        products = await fetch_all_expired_straddles(client)
        print(f"\nTotal BTC straddles in range: {len(products)}")

        if not products:
            print("No products found. Exiting.")
            return

        # ── 1b. Fetch and save BTC hourly spot candles for the window ───────────
        print(f"\nFetching BTC/USD hourly spot candles for window…")
        btc_candles = await fetch_btc_spot_candles(client, WINDOW_START_IST, WINDOW_END_IST)
        btc_path = os.path.join(DATA_DIR, f"btc_candles_{TAG}.json")
        with open(btc_path, "w") as f:
            json.dump(btc_candles, f, indent=2)
        print(f"Saved BTC candles → {btc_path}  ({len(btc_candles)} bars)")

        # Save metadata immediately
        meta_path = os.path.join(DATA_DIR, f"straddles_meta_{TAG}.json")
        with open(meta_path, "w") as f:
            json.dump(products, f, indent=2)
        print(f"Saved metadata    → {meta_path}")

        # ── 2. Fetch candles ─────────────────────────────────────────────────
        print(f"\nFetching OHLCV candles for {len(products)} contracts…")
        candles_by_symbol: dict[str, list[dict]] = {}
        summary_rows: list[dict] = []

        for i, product in enumerate(products, 1):
            symbol = product["symbol"]
            print(f"  [{i}/{len(products)}] {symbol}", end="", flush=True)

            candles = await fetch_candles_for_symbol(
                client, symbol, product["settlement_time_utc"]
            )
            candles_by_symbol[symbol] = candles
            summary_rows.append(summarise(product, candles))

            print(f" → {len(candles)} candles")

            # Polite rate limiting: 3 requests/sec
            if i % 10 == 0:
                await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(0.1)

        # ── 3. Save candles JSON ─────────────────────────────────────────────
        candles_path = os.path.join(DATA_DIR, f"straddles_candles_{TAG}.json")
        with open(candles_path, "w") as f:
            json.dump(candles_by_symbol, f, indent=2)
        print(f"\nSaved candles     → {candles_path}")

        # ── 4. Save flat CSV ─────────────────────────────────────────────────
        csv_path = os.path.join(DATA_DIR, f"straddles_combined_{TAG}.csv")
        if summary_rows:
            fieldnames = [
                "symbol", "strike", "settlement_date_ist", "settlement_time_ist",
                "open_price", "close_price", "high_price", "low_price",
                "price_at_entry_window", "total_volume", "candle_count",
                "product_id", "description",
            ]
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(sorted(summary_rows, key=lambda x: x["settlement_date_ist"]))
        print(f"Saved CSV         → {csv_path}")

        # ── 5. Print summary ─────────────────────────────────────────────────
        date_range = (
            min(p["settlement_date_ist"] for p in products),
            max(p["settlement_date_ist"] for p in products),
        )
        print(f"\n{'─'*50}")
        print(f"Contracts fetched : {len(products)}")
        print(f"Date range (IST)  : {date_range[0]} → {date_range[1]}")
        print(f"Tag               : {TAG}")
        print(f"Files written     : data/btc_candles_{TAG}.json")
        print(f"                    data/straddles_meta_{TAG}.json")
        print(f"                    data/straddles_candles_{TAG}.json")
        print(f"                    data/straddles_combined_{TAG}.csv")
        print(f"{'─'*50}")


if __name__ == "__main__":
    asyncio.run(main())
