"""
fetch_1m_candles.py
--------------------
Fetches 1-minute OHLCV candles for ALL straddle contracts across both periods,
plus 1-minute BTC/USD candles for the same windows.

Re-uses existing straddles_meta files (no re-fetching of product lists).
Each straddle's full lifetime (~26h) fits in one API call (max ~2879 candles).

Outputs:
  data/straddles_1m_{tag}.json     — 1m candles keyed by symbol
  data/btc_1m_{tag}.json           — 1m BTC candles for the full window

Usage:
    python fetch_1m_candles.py
    python fetch_1m_candles.py 2025-09-28 2025-12-27   # single period
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import httpx
import pytz

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL  = "https://api.india.delta.exchange"
IST       = pytz.timezone("Asia/Kolkata")
DATA_DIR  = "data"
STAGGER   = 0.15          # seconds between straddle requests (gentle rate limit)
BATCH_GAP = 0.8           # extra pause every 50 symbols
BTC_BATCH = 24 * 3600     # 24h of BTC 1m candles per request (1440 candles)

# Periods to fetch (all have existing meta files)
PERIODS = [
    ("Q4-2025", "2025-09-28_2025-12-27"),
    ("Q1-2026", "2025-12-28_2026-03-28"),
]

# If CLI args given: fetch just that period
if len(sys.argv) == 3:
    tag = f"{sys.argv[1]}_{sys.argv[2]}"
    PERIODS = [("custom", tag)]

# ── HTTP helpers ──────────────────────────────────────────────────────────────

async def get(client: httpx.AsyncClient, path: str, params: dict) -> dict:
    for attempt in range(4):
        try:
            r = await client.get(path, params=params)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                wait = 2 ** (attempt + 1)
                print(f"  [429] Rate limited — waiting {wait}s…")
                await asyncio.sleep(wait)
            else:
                raise
        except Exception:
            if attempt == 3:
                raise
            await asyncio.sleep(1)
    return {}


# ── Fetch 1m candles for a single symbol ─────────────────────────────────────

async def fetch_1m(
    client: httpx.AsyncClient,
    symbol: str,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    """
    Fetches 1-minute candles for [start_ts, end_ts].
    API returns up to ~2879 candles per call — straddle lifetime is ~1560m so
    one call is sufficient. Paginates backward if window > 2880 minutes.
    """
    all_candles: dict[int, dict] = {}
    cursor_end = end_ts

    while cursor_end > start_ts:
        cursor_start = max(cursor_end - 48 * 3600, start_ts)  # 48h batch
        data = await get(client, "/v2/history/candles", {
            "symbol":     symbol,
            "resolution": "1m",
            "start":      str(cursor_start),
            "end":        str(cursor_end),
        })
        raw = data.get("result", [])
        if not raw:
            break

        for c in raw:
            if isinstance(c, dict):
                ts = c.get("time", 0)
                o  = float(c.get("open",   0) or 0)
                h  = float(c.get("high",   0) or 0)
                l  = float(c.get("low",    0) or 0)
                cl = float(c.get("close",  0) or 0)
                v  = float(c.get("volume", 0) or 0)
            else:
                ts, o, h, l, cl, v = c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])

            if ts and cl:
                dt_ist = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)
                all_candles[ts] = {
                    "t": ts,
                    "dt": dt_ist.strftime("%Y-%m-%d %H:%M IST"),
                    "o": o, "h": h, "l": l, "c": cl, "v": v,
                }

        cursor_end = cursor_start
        await asyncio.sleep(0.05)

    return sorted(all_candles.values(), key=lambda x: x["t"])


# ── Fetch 1m BTC candles for a date window ────────────────────────────────────

async def fetch_btc_1m(
    client: httpx.AsyncClient,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    print(f"  Fetching BTC/USD 1m candles for window…")
    all_btc: dict[int, dict] = {}
    cursor_end = end_ts

    while cursor_end > start_ts:
        cursor_start = max(cursor_end - BTC_BATCH, start_ts)
        data = await get(client, "/v2/history/candles", {
            "symbol":     "BTCUSD",
            "resolution": "1m",
            "start":      str(cursor_start),
            "end":        str(cursor_end),
        })
        for c in data.get("result", []):
            if isinstance(c, dict):
                ts = c.get("time", 0)
                cl = float(c.get("close", 0) or 0)
                if ts and cl:
                    dt_ist = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)
                    all_btc[ts] = {
                        "t": ts, "dt": dt_ist.strftime("%Y-%m-%d %H:%M IST"),
                        "o": float(c.get("open",0) or 0),
                        "h": float(c.get("high",0) or 0),
                        "l": float(c.get("low", 0) or 0),
                        "c": cl,
                        "v": float(c.get("volume",0) or 0),
                    }
        cursor_end = cursor_start
        await asyncio.sleep(0.2)

    bars = sorted(all_btc.values(), key=lambda x: x["t"])
    print(f"  BTC 1m candles: {len(bars):,}")
    return bars


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    async with httpx.AsyncClient(
        base_url=BASE_URL,
        timeout=30.0,
        headers={"Accept": "application/json"},
    ) as client:

        for label, tag in PERIODS:
            meta_path = os.path.join(DATA_DIR, f"straddles_meta_{tag}.json")
            if not os.path.exists(meta_path):
                print(f"[{label}] Meta file not found: {meta_path} — skipping")
                continue

            with open(meta_path) as f:
                products = json.load(f)

            print(f"\n{'═'*60}")
            print(f"[{label}]  {len(products)} straddle contracts")
            print(f"{'═'*60}")

            # ── BTC 1m candles ───────────────────────────────────────────────
            # Derive window from product settlement dates
            dates = sorted(p["settlement_date_ist"] for p in products)
            win_start_str = dates[0]
            win_end_str   = dates[-1]

            win_start_ist = IST.localize(datetime.strptime(win_start_str, "%Y-%m-%d"))
            win_end_ist   = IST.localize(datetime.strptime(win_end_str,   "%Y-%m-%d") + timedelta(days=1))
            btc_start_ts  = int((win_start_ist - timedelta(days=1)).astimezone(timezone.utc).timestamp())
            btc_end_ts    = int(win_end_ist.astimezone(timezone.utc).timestamp())

            btc_path = os.path.join(DATA_DIR, f"btc_1m_{tag}.json")
            if os.path.exists(btc_path):
                print(f"BTC 1m already exists → {btc_path}  (skipping)")
            else:
                btc_bars = await fetch_btc_1m(client, btc_start_ts, btc_end_ts)
                with open(btc_path, "w") as f:
                    json.dump(btc_bars, f)
                print(f"Saved BTC 1m → {btc_path}  ({len(btc_bars):,} bars)")

            # ── Straddle 1m candles ──────────────────────────────────────────
            out_path = os.path.join(DATA_DIR, f"straddles_1m_{tag}.json")

            # Resume: load existing if partial run interrupted
            existing: dict[str, list] = {}
            if os.path.exists(out_path):
                try:
                    with open(out_path) as f:
                        existing = json.load(f)
                    print(f"Resuming: {len(existing)} symbols already fetched")
                except Exception:
                    pass

            candles_by_symbol = dict(existing)
            skipped = 0
            fetched = 0
            empty   = 0

            for i, product in enumerate(products, 1):
                symbol  = product["symbol"]
                settle_str = product["settlement_time_utc"]

                # Skip already fetched
                if symbol in candles_by_symbol:
                    skipped += 1
                    continue

                # Calculate fetch window: 27h before settlement
                try:
                    st_utc   = datetime.fromisoformat(settle_str.replace("Z", "+00:00"))
                    end_ts   = int(st_utc.timestamp()) + 60          # +1m buffer
                    start_ts = end_ts - 27 * 3600
                except Exception:
                    candles_by_symbol[symbol] = []
                    continue

                label_str = f"[{i}/{len(products)}]"
                print(f"  {label_str} {symbol}", end="", flush=True)

                try:
                    candles = await fetch_1m(client, symbol, start_ts, end_ts)
                except Exception as e:
                    print(f" ERROR: {e}")
                    candles = []

                candles_by_symbol[symbol] = candles
                fetched += 1

                if candles:
                    print(f" → {len(candles)} candles")
                else:
                    empty += 1
                    print(f" → (no data)")

                # Gentle rate limiting
                await asyncio.sleep(STAGGER)
                if fetched % 50 == 0:
                    await asyncio.sleep(BATCH_GAP)
                    # Save checkpoint every 50
                    with open(out_path, "w") as f:
                        json.dump(candles_by_symbol, f)
                    print(f"  ── Checkpoint saved ({fetched} fetched, {skipped} skipped) ──")

            # Final save
            with open(out_path, "w") as f:
                json.dump(candles_by_symbol, f)

            total_candles = sum(len(v) for v in candles_by_symbol.values())
            non_empty     = sum(1 for v in candles_by_symbol.values() if v)
            print(f"\n[{label}] Done:")
            print(f"  Symbols with data : {non_empty}/{len(products)}")
            print(f"  Total 1m candles  : {total_candles:,}")
            print(f"  Output            : {out_path}")
            print(f"  Estimated size    : ~{total_candles * 100 // 1_000_000} MB")


if __name__ == "__main__":
    asyncio.run(main())
