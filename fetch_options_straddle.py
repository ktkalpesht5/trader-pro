"""
fetch_options_straddle.py
--------------------------
Builds a synthetic Friday short straddle dataset using real call + put legs.

For each Saturday expiry:
  - Entry: Friday 10PM IST
  - Find ATM strike = closest listed strike to BTC spot at entry
  - Fetch 1m candles for C-BTC-{strike}-{date} + P-BTC-{strike}-{date}
  - Combined premium = call_close + put_close at each minute

Outputs (all in data/):
  options_meta_sat.json          — all Saturday-expiry BTC call/put products
  btc_1m_options_window.json     — BTC 1m candles for the full window
  options_1m_pairs.json          — {sat_date: {strike, call_candles, put_candles}}

Usage: python fetch_options_straddle.py
"""

import asyncio, json, os, sys
from datetime import datetime, timedelta, timezone
import httpx, pytz

IST      = pytz.timezone("Asia/Kolkata")
BASE_URL = "https://api.india.delta.exchange"
DATA_DIR = "data"
STAGGER  = 0.2

# Only fetch data NOT already covered by move_options straddles (pre Sep 28 2025)
# Change to False to fetch ALL Saturdays including overlap
PRE_STRADDLE_ONLY = False   # fetch everything, simulate will filter


# ── HTTP ──────────────────────────────────────────────────────────────────────
async def get(client, path, params=None):
    for attempt in range(4):
        try:
            r = await client.get(path, params=params or {})
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                await asyncio.sleep(2 ** (attempt + 1))
            else:
                raise
        except Exception:
            if attempt == 3: raise
            await asyncio.sleep(1)
    return {}


async def get_paginated(client, path, params=None):
    """Fetch all pages of a paginated endpoint."""
    results = []
    after = None
    while True:
        p = dict(params or {})
        p["limit"] = 100
        if after:
            p["after"] = after
        d = await get(client, path, p)
        batch = d.get("result", [])
        if not batch:
            break
        results.extend(batch)
        after = d.get("meta", {}).get("after")
        if not after:
            break
        await asyncio.sleep(0.1)
    return results


# ── 1m candle fetch ───────────────────────────────────────────────────────────
async def fetch_1m(client, symbol, start_ts, end_ts):
    all_c = {}
    cursor_end = end_ts
    while cursor_end > start_ts:
        cursor_start = max(cursor_end - 48 * 3600, start_ts)
        d = await get(client, "/v2/history/candles", {
            "symbol": symbol, "resolution": "1m",
            "start": str(cursor_start), "end": str(cursor_end),
        })
        raw = d.get("result", [])
        if not raw:
            break
        for c in raw:
            if isinstance(c, dict):
                ts = c.get("time", 0); cl = float(c.get("close", 0) or 0)
            else:
                ts, cl = c[0], float(c[4])
            if ts and cl:
                dt_ist = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)
                all_c[ts] = {"t": ts, "dt": dt_ist.strftime("%Y-%m-%d %H:%M IST"), "c": cl}
        cursor_end = cursor_start
        await asyncio.sleep(0.05)
    return sorted(all_c.values(), key=lambda x: x["t"])


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0,
                                  headers={"Accept": "application/json"}) as client:

        # ── Step 1: Fetch all Saturday-expiry BTC call + put metadata ──────────
        meta_path = os.path.join(DATA_DIR, "options_meta_sat.json")
        if os.path.exists(meta_path):
            print(f"Loading existing options metadata…")
            with open(meta_path) as f:
                all_products = json.load(f)
        else:
            print("Fetching all Saturday-expiry BTC call/put metadata…")
            calls = await get_paginated(client, "/v2/products", {
                "contract_types": "call_options", "states": "expired",
            })
            puts  = await get_paginated(client, "/v2/products", {
                "contract_types": "put_options",  "states": "expired",
            })
            # Also active (for current/upcoming week)
            calls_a = await get_paginated(client, "/v2/products", {
                "contract_types": "call_options", "states": "live",
            })
            puts_a = await get_paginated(client, "/v2/products", {
                "contract_types": "put_options",  "states": "live",
            })
            all_raw = calls + puts + calls_a + puts_a
            all_products = []
            for p in all_raw:
                if "BTC" not in p.get("symbol", ""): continue
                st = p.get("settlement_time", "")
                if not st: continue
                dt = datetime.fromisoformat(st.replace("Z", "+00:00"))
                if dt.strftime("%A") != "Saturday": continue
                ist_dt = dt.astimezone(IST)
                settle_date = ist_dt.strftime("%Y-%m-%d")
                p["settlement_date_ist"] = settle_date
                p["settlement_time_utc"] = st
                all_products.append(p)
            with open(meta_path, "w") as f:
                json.dump(all_products, f)
            print(f"  Saved {len(all_products)} Saturday BTC call/put contracts → {meta_path}")

        # Group by settle date
        from collections import defaultdict
        by_date = defaultdict(lambda: {"calls": [], "puts": []})
        for p in all_products:
            d = p["settlement_date_ist"]
            ctype = p.get("contract_type", "")
            if "call" in ctype.lower():
                by_date[d]["calls"].append(p)
            else:
                by_date[d]["puts"].append(p)

        sat_dates = sorted(by_date.keys())
        print(f"  {len(sat_dates)} Saturday dates  ({sat_dates[0]} → {sat_dates[-1]})")

        # ── Step 2: BTC 1m candles for full window ─────────────────────────────
        btc_path = os.path.join(DATA_DIR, "btc_1m_options_window.json")
        if os.path.exists(btc_path):
            print(f"Loading existing BTC 1m candles…")
            with open(btc_path) as f:
                btc_bars = json.load(f)
            print(f"  {len(btc_bars):,} bars loaded")
        else:
            print("Fetching BTC 1m candles for full window…")
            # Window: day before first Saturday through last Saturday
            first = datetime.strptime(sat_dates[0], "%Y-%m-%d")
            last  = datetime.strptime(sat_dates[-1], "%Y-%m-%d")
            start_ts = int(IST.localize(first - timedelta(days=1)).astimezone(timezone.utc).timestamp())
            end_ts   = int(IST.localize(last + timedelta(days=1)).astimezone(timezone.utc).timestamp())
            btc_bars_dict = {}
            cursor_end = end_ts
            while cursor_end > start_ts:
                cursor_start = max(cursor_end - 24 * 3600, start_ts)
                d = await get(client, "/v2/history/candles", {
                    "symbol": "BTCUSD", "resolution": "1m",
                    "start": str(cursor_start), "end": str(cursor_end),
                })
                for c in d.get("result", []):
                    if isinstance(c, dict):
                        ts = c.get("time", 0); cl = float(c.get("close", 0) or 0)
                    else:
                        ts, cl = c[0], float(c[4])
                    if ts and cl:
                        dt_ist = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)
                        btc_bars_dict[ts] = {"t": ts, "dt": dt_ist.strftime("%Y-%m-%d %H:%M IST"), "c": cl}
                cursor_end = cursor_start
                await asyncio.sleep(0.2)
                pct = (end_ts - cursor_end) / (end_ts - start_ts) * 100
                print(f"  BTC fetch {pct:.0f}%…", end="\r", flush=True)
            btc_bars = sorted(btc_bars_dict.values(), key=lambda x: x["t"])
            with open(btc_path, "w") as f:
                json.dump(btc_bars, f)
            print(f"\n  Saved {len(btc_bars):,} BTC bars → {btc_path}")

        # Build BTC lookup
        btc_by_ts = {b["t"]: b["c"] for b in btc_bars}
        btc_ts_sorted = sorted(btc_by_ts.keys())

        def btc_at(ts):
            lo, hi = 0, len(btc_ts_sorted) - 1
            while lo < hi:
                mid = (lo + hi) // 2
                if btc_ts_sorted[mid] < ts: lo = mid + 1
                else: hi = mid
            best = btc_ts_sorted[lo]
            return btc_by_ts[best] if abs(best - ts) <= 600 else 0.0

        # ── Step 3: For each Saturday, find ATM at Friday 10PM, fetch candles ──
        pairs_path = os.path.join(DATA_DIR, "options_1m_pairs.json")
        existing_pairs = {}
        if os.path.exists(pairs_path):
            with open(pairs_path) as f:
                existing_pairs = json.load(f)
            print(f"Resuming: {len(existing_pairs)} dates already fetched")

        pairs = dict(existing_pairs)
        new_fetched = 0

        for i, sat_d in enumerate(sat_dates, 1):
            if sat_d in pairs:
                continue

            sat_dt    = datetime.strptime(sat_d, "%Y-%m-%d")
            fri_dt    = sat_dt - timedelta(days=1)
            entry_ist = IST.localize(datetime(fri_dt.year, fri_dt.month, fri_dt.day, 22, 0))
            entry_ts  = int(entry_ist.astimezone(timezone.utc).timestamp())
            expiry_ist = IST.localize(datetime(sat_dt.year, sat_dt.month, sat_dt.day, 17, 30))
            expiry_ts  = int(expiry_ist.astimezone(timezone.utc).timestamp())

            btc = btc_at(entry_ts)
            if not btc:
                print(f"  [{i}/{len(sat_dates)}] {sat_d} — no BTC price at Fri 10PM, skip")
                pairs[sat_d] = None
                continue

            # Find ATM call and put (closest strike to BTC)
            calls = by_date[sat_d]["calls"]
            puts  = by_date[sat_d]["puts"]
            call_strikes = {int(p.get("strike_price", 0)): p for p in calls if p.get("strike_price")}
            put_strikes  = {int(p.get("strike_price", 0)): p for p in puts  if p.get("strike_price")}
            common = set(call_strikes.keys()) & set(put_strikes.keys())
            if not common:
                print(f"  [{i}/{len(sat_dates)}] {sat_d} — no matching call+put strikes, skip")
                pairs[sat_d] = None
                continue

            atm_strike = min(common, key=lambda s: abs(s - btc))
            call_sym = call_strikes[atm_strike]["symbol"]
            put_sym  = put_strikes[atm_strike]["symbol"]

            print(f"  [{i}/{len(sat_dates)}] {sat_d}  BTC=${btc:,.0f}  ATM=${atm_strike:,}  {call_sym} + {put_sym}", end="", flush=True)

            # Fetch window: Friday 10PM to Saturday 5:30PM (+1h buffer each side)
            fetch_start = entry_ts - 3600
            fetch_end   = expiry_ts + 3600

            call_candles = await fetch_1m(client, call_sym, fetch_start, fetch_end)
            await asyncio.sleep(STAGGER)
            put_candles  = await fetch_1m(client, put_sym,  fetch_start, fetch_end)
            await asyncio.sleep(STAGGER)

            print(f"  call={len(call_candles)}c  put={len(put_candles)}c")

            pairs[sat_d] = {
                "sat_date":   sat_d,
                "fri_date":   fri_dt.strftime("%Y-%m-%d"),
                "strike":     atm_strike,
                "btc_entry":  round(btc, 0),
                "entry_ts":   entry_ts,
                "expiry_ts":  expiry_ts,
                "call_sym":   call_sym,
                "put_sym":    put_sym,
                "call_candles": call_candles,
                "put_candles":  put_candles,
            }
            new_fetched += 1

            # Checkpoint every 10
            if new_fetched % 10 == 0:
                with open(pairs_path, "w") as f:
                    json.dump(pairs, f)
                print(f"  ── checkpoint saved ({new_fetched} new) ──")

        with open(pairs_path, "w") as f:
            json.dump(pairs, f)
        valid = sum(1 for v in pairs.values() if v)
        print(f"\nDone. {valid} valid pairs saved → {pairs_path}")


if __name__ == "__main__":
    asyncio.run(main())
