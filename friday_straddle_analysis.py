"""
friday_straddle_analysis.py
----------------------------
Simulates shorting 1 ATM BTC straddle at three entry times for every
Friday or Saturday expiry in a given dataset.

Usage:
    python friday_straddle_analysis.py <meta.json> <candles.json> <btc.json> <out_prefix> <weekday>
    weekday: 4=Friday, 5=Saturday

Example (Saturday, new period):
    python friday_straddle_analysis.py \\
        data/straddles_meta_2025-09-28_2025-12-27.json \\
        data/straddles_candles_2025-09-28_2025-12-27.json \\
        data/btc_candles_2025-09-28_2025-12-27.json \\
        data/saturday_analysis_2025q4 \\
        5
"""

import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timezone, timedelta

import httpx
import pytz

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL  = "https://api.india.delta.exchange"
IST       = pytz.timezone("Asia/Kolkata")
DATA_DIR  = os.path.join(os.path.dirname(__file__), "data")

# CLI args: meta_path candles_path btc_path out_prefix weekday
if len(sys.argv) == 6:
    META_PATH    = sys.argv[1]
    CANDLES_PATH = sys.argv[2]
    BTC_PATH     = sys.argv[3]
    OUT_PREFIX   = sys.argv[4]
    TARGET_WEEKDAY = int(sys.argv[5])   # 4=Friday, 5=Saturday
    USE_STORED_BTC = True
else:
    META_PATH    = os.path.join(DATA_DIR, "straddles_meta.json")
    CANDLES_PATH = os.path.join(DATA_DIR, "straddles_candles.json")
    BTC_PATH     = None
    OUT_PREFIX   = os.path.join(DATA_DIR, "saturday_analysis")
    TARGET_WEEKDAY = 5
    USE_STORED_BTC = False

# Entry times on the Thursday/Friday before expiry (IST hour, minute)
ENTRY_SLOTS = [
    ("8PM_Fri",     20, 0,  "Friday 8:00 PM IST (2.5h after listing)"),
    ("Midnight",     0, 0,  "Saturday 12:00 AM IST (6.5h after listing)"),
    ("4AM_Sat",      4, 0,  "Saturday 4:00 AM IST (10.5h after listing)"),
]


# ── HTTP helper ────────────────────────────────────────────────────────────────

async def get(client: httpx.AsyncClient, path: str, params: dict = None) -> dict:
    for attempt in range(3):
        try:
            r = await client.get(path, params=params or {})
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                await asyncio.sleep(2 ** attempt)
            else:
                raise
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(1)
    return {}


# ── Fetch BTC hourly spot candles for the full 3-month window ─────────────────

async def fetch_btc_candles(client: httpx.AsyncClient) -> dict[int, float]:
    """
    Returns a dict mapping unix_timestamp → BTC close price for every 1h candle.
    Fetches in batches of 500 to cover 90 days × 24h = 2,160 candles.
    """
    print("Fetching BTC/USD hourly spot candles…")
    now_ist      = datetime.now(IST)
    end_ist      = now_ist.replace(minute=0, second=0, microsecond=0)
    start_ist    = end_ist - timedelta(days=92)          # small buffer beyond 90d
    end_ts       = int(end_ist.astimezone(timezone.utc).timestamp())
    start_ts     = int(start_ist.astimezone(timezone.utc).timestamp())

    btc: dict[int, float] = {}
    batch_secs   = 500 * 3600                            # 500 hourly candles per request

    cursor_end   = end_ts
    while cursor_end > start_ts:
        cursor_start = max(cursor_end - batch_secs, start_ts)
        data = await get(client, "/v2/history/candles", {
            "symbol":     "BTCUSD",
            "resolution": "1h",
            "start":      str(cursor_start),
            "end":        str(cursor_end),
        })
        candles = data.get("result", [])
        for c in candles:
            if isinstance(c, dict):
                ts    = c.get("time", 0)
                close = float(c.get("close", 0) or 0)
            else:
                ts    = c[0]
                close = float(c[4])
            if ts and close:
                btc[ts] = close

        cursor_end = cursor_start
        await asyncio.sleep(0.2)

    print(f"  BTC candles loaded: {len(btc)} hourly bars")
    return btc


def btc_at(btc: dict[int, float], target_ist: datetime) -> float | None:
    """
    Returns BTC close price at (or just before) target_ist.
    The hourly candles from Delta India are aligned to :30 IST boundaries
    because settlement is at 17:30 IST (12:00 UTC). A candle timestamped T
    covers the period [T, T+3600). We want the candle whose T ≤ target ≤ T+3600.
    """
    target_ts = int(target_ist.astimezone(timezone.utc).timestamp())
    # Find nearest candle timestamp ≤ target
    candidates = [(ts, price) for ts, price in btc.items() if ts <= target_ts]
    if not candidates:
        return None
    best_ts, price = max(candidates, key=lambda x: x[0])
    # Accept only if within 2 hours (guard against big gaps)
    if target_ts - best_ts > 7200:
        return None
    return price


# ── Candle lookup helpers ──────────────────────────────────────────────────────

def price_at(candles: list[dict], target_ist: datetime) -> float | None:
    """
    Returns the close price of the candle whose window contains target_ist.
    """
    target_ts = int(target_ist.astimezone(timezone.utc).timestamp())
    candidates = [(c, c["timestamp_unix"]) for c in candles if c["timestamp_unix"] <= target_ts]
    if not candidates:
        return None
    best_candle, best_ts = max(candidates, key=lambda x: x[1])
    if target_ts - best_ts > 7200:
        return None
    return best_candle["close"]


def candles_from(candles: list[dict], entry_ist: datetime) -> list[dict]:
    """Returns all candles at or after the entry time."""
    entry_ts = int(entry_ist.astimezone(timezone.utc).timestamp())
    return [c for c in candles if c["timestamp_unix"] >= entry_ts - 1800]


# ── ATM selection ──────────────────────────────────────────────────────────────

def find_atm(
    friday_contracts: list[dict],
    all_candles: dict[str, list[dict]],
    btc_spot: float,
    entry_ist: datetime,
) -> dict | None:
    """
    Returns the contract whose strike is closest to btc_spot AND that has
    a candle covering the entry time. Falls back to closest with any data
    if no contract has data at the exact entry time.
    """
    # Sort by distance from spot
    ranked = sorted(friday_contracts, key=lambda c: abs(c["strike"] - btc_spot))

    # Prefer exact match (has candle at entry time)
    for contract in ranked:
        cdata = all_candles.get(contract["symbol"], [])
        p = price_at(cdata, entry_ist)
        if p and p > 0:
            return contract

    # Fallback: closest strike with any candle data at all
    for contract in ranked:
        if all_candles.get(contract["symbol"]):
            return contract

    return None


# ── P&L calculation ────────────────────────────────────────────────────────────

def calc_pnl(
    candles: list[dict],
    entry_price: float,
    entry_ist: datetime,
) -> dict:
    """
    For a short straddle entered at entry_price:
      max_profit  = largest favourable move (straddle price dropped to its lowest)
      max_loss    = largest adverse move    (straddle price rose to its highest)
      held_pnl    = P&L if held to last available candle (usually expiry)
    All values in USDT; positive = profit for the short seller.
    """
    forward = candles_from(candles, entry_ist)
    if not forward:
        return {}

    closes = [c["close"] for c in forward if c["close"] > 0]
    if not closes:
        return {}

    min_close  = min(closes)
    max_close  = max(closes)
    last_close = closes[-1]

    return {
        "entry_price":       round(entry_price, 2),
        "max_profit":        round(entry_price - min_close,  2),   # short gains when price falls
        "max_loss":          round(max_close - entry_price,  2),   # short loses when price rises
        "min_straddle_seen": round(min_close,  2),
        "max_straddle_seen": round(max_close,  2),
        "last_price":        round(last_close, 2),
        "held_to_expiry_pnl":round(entry_price - last_close, 2),
        "candles_forward":   len(forward),
    }


# ── Main analysis ──────────────────────────────────────────────────────────────

async def main():
    # Load stored straddle data
    with open(META_PATH)    as f:
        meta     = json.load(f)
    with open(CANDLES_PATH) as f:
        all_candles = json.load(f)

    # Group meta by settlement date
    by_date: dict[str, list[dict]] = {}
    for p in meta:
        by_date.setdefault(p["settlement_date_ist"], []).append(p)

    DAY_NAMES = {4: "Friday", 5: "Saturday"}
    day_label = DAY_NAMES.get(TARGET_WEEKDAY, f"weekday{TARGET_WEEKDAY}")
    fridays = sorted(
        date for date in by_date
        if datetime.strptime(date, "%Y-%m-%d").weekday() == TARGET_WEEKDAY
    )
    print(f"\n{day_label}s to analyse: {len(fridays)}")
    for f in fridays:
        print(f"  {f} — {len(by_date[f])} contracts")

    if USE_STORED_BTC and BTC_PATH and os.path.exists(BTC_PATH):
        print("Loading BTC candles from file…")
        with open(BTC_PATH) as f:
            raw = json.load(f)
        # Stored as list of dicts with timestamp_unix and close
        btc_hourly = {c["timestamp_unix"]: c["close"] for c in raw}
        print(f"  {len(btc_hourly)} bars loaded")
    else:
        async with httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=20.0,
            headers={"Accept": "application/json"},
        ) as client:
            btc_hourly = await fetch_btc_candles(client)

    # ── Run analysis ────────────────────────────────────────────────────────────
    results  = []
    csv_rows = []

    for friday_str in fridays:
        friday_dt = datetime.strptime(friday_str, "%Y-%m-%d")
        thursday  = friday_dt - timedelta(days=1)
        contracts = by_date[friday_str]

        print(f"\n{'─'*60}")
        print(f"Friday {friday_str} — {len(contracts)} contracts available")

        for slot_id, hour, minute, description in ENTRY_SLOTS:
            # Build the exact entry datetime in IST
            if slot_id == "8PM_Fri":
                base_date = thursday  # day before Saturday expiry = Friday
            else:
                base_date = friday_dt

            entry_ist = IST.localize(datetime(
                base_date.year, base_date.month, base_date.day,
                hour, minute, 0
            ))

            # BTC spot at entry time
            spot = btc_at(btc_hourly, entry_ist)
            if spot is None:
                print(f"  [{slot_id}] No BTC spot data at {entry_ist.strftime('%Y-%m-%d %H:%M IST')} — skip")
                continue

            # ATM contract
            atm = find_atm(contracts, all_candles, spot, entry_ist)
            if atm is None:
                print(f"  [{slot_id}] No ATM contract found — skip")
                continue

            sym    = atm["symbol"]
            cdata  = all_candles.get(sym, [])
            entry_price = price_at(cdata, entry_ist)

            if not entry_price or entry_price <= 0:
                # Try next-closest strike
                for alt in sorted(contracts, key=lambda c: abs(c["strike"] - spot)):
                    if alt["symbol"] == sym:
                        continue
                    ep = price_at(all_candles.get(alt["symbol"], []), entry_ist)
                    if ep and ep > 0:
                        atm         = alt
                        sym         = alt["symbol"]
                        cdata       = all_candles.get(sym, [])
                        entry_price = ep
                        break

            if not entry_price or entry_price <= 0:
                print(f"  [{slot_id}] BTC={spot:,.0f} → ATM={atm['strike']} but no price at entry — skip")
                continue

            pnl = calc_pnl(cdata, entry_price, entry_ist)
            if not pnl:
                print(f"  [{slot_id}] Insufficient forward candles — skip")
                continue

            row = {
                "friday":            friday_str,
                "entry_slot":        slot_id,
                "entry_time_ist":    entry_ist.strftime("%Y-%m-%d %H:%M IST"),
                "btc_spot_at_entry": round(spot, 0),
                "atm_symbol":        sym,
                "atm_strike":        atm["strike"],
                "strike_vs_spot":    round(atm["strike"] - spot, 0),
                **pnl,
                "description":       description,
            }
            results.append(row)
            csv_rows.append(row)

            direction = "PROFIT" if pnl["held_to_expiry_pnl"] > 0 else "LOSS"
            print(
                f"  [{slot_id}] BTC=${spot:,.0f} → {sym} | "
                f"entry=${pnl['entry_price']} | "
                f"max_profit=${pnl['max_profit']} | "
                f"max_loss=${pnl['max_loss']} | "
                f"expiry={direction} ${abs(pnl['held_to_expiry_pnl'])}"
            )

    # ── Save outputs ────────────────────────────────────────────────────────────
    json_path = OUT_PREFIX + ".json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)

    csv_path = OUT_PREFIX + ".csv"
    if csv_rows:
        fields = [
            "friday", "entry_slot", "entry_time_ist",
            "btc_spot_at_entry", "atm_symbol", "atm_strike", "strike_vs_spot",
            "entry_price", "max_profit", "max_loss",
            "min_straddle_seen", "max_straddle_seen",
            "held_to_expiry_pnl", "last_price", "candles_forward",
        ]
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(csv_rows)

    # ── Summary table ────────────────────────────────────────────────────────────
    print(f"\n{'═'*80}")
    print(f"{'FRIDAY STRADDLE SHORT ANALYSIS — SUMMARY':^80}")
    print(f"{'═'*80}")
    print(f"{'Friday':<12} {'Slot':<12} {'BTC Spot':>9} {'Strike':>8} {'Entry':>7} "
          f"{'MaxProfit':>10} {'MaxLoss':>9} {'ExpiryPnL':>10}")
    print(f"{'─'*80}")
    for r in results:
        pnl_str = f"+${r['held_to_expiry_pnl']}" if r['held_to_expiry_pnl'] >= 0 else f"-${abs(r['held_to_expiry_pnl'])}"
        print(
            f"{r['friday']:<12} {r['entry_slot']:<12} "
            f"${r['btc_spot_at_entry']:>8,.0f} "
            f"${r['atm_strike']:>7,} "
            f"${r['entry_price']:>6.0f} "
            f"  +${r['max_profit']:>7.0f} "
            f"  -${r['max_loss']:>6.0f} "
            f"  {pnl_str:>9}"
        )

    # Aggregate stats per slot
    print(f"\n{'─'*80}")
    print("AGGREGATE BY ENTRY SLOT:")
    for slot_id, _, _, desc in ENTRY_SLOTS:
        slot_rows = [r for r in results if r["entry_slot"] == slot_id]
        if not slot_rows:
            continue
        wins      = sum(1 for r in slot_rows if r["held_to_expiry_pnl"] > 0)
        avg_mp    = sum(r["max_profit"]          for r in slot_rows) / len(slot_rows)
        avg_ml    = sum(r["max_loss"]            for r in slot_rows) / len(slot_rows)
        avg_expiry= sum(r["held_to_expiry_pnl"]  for r in slot_rows) / len(slot_rows)
        total     = len(slot_rows)
        print(f"\n  {desc}")
        print(f"    Fridays analysed : {total}")
        print(f"    Win rate         : {wins}/{total} ({100*wins/total:.0f}%)")
        print(f"    Avg max profit   : +${avg_mp:.2f}")
        print(f"    Avg max loss     : -${avg_ml:.2f}")
        print(f"    Avg expiry P&L   : ${avg_expiry:+.2f}")

    print(f"\nSaved → {json_path}")
    print(f"Saved → {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
