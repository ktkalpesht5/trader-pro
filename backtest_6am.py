#!/usr/bin/env python3
"""
BTC Short Straddle 6AM Entry Backtest
======================================

Backtests short straddle positions entered at 06:00 IST with trailing SL ($150 above running min).
Hard exit at 16:30 IST unconditional.

Usage:
    python backtest_6am.py                         # last 365 days
    python backtest_6am.py 2025-04-01 2026-04-19   # custom date range
    python backtest_6am.py --date 2026-04-15       # single date (validation mode)
"""

import asyncio
import httpx
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional
import pytz
from collections import defaultdict

BASE_URL = "https://api.india.delta.exchange"
IST = pytz.timezone("Asia/Kolkata")
ENTRY_HOUR_IST = 6
EXIT_HOUR_IST = 16
EXIT_MINUTE_IST = 30
TRAIL_SL = 150  # dollars above running min
BATCH_SIZE = 10

# ============================================================================
# Helper Functions
# ============================================================================

async def _get(client: httpx.AsyncClient, path: str, params: dict = None) -> dict:
    """Fetch with exponential backoff on 429."""
    for attempt in range(4):
        try:
            r = await client.get(path, params=params or {})
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                wait = 2 ** (attempt + 1)
                print(f"  [429] Rate limited, waiting {wait}s...", file=sys.stderr)
                await asyncio.sleep(wait)
            else:
                raise
        except Exception as e:
            if attempt == 3:
                raise
            await asyncio.sleep(1)
    return {}


async def fetch_candles(
    client: httpx.AsyncClient,
    symbol: str,
    start_ts: int,
    end_ts: int,
    resolution: str = "1m",
) -> list[dict]:
    """Fetch candles with pagination (500 max per call). Returns sorted, deduplicated list."""
    resolution_secs = {"1m": 60, "5m": 300, "1h": 3600}.get(resolution, 300)
    all_candles = []
    chunk_start = start_ts

    while chunk_start < end_ts:
        chunk_end = min(chunk_start + resolution_secs * 500, end_ts)
        try:
            data = await _get(
                client,
                "/v2/history/candles",
                {
                    "symbol": symbol,
                    "resolution": resolution,
                    "start": str(chunk_start),
                    "end": str(chunk_end),
                },
            )
        except Exception as e:
            print(f"  Error fetching {symbol}: {e}", file=sys.stderr)
            return []

        result = data.get("result") or []
        for c in result:
            try:
                if isinstance(c, dict):
                    all_candles.append(
                        {
                            "time": int(c["time"]),
                            "open": float(c["open"]),
                            "high": float(c["high"]),
                            "low": float(c["low"]),
                            "close": float(c["close"]),
                        }
                    )
                elif isinstance(c, (list, tuple)) and len(c) >= 5:
                    all_candles.append(
                        {
                            "time": int(c[0]),
                            "open": float(c[1]),
                            "high": float(c[2]),
                            "low": float(c[3]),
                            "close": float(c[4]),
                        }
                    )
            except (ValueError, TypeError):
                pass

        chunk_start = chunk_end

    # Deduplicate and sort
    seen = set()
    deduped = []
    for c in sorted(all_candles, key=lambda x: x["time"]):
        if c["time"] not in seen:
            seen.add(c["time"])
            deduped.append(c)

    return deduped


def get_atm_strike(btc_spot: float) -> int:
    """Round BTC spot to nearest $200."""
    return round(btc_spot / 200) * 200


def make_symbol(strike: int, date: datetime) -> str:
    """MV-BTC-{STRIKE}-{DDMMYY}"""
    return f"MV-BTC-{strike}-{date.strftime('%d%m%y')}"


def ts_to_ist(ts: int) -> datetime:
    """Convert unix timestamp to IST datetime."""
    return datetime.fromtimestamp(ts, tz=IST)


def ist_datetime(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    """Build IST datetime."""
    return IST.localize(datetime(year, month, day, hour, minute, 0))


def date_to_trading_day_bounds(date: datetime) -> tuple[int, int]:
    """Return (start_ts, end_ts) for a trading day in IST. Trading day = 00:00 to 16:30 IST."""
    start = IST.localize(datetime(date.year, date.month, date.day, 0, 0, 0))
    end = IST.localize(datetime(date.year, date.month, date.day, 16, 30, 0))
    return int(start.timestamp()), int(end.timestamp())


def simulate_trade(candles_1m: list[dict], trade_date) -> Optional[dict]:
    """
    Simulate a trade on the given 1m candles.

    Entry: first candle at or after 06:00 IST
    Exit: trail SL ($150 above running min) or hard exit at 16:30 IST
    Return: {entry_price, exit_price, exit_reason, entry_ts, exit_ts, pnl}
    """
    if not candles_1m:
        return None

    # Target entry: 06:00 IST on trade_date
    target_entry = IST.localize(
        datetime(trade_date.year, trade_date.month, trade_date.day, ENTRY_HOUR_IST, 0, 0)
    )
    target_entry_ts = int(target_entry.timestamp())

    hard_exit_ts = int(
        IST.localize(
            datetime(
                trade_date.year,
                trade_date.month,
                trade_date.day,
                EXIT_HOUR_IST,
                EXIT_MINUTE_IST,
                0,
            )
        ).timestamp()
    )

    # Find entry candle (first at or after 06:00)
    entry_idx = None
    for i, c in enumerate(candles_1m):
        if c["time"] >= target_entry_ts:
            entry_idx = i
            break

    if entry_idx is None:
        return None

    entry_price = candles_1m[entry_idx]["close"]
    entry_ts = candles_1m[entry_idx]["time"]
    running_min = entry_price

    # Simulate forward from entry+1
    for i in range(entry_idx + 1, len(candles_1m)):
        c = candles_1m[i]

        # Check hard exit first
        if c["time"] >= hard_exit_ts:
            return {
                "entry_price": entry_price,
                "exit_price": c["close"],
                "exit_reason": "hard_exit",
                "entry_ts": entry_ts,
                "exit_ts": c["time"],
                "pnl": entry_price - c["close"],
            }

        # Update running min
        running_min = min(running_min, c["low"])

        # Check trail SL: exit if price >= running_min + $150
        if c["close"] >= running_min + TRAIL_SL:
            return {
                "entry_price": entry_price,
                "exit_price": c["close"],
                "exit_reason": "trail_sl",
                "entry_ts": entry_ts,
                "exit_ts": c["time"],
                "pnl": entry_price - c["close"],
            }

    # No exit triggered (shouldn't happen if hard exit is in range)
    return None


async def process_day(
    client: httpx.AsyncClient, trade_date: datetime
) -> Optional[dict]:
    """
    Process one trading day:
    1. Fetch BTC hourly candles
    2. Get spot at 06:00 IST
    3. Compute ATM strike
    4. Fetch straddle 1m candles (try ATM, ATM±200, ATM±400)
    5. Simulate trade
    """
    # Weekends have no straddles
    if trade_date.weekday() >= 5:
        return None

    # Fetch BTC hourly candles (4 hours before and after 06:00 to be safe)
    target_entry = IST.localize(
        datetime(trade_date.year, trade_date.month, trade_date.day, ENTRY_HOUR_IST, 0, 0)
    )
    btc_start = int((target_entry - timedelta(hours=4)).timestamp())
    btc_end = int((target_entry + timedelta(hours=2)).timestamp())

    try:
        btc_candles = await fetch_candles(client, "BTCUSD", btc_start, btc_end, "1h")
    except Exception as e:
        print(f"  Failed to fetch BTC for {trade_date.date()}: {e}", file=sys.stderr)
        return None

    # Find BTC spot at 06:00 IST (closest hourly close at or before)
    btc_spot = None
    for c in reversed(btc_candles):
        if c["time"] <= int(target_entry.timestamp()):
            btc_spot = c["close"]
            break

    if btc_spot is None:
        return {"status": "no_btc_data", "date": trade_date.date()}

    # ATM strike
    atm = get_atm_strike(btc_spot)

    # Try fallback strikes
    strikes_to_try = [atm, atm + 200, atm - 200, atm + 400, atm - 400]
    straddle_candles = None

    start_ts, end_ts = date_to_trading_day_bounds(trade_date)

    for strike in strikes_to_try:
        symbol = make_symbol(strike, trade_date)
        try:
            candles = await fetch_candles(client, symbol, start_ts, end_ts, "1m")
            if candles and len(candles) > 0:
                straddle_candles = candles
                break
        except Exception:
            await asyncio.sleep(0.2)

    if straddle_candles is None:
        return {"status": "no_straddle_data", "date": trade_date.date(), "spot": btc_spot}

    # Simulate
    result = simulate_trade(straddle_candles, trade_date)
    if result:
        result["date"] = trade_date.date()
        result["strike"] = strikes_to_try[0]  # ATM used (or first successful)
        return result

    return {"status": "no_exit_signal", "date": trade_date.date()}


async def run_backtest(start_date: datetime, end_date: datetime) -> list[dict]:
    """Run backtest over date range."""
    print(f"Backtesting {start_date.date()} to {end_date.date()}...")
    results = []
    errors = []

    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        for i in range(0, len(dates), BATCH_SIZE):
            batch = dates[i : i + BATCH_SIZE]
            print(f"  Processing batch {i//BATCH_SIZE + 1} ({len(batch)} dates)...")

            tasks = [process_day(client, d) for d in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in batch_results:
                if isinstance(r, Exception):
                    errors.append(str(r))
                elif r:
                    results.append(r)

            await asyncio.sleep(0.5)  # throttle between batches

    return results


def summarize(results: list[dict]) -> dict:
    """Compute summary statistics."""
    trades = [r for r in results if "entry_price" in r]
    skipped = [r for r in results if "status" in r]

    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] < 0]
    breakeven = [t for t in trades if t["pnl"] == 0]

    total_pnl = sum(t["pnl"] for t in trades)
    avg_pnl = total_pnl / len(trades) if trades else 0
    win_rate = 100 * len(wins) / len(trades) if trades else 0

    # Max drawdown
    cumulative_equity = 0
    peak = 0
    max_dd = 0
    for t in trades:
        cumulative_equity += t["pnl"]
        peak = max(peak, cumulative_equity)
        max_dd = min(max_dd, cumulative_equity - peak)

    # Exit breakdown
    exit_counts = defaultdict(int)
    for t in trades:
        exit_counts[t.get("exit_reason", "unknown")] += 1

    skip_counts = defaultdict(int)
    for s in skipped:
        skip_counts[s.get("status", "unknown")] += 1

    return {
        "total_trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(breakeven),
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "max_drawdown": max_dd,
        "exit_breakdown": dict(exit_counts),
        "skip_breakdown": dict(skip_counts),
        "trades": trades,
    }


def print_results(summary: dict):
    """Print results table and summary."""
    trades = summary["trades"]

    print("\n" + "=" * 90)
    print(f"{'Date':<12} {'Strike':<10} {'Entry':<12} {'Exit':<12} {'Exit Reason':<15} {'PnL':<12}")
    print("=" * 90)

    for t in trades:
        date_str = str(t["date"])
        strike_str = f"${t.get('strike', 0):,.0f}"
        entry_str = f"${t['entry_price']:,.2f}"
        exit_str = f"${t['exit_price']:,.2f}"
        reason_str = t.get("exit_reason", "unknown")
        pnl_str = f"${t['pnl']:+,.2f}"

        print(
            f"{date_str:<12} {strike_str:<10} {entry_str:<12} {exit_str:<12} {reason_str:<15} {pnl_str:<12}"
        )

    print("=" * 90)
    print()

    s = summary
    print(f"Total Trades: {s['total_trades']}")
    print(f"Wins: {s['wins']} | Losses: {s['losses']} | Breakeven: {s['breakeven']}")
    print(f"Win Rate: {s['win_rate']:.1f}%")
    print(f"Total PnL: ${s['total_pnl']:+,.2f}")
    print(f"Avg PnL per trade: ${s['avg_pnl']:+,.2f}")
    print(f"Max Drawdown: ${s['max_drawdown']:+,.2f}")
    print()
    print("Exit Breakdown:")
    for reason, count in sorted(s["exit_breakdown"].items()):
        print(f"  {reason}: {count}")
    print()
    print("Skip Breakdown:")
    for status, count in sorted(s["skip_breakdown"].items()):
        print(f"  {status}: {count}")


async def main():
    """Main entry point."""
    if len(sys.argv) > 1 and sys.argv[1] == "--date":
        # Single date validation mode
        date_str = sys.argv[2]
        date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=IST)
        print(f"Validating single date: {date.date()}")

        async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
            result = await process_day(client, date)
            if result and "entry_price" in result:
                print(f"Trade found: {result}")
            else:
                print(f"No trade: {result}")

    elif len(sys.argv) == 3:
        # Custom date range
        start_str, end_str = sys.argv[1], sys.argv[2]
        start_date = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=IST)
        end_date = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=IST)
        results = await run_backtest(start_date, end_date)
        summary = summarize(results)
        print_results(summary)

    else:
        # Default: last 365 days
        end_date = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=365)
        results = await run_backtest(start_date, end_date)
        summary = summarize(results)
        print_results(summary)


if __name__ == "__main__":
    asyncio.run(main())
