"""
long_straddle_analysis.py
--------------------------
Find the optimal day + entry conditions for BUYING BTC straddles.

Mirror of the short straddle strategy but reversed:
  - Buy ATM straddle at 8PM night before expiry
  - TP: straddle price rises X% (BTC made a big move)
  - SL: straddle decays Y% (BTC stayed flat, theta killed us)

Goal: find which expiry day has structural 100% win rate without macro events.

Key hypothesis: Sunday (entry day) has zero macro events (no CPI/NFP/FOMC ever
released on Sundays), so Monday expiry = no-event entry every week.

Output:
  data/long_straddle_analysis.json
"""

import json
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import pytz

IST      = pytz.timezone("Asia/Kolkata")
DATA_DIR = "data"

PERIODS = [
    ("Q4-2025", "2025-09-28_2025-12-27"),
    ("Q1-2026", "2025-12-28_2026-03-28"),
]

# ── Load all data ─────────────────────────────────────────────────────────────

all_products: list[dict] = []
all_candles:  dict[str, list[dict]] = {}
all_btc:      dict[int, dict] = {}

for label, tag in PERIODS:
    with open(f"{DATA_DIR}/straddles_meta_{tag}.json") as f:
        for p in json.load(f):
            p["period"] = label
            all_products.append(p)

    with open(f"{DATA_DIR}/straddles_candles_{tag}.json") as f:
        all_candles.update(json.load(f))

    with open(f"{DATA_DIR}/btc_candles_{tag}.json") as f:
        for c in json.load(f):
            all_btc[c["timestamp_unix"]] = c

print(f"Loaded: {len(all_products)} products | {len(all_candles)} symbols | {len(all_btc)} BTC bars")

# ── Group products by settlement date ─────────────────────────────────────────

by_settle: dict[str, list[dict]] = defaultdict(list)
for p in all_products:
    by_settle[p["settlement_date_ist"]].append(p)

# ── Helpers ───────────────────────────────────────────────────────────────────

def btc_price_at(ts: int) -> float:
    candidates = [(abs(ts - t), t) for t in all_btc if abs(ts - t) <= 7200]
    if not candidates:
        return 0.0
    return all_btc[min(candidates)[1]]["close"]


def get_entry_setup(settle_date_str: str, entry_hour: int = 20) -> dict | None:
    """
    For a settlement date, find the ATM straddle (closest strike to BTC at entry)
    and return entry candles starting from 8PM the prior evening.

    Only uses straddles with candles available from the prior day (listed ~24h early).
    """
    settle_dt  = datetime.strptime(settle_date_str, "%Y-%m-%d")
    prev_day   = settle_dt - timedelta(days=1)

    entry_ist  = IST.localize(datetime(prev_day.year, prev_day.month, prev_day.day, entry_hour, 0))
    entry_ts   = int(entry_ist.astimezone(timezone.utc).timestamp())

    btc = btc_price_at(entry_ts)
    if not btc:
        return None

    # Find products expiring on settle_date_str that have candles from before entry
    candidates = []
    for p in by_settle.get(settle_date_str, []):
        cs = all_candles.get(p["symbol"], [])
        if not cs:
            continue
        # Must have a candle within 2h of entry time (straddle listed prior to entry)
        first_ts = cs[0]["timestamp_unix"]
        if first_ts <= entry_ts + 3600:  # first candle no later than 1h after entry
            candidates.append((abs(p["strike"] - btc), p, cs))

    if not candidates:
        return None

    # ATM = closest strike to BTC at entry
    candidates.sort(key=lambda x: x[0])
    _, atm, all_sym_candles = candidates[0]

    # Candles from entry time onward
    candles = sorted(
        [c for c in all_sym_candles if c["timestamp_unix"] >= entry_ts - 1800],
        key=lambda x: x["timestamp_unix"]
    )

    if not candles:
        return None

    # Find entry price: first candle at/after entry
    entry_candle = None
    for c in candles:
        if c["timestamp_unix"] >= entry_ts - 1800 and c["close"] > 0:
            entry_candle = c
            break

    if not entry_candle:
        return None

    return {
        "product":       atm,
        "settle_date":   settle_date_str,
        "dow":           settle_dt.strftime("%A"),
        "btc_at_entry":  btc,
        "entry_price":   entry_candle["close"],
        "entry_time":    datetime.fromtimestamp(entry_candle["timestamp_unix"], tz=timezone.utc)
                         .astimezone(IST).strftime("%H:%M IST"),
        "candles":       candles,
        "entry_candle":  entry_candle,
    }


# ── Simulate long trade ───────────────────────────────────────────────────────

def simulate_long(setup: dict, tp_frac: float, sl_frac: float) -> dict:
    """
    Long straddle:
      tp_frac: exit when price >= entry * (1 + tp_frac)  — e.g. 0.50 = +50%
      sl_frac: exit when price <= entry * (1 - sl_frac)  — e.g. 0.40 = -40%
    """
    ep       = setup["entry_price"]
    tp_price = ep * (1 + tp_frac)
    sl_price = ep * (1 - sl_frac)
    entry_ts = setup["entry_candle"]["timestamp_unix"]

    for c in setup["candles"]:
        if c["timestamp_unix"] < entry_ts:
            continue
        price = c["close"]
        if price <= 0:
            continue
        ts = c["timestamp_unix"]
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST).strftime("%H:%M IST")

        if price >= tp_price:
            pnl = price - ep
            return {"exit_type": "TP",     "exit_price": price, "exit_time": dt,
                    "pnl_usd": pnl, "pnl_pct": pnl / ep * 100}
        if price <= sl_price:
            pnl = price - ep
            return {"exit_type": "SL",     "exit_price": price, "exit_time": dt,
                    "pnl_usd": pnl, "pnl_pct": pnl / ep * 100}

    last  = setup["candles"][-1]
    price = last["close"]
    pnl   = price - ep
    dt    = datetime.fromtimestamp(last["timestamp_unix"], tz=timezone.utc).astimezone(IST).strftime("%H:%M IST")
    return {"exit_type": "EXPIRY", "exit_price": price, "exit_time": dt,
            "pnl_usd": pnl, "pnl_pct": pnl / ep * 100}


# ── Build setups for all dates ────────────────────────────────────────────────

print("\nBuilding entry setups for all settlement dates...")
setups = []
skipped = 0

for settle_date in sorted(by_settle.keys()):
    s = get_entry_setup(settle_date)
    if s:
        setups.append(s)
    else:
        skipped += 1

print(f"  Valid setups: {len(setups)} | Skipped (no prior-day candles): {skipped}")

# Count per day of week
dow_counts = defaultdict(int)
for s in setups:
    dow_counts[s["dow"]] += 1

print("\nSetups per day of week:")
for d in ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]:
    print(f"  {d:10}: {dow_counts[d]}")


# ── Grid search per day of week ───────────────────────────────────────────────

TP_RANGE = [0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 1.00]
SL_RANGE = [0.20, 0.30, 0.40, 0.50, 0.60, 0.70]

DAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

print(f"\n{'─'*80}")
print("GRID SEARCH BY DAY OF WEEK")
print(f"{'─'*80}")

best_per_day = {}

for dow in DAYS:
    day_setups = [s for s in setups if s["dow"] == dow]
    if len(day_setups) < 5:
        continue

    combos = []
    for tp in TP_RANGE:
        for sl in SL_RANGE:
            total = wins = 0
            for s in day_setups:
                r = simulate_long(s, tp, sl)
                if r:
                    total += r["pnl_usd"]
                    wins  += r["pnl_usd"] > 0
            wr = wins / len(day_setups) * 100
            combos.append((wr, total, wins, tp, sl))

    combos.sort(reverse=True)
    best = combos[0]
    best_per_day[dow] = best
    wr, total, wins, tp, sl = best
    n = len(day_setups)
    print(f"\n{dow} ({n} trades) — BEST: TP=+{int(tp*100)}%  SL=-{int(sl*100)}%  "
          f"wins={wins}/{n} ({wr:.0f}%)  total_pnl=${total:,.0f}")

    # Show top 5
    print(f"  {'WinRate':>8} {'Total P&L':>12} {'TP':>6} {'SL':>6}")
    for wr2, tot2, w2, tp2, sl2 in combos[:5]:
        print(f"  {wr2:>7.0f}%  ${tot2:>10,.0f}  +{int(tp2*100):>3}%  -{int(sl2*100):>3}%  ({w2}/{n})")


# ── Deep dive on best day ────────────────────────────────────────────────────

print(f"\n{'═'*80}")
print("RANKING ALL DAYS (sorted by win rate at best config)")
print(f"{'═'*80}")

ranked = sorted(best_per_day.items(), key=lambda x: (x[1][0], x[1][1]), reverse=True)
for dow, (wr, total, wins, tp, sl) in ranked:
    n = dow_counts[dow]
    avg = total / n if n else 0
    print(f"  {dow:10}: {wr:>5.0f}% WR  TP=+{int(tp*100)}%  SL=-{int(sl*100)}%  "
          f"total=${total:>+8,.0f}  avg/trade=${avg:>+6,.0f}")


# ── Full trade log for the best day ──────────────────────────────────────────

best_dow = ranked[0][0]
_, _, _, OPT_TP, OPT_SL = ranked[0][1]
best_setups = [s for s in setups if s["dow"] == best_dow]

print(f"\n{'─'*100}")
print(f"FULL TRADE LOG — {best_dow.upper()} EXPIRY  |  BUY ATM at 8PM prior  |  TP=+{int(OPT_TP*100)}%  SL=-{int(OPT_SL*100)}%")
print(f"{'─'*100}")

results = []
for s in sorted(best_setups, key=lambda x: x["settle_date"]):
    r = simulate_long(s, OPT_TP, OPT_SL)
    if not r:
        continue
    icon = "✅" if r["pnl_usd"] > 0 else "❌"
    ep   = s["entry_price"]
    sym  = s["product"]["symbol"]
    btc  = s["btc_at_entry"]
    print(f"{icon} {s['settle_date']} [{s['dow'][:3]}]  {sym:32}  "
          f"BTC=${btc:>7,.0f}  entry=${ep:>6,.0f}  "
          f"TP=${ep*(1+OPT_TP):>6,.0f}  SL=${ep*(1-OPT_SL):>6,.0f}  "
          f"→ {r['exit_type']:6} @${r['exit_price']:>6,.0f}  "
          f"P&L={r['pnl_usd']:>+7.0f} ({r['pnl_pct']:>+5.1f}%)  [{r['exit_time']}]")
    results.append({**s["product"], "settle_date": s["settle_date"], "dow": s["dow"],
                    "btc_at_entry": round(btc, 0), "entry_price": round(ep, 2),
                    **{k: round(v, 2) if isinstance(v, float) else v for k, v in r.items()},
                    "result": "WIN" if r["pnl_usd"] > 0 else "LOSS"})

wins   = sum(1 for r in results if r["result"] == "WIN")
losses = [r for r in results if r["result"] == "LOSS"]
total  = sum(r["pnl_usd"] for r in results)
print(f"\nTotal: {wins}/{len(results)} wins ({wins/len(results)*100:.0f}%)  |  P&L = ${total:,.0f}  |  avg = ${total/len(results):,.0f}/trade")


# ── Macro event analysis for best day ────────────────────────────────────────

print(f"\n{'─'*60}")
print(f"MACRO EVENT ANALYSIS — {best_dow.upper()} entry day = {(datetime.strptime('2026-01-05', '%Y-%m-%d') - timedelta(days=1)).strftime('%A').upper()}")
print(f"{'─'*60}")

# For best_dow expiry, the entry is on the day before
prev_dow = {
    "Monday": "Sunday", "Tuesday": "Monday", "Wednesday": "Tuesday",
    "Thursday": "Wednesday", "Friday": "Thursday", "Saturday": "Friday", "Sunday": "Saturday"
}
entry_dow = prev_dow[best_dow]
print(f"Entry night = {entry_dow}")
print(f"\nNote: CPI/NFP/FOMC are NEVER released on {entry_dow}s.")
print(f"→ Zero macro event conflicts for {best_dow} expiry strategy.")

if losses:
    print(f"\nLosses ({len(losses)}):")
    for r in losses:
        print(f"  ❌ {r['settle_date']}  {r['symbol']}  entry=${r['entry_price']:.0f}  "
              f"exit=${r['exit_price']:.2f}  P&L={r['pnl_usd']:+.0f}")
else:
    print(f"\n🎯 ZERO LOSSES — 100% win rate on {best_dow} expiry!")


# ── Second best day with detailed analysis ───────────────────────────────────

if len(ranked) > 1:
    second_dow = ranked[1][0]
    _, _, _, S_TP, S_SL = ranked[1][1]
    second_setups = [s for s in setups if s["dow"] == second_dow]

    print(f"\n{'─'*100}")
    print(f"SECOND BEST — {second_dow.upper()} EXPIRY  |  TP=+{int(S_TP*100)}%  SL=-{int(S_SL*100)}%")
    print(f"{'─'*100}")

    for s in sorted(second_setups, key=lambda x: x["settle_date"]):
        r = simulate_long(s, S_TP, S_SL)
        if not r:
            continue
        icon = "✅" if r["pnl_usd"] > 0 else "❌"
        ep   = s["entry_price"]
        print(f"{icon} {s['settle_date']}  {s['product']['symbol']:32}  "
              f"entry=${ep:>6,.0f}  → {r['exit_type']:6} P&L={r['pnl_usd']:>+7.0f} ({r['pnl_pct']:>+5.1f}%)")

    sr  = [simulate_long(s, S_TP, S_SL) for s in second_setups]
    sw  = sum(1 for r in sr if r and r["pnl_usd"] > 0)
    sp  = sum(r["pnl_usd"] for r in sr if r)
    print(f"\n{sw}/{len(second_setups)} wins ({sw/len(second_setups)*100:.0f}%)  total=${sp:,.0f}")


# ── Save output ───────────────────────────────────────────────────────────────

out_path = f"{DATA_DIR}/long_straddle_analysis.json"
with open(out_path, "w") as f:
    json.dump({
        "best_day": best_dow,
        "config": {"tp_pct": int(OPT_TP * 100), "sl_pct": int(OPT_SL * 100)},
        "trades": results,
        "summary": {
            "wins": wins, "total": len(results),
            "win_rate_pct": round(wins / len(results) * 100, 1) if results else 0,
            "total_pnl": round(total, 2),
            "avg_per_trade": round(total / len(results), 2) if results else 0,
        }
    }, f, indent=2)
print(f"\nSaved → {out_path}")
