"""
build_strategy.py
-----------------
Loads all Saturday data across both periods, runs grid search for optimal
TP/SL/BTC-move thresholds, then outputs:

  data/strategy_trades.json   — every trade with exact symbol + simulated exit
  data/strategy_trades.csv    — flat CSV version
  (prints full strategy summary to stdout)

Usage:
    python build_strategy.py
"""

import json, csv, os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import pytz

IST      = pytz.timezone("Asia/Kolkata")
DATA_DIR = "data"

# ── Load all data ─────────────────────────────────────────────────────────────

PERIODS = [
    ("Q4-2025", "2025-09-28_2025-12-27"),
    ("Q1-2026", "2025-12-28_2026-03-28"),
]

all_trades  = []
all_candles = {}
all_btc     = {}

for label, tag in PERIODS:
    with open(f"{DATA_DIR}/saturday_analysis_{tag}.json") as f:
        for t in json.load(f):
            t["period"] = label
            all_trades.append(t)

    with open(f"{DATA_DIR}/straddles_candles_{tag}.json") as f:
        all_candles.update(json.load(f))

    with open(f"{DATA_DIR}/btc_candles_{tag}.json") as f:
        for c in json.load(f):
            all_btc[c["timestamp_unix"]] = c

print(f"Loaded: {len(all_trades)} trades | {len(all_candles)} symbols | {len(all_btc)} BTC bars\n")

# ── BTC price lookup ──────────────────────────────────────────────────────────

def btc_price_at(ts: int) -> float:
    candidates = [(abs(ts - t), t) for t in all_btc if abs(ts - t) <= 7200]
    if not candidates:
        return 0.0
    return all_btc[min(candidates)[1]]["close"]

# ── Entry candle lookup ───────────────────────────────────────────────────────

def entry_candles(trade: dict):
    friday_str = trade["friday"]
    slot       = trade["entry_slot"]
    friday_dt  = datetime.strptime(friday_str, "%Y-%m-%d")
    thursday   = friday_dt - timedelta(days=1)
    hour_map   = {"8PM_Fri": (thursday, 20, 0), "Midnight": (friday_dt, 0, 0), "4AM_Sat": (friday_dt, 4, 0)}
    base, h, m = hour_map[slot]
    entry_ist  = IST.localize(datetime(base.year, base.month, base.day, h, m))
    entry_ts   = int(entry_ist.astimezone(timezone.utc).timestamp())
    candles    = sorted(
        [c for c in all_candles.get(trade["atm_symbol"], []) if c["timestamp_unix"] >= entry_ts - 1800],
        key=lambda x: x["timestamp_unix"]
    )
    return candles, entry_ts

# ── Simulate single trade ─────────────────────────────────────────────────────

def simulate(trade: dict, tp_frac: float, sl_frac: float, btc_limit: float) -> dict:
    """
    tp_frac   : exit when straddle <= entry * tp_frac   (e.g. 0.45 = 55% decay TP)
    sl_frac   : exit when straddle >= entry * sl_frac   (e.g. 1.70 = 70% rise SL)
    btc_limit : exit when |BTC - strike| >= btc_limit
    """
    sym          = trade["atm_symbol"]
    entry_price  = trade["entry_price"]
    strike       = trade["atm_strike"]
    candles, _   = entry_candles(trade)

    if not candles:
        return {}

    tp_price = entry_price * tp_frac
    sl_price = entry_price * sl_frac

    for c in candles:
        price = c["close"]
        if price <= 0:
            continue
        ts    = c["timestamp_unix"]
        dt    = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST).strftime("%H:%M IST")
        btc   = btc_price_at(ts)
        btc_move = abs(btc - strike) if btc else 0

        if price <= tp_price:
            pnl = entry_price - price
            return {"exit_type": "TP",       "exit_price": price, "exit_time": dt,
                    "pnl_usd": pnl, "pnl_pct": pnl/entry_price*100, "btc_at_exit": btc}
        if price >= sl_price:
            pnl = entry_price - price
            return {"exit_type": "SL",       "exit_price": price, "exit_time": dt,
                    "pnl_usd": pnl, "pnl_pct": pnl/entry_price*100, "btc_at_exit": btc}
        if btc_move >= btc_limit:
            pnl = entry_price - price
            return {"exit_type": "BTC_MOVE", "exit_price": price, "exit_time": dt,
                    "pnl_usd": pnl, "pnl_pct": pnl/entry_price*100, "btc_at_exit": btc,
                    "btc_move": round(btc_move, 0)}

    last  = candles[-1]
    price = last["close"]
    pnl   = entry_price - price
    dt    = datetime.fromtimestamp(last["timestamp_unix"], tz=timezone.utc).astimezone(IST).strftime("%H:%M IST")
    btc   = btc_price_at(last["timestamp_unix"])
    return {"exit_type": "EXPIRY", "exit_price": price, "exit_time": dt,
            "pnl_usd": pnl, "pnl_pct": pnl/entry_price*100, "btc_at_exit": btc}

# ── Grid search ───────────────────────────────────────────────────────────────

print("Grid searching TP/SL/BTC-move combinations...")
best = []
for tp  in [0.35, 0.40, 0.45, 0.50, 0.55]:
    for sl  in [1.50, 1.60, 1.70, 1.80, 2.00]:
        for bm  in [700, 1000, 1500, 2000, 9999]:
            total = wins = 0
            for t in all_trades:
                r = simulate(t, tp, sl, bm)
                if r:
                    total += r["pnl_usd"]
                    wins  += r["pnl_usd"] > 0
            best.append((total, wins, tp, sl, bm))

best.sort(reverse=True)
print(f"\n{'Total P&L':>12} {'W/L':>7} {'TP_decay':>9} {'SL_rise':>8} {'BTC_Limit':>10}")
print("─" * 52)
for total, wins, tp, sl, bm in best[:15]:
    bm_s = f"${bm:,}" if bm < 9999 else "none"
    print(f"${total:>11,.0f}  {wins:>3}/{len(all_trades)}  -{int((1-tp)*100)}% decay"
          f"  +{int((sl-1)*100)}% SL  {bm_s:>10}")

# ── Pick optimal config ───────────────────────────────────────────────────────

opt_total, opt_wins, OPT_TP, OPT_SL, OPT_BM = best[0]
print(f"\n✅ OPTIMAL CONFIG: TP={int((1-OPT_TP)*100)}% decay  |  SL=+{int((OPT_SL-1)*100)}%  |  BTC move limit=${OPT_BM:,}")

# Also show the "balanced" config (good P&L + sensible SL)
# Pick best config with SL <= 1.70 and BTC limit <= 1500
balanced = [b for b in best if b[3] <= 1.70 and b[4] <= 1500]
if balanced:
    bal_total, bal_wins, BAL_TP, BAL_SL, BAL_BM = balanced[0]
    print(f"🔒 BALANCED CONFIG: TP={int((1-BAL_TP)*100)}% decay  |  SL=+{int((BAL_SL-1)*100)}%  |  BTC move limit=${BAL_BM:,}  → Total P&L=${bal_total:,.0f}")

# ── Run full trade log with optimal config ────────────────────────────────────

print(f"\n{'─'*100}")
print("FULL TRADE LOG — OPTIMAL CONFIG")
print(f"{'─'*100}")

HEADERS = ["period","saturday","entry_slot","atm_symbol","strike","btc_at_entry",
           "entry_price","tp_price","sl_price","exit_type","exit_price",
           "exit_time","pnl_usd","pnl_pct","btc_at_exit","result"]
rows = []

by_week = defaultdict(list)
for t in sorted(all_trades, key=lambda x: (x["friday"], x["entry_slot"])):
    r = simulate(t, OPT_TP, OPT_SL, OPT_BM)
    if not r:
        continue
    tp_price = round(t["entry_price"] * OPT_TP, 2)
    sl_price = round(t["entry_price"] * OPT_SL, 2)
    row = {
        "period":       t["period"],
        "saturday":     t["friday"],
        "entry_slot":   t["entry_slot"],
        "atm_symbol":   t["atm_symbol"],
        "strike":       t["atm_strike"],
        "btc_at_entry": t["btc_spot_at_entry"],
        "entry_price":  t["entry_price"],
        "tp_price":     tp_price,
        "sl_price":     sl_price,
        "exit_type":    r["exit_type"],
        "exit_price":   round(r["exit_price"], 2),
        "exit_time":    r["exit_time"],
        "pnl_usd":      round(r["pnl_usd"], 2),
        "pnl_pct":      round(r["pnl_pct"], 1),
        "btc_at_exit":  round(r.get("btc_at_exit", 0), 0),
        "result":       "WIN" if r["pnl_usd"] > 0 else "LOSS",
    }
    rows.append(row)
    by_week[t["friday"]].append(row)

    icon = "✅" if row["result"] == "WIN" else "❌"
    print(f"{icon} {t['friday']} [{t['entry_slot']:10}] {t['atm_symbol']:30} "
          f"entry=${t['entry_price']:>7.0f}  TP=${tp_price:>7.0f}  SL=${sl_price:>7.0f}  "
          f"→ {r['exit_type']:8} @${r['exit_price']:>7.0f}  P&L={r['pnl_usd']:>+8.2f} ({r['pnl_pct']:>+5.1f}%)")

# Weekly combined
print(f"\n{'─'*60}")
print("WEEKLY COMBINED P&L")
print(f"{'─'*60}")
grand_total = 0
weekly_wins = 0
for sat in sorted(by_week):
    week_pnl = sum(r["pnl_usd"] for r in by_week[sat])
    grand_total += week_pnl
    weekly_wins += week_pnl > 0
    icon = "✅" if week_pnl > 0 else "❌"
    slots = "  ".join(f"{r['entry_slot']:10}:{r['pnl_usd']:>+8.2f}" for r in by_week[sat])
    print(f"{icon} {sat}  combined={week_pnl:>+9.2f}   {slots}")

print(f"\nGrand total: ${grand_total:,.2f}  |  Weekly wins: {weekly_wins}/{len(by_week)}")

# ── Exit-type breakdown ───────────────────────────────────────────────────────
exit_counts = defaultdict(lambda: {"n":0,"pnl":0,"wins":0})
for r in rows:
    e = exit_counts[r["exit_type"]]
    e["n"] += 1; e["pnl"] += r["pnl_usd"]; e["wins"] += r["result"]=="WIN"

print(f"\nEXIT TYPE BREAKDOWN:")
for etype, d in sorted(exit_counts.items()):
    print(f"  {etype:10}: {d['n']:>3} trades  wins={d['wins']}/{d['n']}  total_pnl=${d['pnl']:>+9,.2f}  avg=${d['pnl']/d['n']:>+7,.2f}")

# ── Save outputs ──────────────────────────────────────────────────────────────
json_out = f"{DATA_DIR}/strategy_trades.json"
csv_out  = f"{DATA_DIR}/strategy_trades.csv"

with open(json_out, "w") as f:
    json.dump({"config": {"tp_pct": int((1-OPT_TP)*100), "sl_pct": int((OPT_SL-1)*100),
                          "btc_move_limit": OPT_BM}, "trades": rows}, f, indent=2)

with open(csv_out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=HEADERS)
    w.writeheader(); w.writerows(rows)

print(f"\nSaved → {json_out}")
print(f"Saved → {csv_out}")
