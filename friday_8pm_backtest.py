"""
friday_multi_entry_backtest.py
--------------------------------
Backtest: Short ATM straddle at 8PM, 9PM, 10PM, 11PM, Midnight IST every Friday (Saturday expiry).
- TP: straddle falls $300 from entry (profit $300)
- SL: straddle rises $200 from entry (loss $200)
- Also reports: max drawdown, max profit if held to expiry
Uses 1-minute candle data.
"""

import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import pytz

IST      = pytz.timezone("Asia/Kolkata")
DATA_DIR = "data"

PERIODS = [
    ("Q4-2025", "2025-09-28_2025-12-27"),
    ("Q1-2026", "2025-12-28_2026-03-28"),
]

TP_PROFIT = 300
SL_LOSS   = 200

ENTRY_SLOTS = [
    ("8PM",  20, 0,  0),   # (label, hour, minute, is_next_day)
    ("9PM",  21, 0,  0),
    ("10PM", 22, 0,  0),
    ("11PM", 23, 0,  0),
    ("12AM",  0, 0,  1),   # midnight = next day (Saturday)
]

# ── Load data ──────────────────────────────────────────────────────────────────
print("Loading data...")
all_products: list[dict] = []
all_1m: dict[str, list[dict]] = {}
all_btc_1m: dict[int, dict]   = {}

for label, tag in PERIODS:
    with open(f"{DATA_DIR}/straddles_meta_{tag}.json") as f:
        for p in json.load(f):
            p["period"] = label
            all_products.append(p)

    with open(f"{DATA_DIR}/straddles_1m_{tag}.json") as f:
        all_1m.update(json.load(f))

    with open(f"{DATA_DIR}/btc_1m_{tag}.json") as f:
        for c in json.load(f):
            all_btc_1m[c["t"]] = c

print(f"  Products: {len(all_products)} | Symbols: {len(all_1m)} | BTC 1m bars: {len(all_btc_1m):,}")

# Saturday expiry only
sat_products = [p for p in all_products
                if datetime.strptime(p["settlement_date_ist"], "%Y-%m-%d").strftime("%A") == "Saturday"]
by_settle: dict[str, list[dict]] = defaultdict(list)
for p in sat_products:
    by_settle[p["settlement_date_ist"]].append(p)

print(f"  Saturday expiry dates: {len(by_settle)}")

# sorted BTC timestamps for fast lookup
btc_ts_sorted = sorted(all_btc_1m.keys())

def btc_at(ts: int) -> float:
    lo, hi = 0, len(btc_ts_sorted) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if btc_ts_sorted[mid] < ts:
            lo = mid + 1
        else:
            hi = mid
    best = btc_ts_sorted[lo]
    if abs(best - ts) > 600:
        return 0.0
    return all_btc_1m[best]["c"]

# ── Get entry setup for a given settle date + entry hour ──────────────────────
def get_setup(settle_date_str: str, entry_h: int, entry_m: int, next_day: int) -> dict | None:
    settle_dt  = datetime.strptime(settle_date_str, "%Y-%m-%d")
    friday_dt  = settle_dt - timedelta(days=1)
    base_dt    = settle_dt if next_day else friday_dt

    entry_ist  = IST.localize(datetime(base_dt.year, base_dt.month, base_dt.day, entry_h, entry_m))
    entry_ts   = int(entry_ist.astimezone(timezone.utc).timestamp())
    expiry_ist = IST.localize(datetime(settle_dt.year, settle_dt.month, settle_dt.day, 17, 30))
    expiry_ts  = int(expiry_ist.astimezone(timezone.utc).timestamp())

    btc = btc_at(entry_ts)
    if not btc:
        return None

    candidates = []
    for p in by_settle.get(settle_date_str, []):
        cs = all_1m.get(p["symbol"], [])
        if not cs:
            continue
        if cs[0]["t"] <= entry_ts + 3600:
            candidates.append((abs(p["strike"] - btc), p["strike"], p, cs))
    if not candidates:
        return None

    candidates.sort()
    _, strike, atm, candles = candidates[0]

    trade_candles = sorted([c for c in candles if c["t"] >= entry_ts - 300], key=lambda x: x["t"])
    if not trade_candles:
        return None

    entry_candle = next((c for c in trade_candles if c["t"] >= entry_ts - 300 and c["c"] > 0), None)
    if not entry_candle:
        return None

    return {
        "settle_date": settle_date_str,
        "symbol":      atm["symbol"],
        "strike":      strike,
        "btc_entry":   round(btc, 0),
        "entry_price": entry_candle["c"],
        "entry_ts":    entry_candle["t"],
        "candles":     trade_candles,
        "expiry_ts":   expiry_ts,
    }

# ── Simulate ────────────────────────────────────────────────────────────────────
def simulate(setup: dict) -> dict:
    ep        = setup["entry_price"]
    tp_lvl    = ep - TP_PROFIT
    sl_lvl    = ep + SL_LOSS
    entry_ts  = setup["entry_ts"]
    expiry_ts = setup["expiry_ts"]

    max_adverse = 0.0
    min_price   = ep
    last_price  = ep
    last_dt     = ""

    for c in setup["candles"]:
        if c["t"] < entry_ts:
            continue
        price = c["c"]
        if price <= 0:
            continue
        ts  = c["t"]
        dt  = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST).strftime("%H:%M")

        adverse = price - ep
        if adverse > max_adverse:
            max_adverse = adverse
        if price < min_price:
            min_price = price

        last_price = price
        last_dt    = dt

        if ts > expiry_ts:
            break

        if price <= tp_lvl:
            return {"exit_type": "TP",     "exit_price": round(price, 0), "exit_time": dt,
                    "pnl": round(ep - price, 0), "max_dd": round(max_adverse, 0),
                    "max_profit": round(ep - min_price, 0)}
        if price >= sl_lvl:
            return {"exit_type": "SL",     "exit_price": round(price, 0), "exit_time": dt,
                    "pnl": round(ep - price, 0), "max_dd": round(max_adverse, 0),
                    "max_profit": round(ep - min_price, 0)}

    # expiry
    last_cs = [c for c in setup["candles"] if c["t"] <= expiry_ts and c["c"] > 0]
    ep2     = last_cs[-1]["c"] if last_cs else last_price
    dt2     = datetime.fromtimestamp(last_cs[-1]["t"], tz=timezone.utc).astimezone(IST).strftime("%H:%M") if last_cs else last_dt
    return {"exit_type": "EXPIRY", "exit_price": round(ep2, 0), "exit_time": dt2,
            "pnl": round(ep - ep2, 0), "max_dd": round(max_adverse, 0),
            "max_profit": round(ep - min_price, 0)}

# ── Build all results ───────────────────────────────────────────────────────────
all_dates = sorted(by_settle.keys())
slot_labels = [s[0] for s in ENTRY_SLOTS]

# results[settle_date][slot_label] = {setup, result}
data: dict[str, dict] = {}

for d in all_dates:
    data[d] = {}
    for label, h, m, nd in ENTRY_SLOTS:
        s = get_setup(d, h, m, nd)
        if s:
            r = simulate(s)
            data[d][label] = {"setup": s, "result": r}

# ── Print mega table ────────────────────────────────────────────────────────────
# Column widths: Date | Symbol | BTC | then per slot: Entry SL TP Exit PnL MaxDD MaxProfit ExitType
SLOT_W = 52  # width per slot block

header1 = f"{'Date':12} {'Symbol':32} {'BTC':>8}"
header2 = f"{'':12} {'':32} {'':>8}"

for sl in slot_labels:
    header1 += f"  {'── ' + sl + ' ──':^50}"
    header2 += f"  {'Entry':>7} {'SL@':>7} {'TP@':>7} {'Exit$':>6} {'Type':>6} {'PnL':>6} {'MaxDD':>6} {'MaxPro':>7} {'Time':>6}"

total_width = 12 + 32 + 8 + len(slot_labels) * 54 + 4
print("=" * total_width)
print(f"FRIDAY NIGHT SHORT STRADDLE — SATURDAY EXPIRY  |  TP=$300  SL=$200  |  24 Saturdays")
print("=" * total_width)
print(header1)
print(header2)
print("-" * total_width)

# Summary accumulators
slot_totals = {sl: {"pnl": 0, "wins": 0, "n": 0, "tp": 0, "sl_hits": 0, "expiry": 0} for sl in slot_labels}
expiry_totals = {sl: {"pnl": 0, "wins": 0} for sl in slot_labels}

rows = []
for d in all_dates:
    settle_dt = datetime.strptime(d, "%Y-%m-%d")
    friday = (settle_dt - timedelta(days=1)).strftime("%Y-%m-%d")

    # Use 8PM symbol for display (most representative)
    sym = data[d].get("8PM", {}).get("setup", {}).get("symbol", "N/A")
    btc_disp = data[d].get("8PM", {}).get("setup", {}).get("btc_entry", 0)
    sym_short = sym.replace("MV-BTC-", "").replace("-", " @ ") if sym != "N/A" else "N/A"

    line = f"{friday:12} {sym:32} ${btc_disp:>7,.0f}"

    for sl in slot_labels:
        entry_data = data[d].get(sl)
        if not entry_data:
            line += f"  {'N/A':>7} {'':>7} {'':>7} {'':>6} {'':>6} {'':>6} {'':>6} {'':>7} {'':>6}"
            continue

        s = entry_data["setup"]
        r = entry_data["result"]
        ep     = s["entry_price"]
        tp_lvl = ep - TP_PROFIT
        sl_lvl = ep + SL_LOSS
        icon   = "✅" if r["pnl"] > 0 else "❌"

        line += (f"  ${ep:>6.0f} ${sl_lvl:>6.0f} ${tp_lvl:>6.0f}"
                 f" ${r['exit_price']:>5.0f} {r['exit_type']:>6}"
                 f" {r['pnl']:>+6.0f} ${r['max_dd']:>5.0f} ${r['max_profit']:>6.0f}"
                 f" {r['exit_time']:>6}")

        slot_totals[sl]["pnl"]  += r["pnl"]
        slot_totals[sl]["n"]    += 1
        slot_totals[sl]["wins"] += 1 if r["pnl"] > 0 else 0
        if r["exit_type"] == "TP":     slot_totals[sl]["tp"]     += 1
        elif r["exit_type"] == "SL":   slot_totals[sl]["sl_hits"] += 1
        else:                          slot_totals[sl]["expiry"]  += 1

    rows.append(line)
    print(line)

# ── Summary row ────────────────────────────────────────────────────────────────
print("-" * total_width)
print(f"\n{'SUMMARY':12} {'':32} {'':>8}", end="")
for sl in slot_labels:
    t = slot_totals[sl]
    n = t["n"] or 1
    wr = t["wins"] / n * 100
    print(f"  {'PnL:':>7} {t['pnl']:>+6.0f}  {'WR:':>5} {wr:>4.0f}%  "
          f"{'TP/SL/EXP:':>10} {t['tp']}/{t['sl_hits']}/{t['expiry']}  "
          f"{'avg:':>4} ${t['pnl']/n:>+5.0f}  {'':>28}", end="")
print()

# ── Per-slot summary table ─────────────────────────────────────────────────────
print(f"\n{'='*80}")
print(f"{'SLOT':>6}  {'Trades':>7} {'Wins':>6} {'WR%':>5} {'TPs':>4} {'SLs':>4} {'Exp':>4} "
      f"{'Total PnL':>11} {'Avg/Trade':>10} {'Best':>7} {'Worst':>7}")
print(f"{'─'*80}")
for sl in slot_labels:
    t = slot_totals[sl]
    n = t["n"] or 1
    # gather individual pnls for this slot
    pnls = [data[d][sl]["result"]["pnl"] for d in all_dates if sl in data[d]]
    best  = max(pnls) if pnls else 0
    worst = min(pnls) if pnls else 0
    wr = t["wins"] / n * 100
    print(f"{sl:>6}  {n:>7} {t['wins']:>6} {wr:>4.0f}%  {t['tp']:>3} {t['sl_hits']:>4} {t['expiry']:>4} "
          f"  ${t['pnl']:>+9,.0f}  ${t['pnl']/n:>+8,.0f}  ${best:>+6,.0f}  ${worst:>+6,.0f}")

# ── Hold-to-expiry table (no TP/SL) ───────────────────────────────────────────
print(f"\n{'='*80}")
print("IF HELD TO EXPIRY — NO TP/SL  (re-simulates ignoring exit rules)")
print(f"{'─'*80}")
print(f"{'SLOT':>6}  {'Trades':>7} {'Wins':>6} {'WR%':>5} {'Total PnL':>11} {'Avg/Trade':>10} "
      f"{'Best':>8} {'Worst':>8}")
print(f"{'─'*80}")

for sl in slot_labels:
    exp_pnls = []
    for d in all_dates:
        if sl not in data[d]:
            continue
        s = data[d][sl]["setup"]
        ep = s["entry_price"]
        last_cs = [c for c in s["candles"] if c["t"] <= s["expiry_ts"] and c["c"] > 0]
        if last_cs:
            exp_pnls.append(ep - last_cs[-1]["c"])

    if not exp_pnls:
        continue
    n     = len(exp_pnls)
    total = sum(exp_pnls)
    wins  = sum(1 for p in exp_pnls if p > 0)
    wr    = wins / n * 100
    print(f"{sl:>6}  {n:>7} {wins:>6} {wr:>4.0f}%  ${total:>+9,.0f}  ${total/n:>+8,.0f}  "
          f"${max(exp_pnls):>+7,.0f}  ${min(exp_pnls):>+7,.0f}")

# ── Per-date hold-to-expiry comparison ────────────────────────────────────────
print(f"\n{'='*100}")
print("HOLD-TO-EXPIRY PnL PER DATE (no TP/SL)  — each slot entry price vs settlement price")
print(f"{'─'*100}")
header = f"{'Date':12} {'Symbol':30}"
for sl in slot_labels:
    header += f"  {sl:>12}"
header += f"  {'Expiry$':>8}"
print(header)
print(f"{'─'*100}")

for d in all_dates:
    friday = (datetime.strptime(d, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    sym    = data[d].get("8PM", {}).get("setup", {}).get("symbol", "")
    line   = f"{friday:12} {sym:30}"
    exp_price = None

    for sl in slot_labels:
        if sl not in data[d]:
            line += f"  {'N/A':>12}"
            continue
        s = data[d][sl]["setup"]
        ep = s["entry_price"]
        last_cs = [c for c in s["candles"] if c["t"] <= s["expiry_ts"] and c["c"] > 0]
        if last_cs:
            exp_p  = last_cs[-1]["c"]
            exp_price = exp_p
            pnl    = ep - exp_p
            icon   = "✅" if pnl > 0 else "❌"
            line  += f"  {icon} ${ep:>5.0f} {pnl:>+5.0f}"
        else:
            line += f"  {'no data':>12}"

    if exp_price:
        line += f"  ${exp_price:>7.0f}"
    print(line)
