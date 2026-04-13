"""
compare_fri_vs_sun.py
----------------------
Side-by-side comparison:
  Strategy A: SHORT Saturday expiry — entry Friday 10PM IST | TP -$300 | SL +$500
  Strategy B: LONG  Sunday  expiry — entry Sunday  8:30AM IST | TP +$700 | SL -$400
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
SKIP_DATES = {"2025-10-11"}

# ── Load ───────────────────────────────────────────────────────────────────────
all_products, all_1m, all_btc_1m = [], {}, {}
for label, tag in PERIODS:
    with open(f"{DATA_DIR}/straddles_meta_{tag}.json") as f:
        for p in json.load(f): all_products.append(p)
    with open(f"{DATA_DIR}/straddles_1m_{tag}.json") as f:
        all_1m.update(json.load(f))
    with open(f"{DATA_DIR}/btc_1m_{tag}.json") as f:
        for c in json.load(f): all_btc_1m[c["t"]] = c

btc_ts_sorted = sorted(all_btc_1m.keys())

def btc_at(ts):
    lo, hi = 0, len(btc_ts_sorted) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if btc_ts_sorted[mid] < ts: lo = mid + 1
        else: hi = mid
    best = btc_ts_sorted[lo]
    return all_btc_1m[best]["c"] if abs(best - ts) <= 600 else 0.0

# ── Saturday products (for Fri 10PM short) ────────────────────────────────────
sat_products = [p for p in all_products
                if datetime.strptime(p["settlement_date_ist"], "%Y-%m-%d").strftime("%A") == "Saturday"]
by_settle_sat = defaultdict(list)
for p in sat_products: by_settle_sat[p["settlement_date_ist"]].append(p)
sat_dates = sorted(d for d in by_settle_sat if d not in SKIP_DATES)

# ── Sunday products (for Sun 8:30AM long) ────────────────────────────────────
sun_products = [p for p in all_products
                if datetime.strptime(p["settlement_date_ist"], "%Y-%m-%d").strftime("%A") == "Sunday"]
by_settle_sun = defaultdict(list)
for p in sun_products: by_settle_sun[p["settlement_date_ist"]].append(p)
sun_dates = sorted(d for d in by_settle_sun if d not in SKIP_DATES)


def get_setup(settle_date_str, by_settle, entry_h, entry_m, prev_day=False):
    sd       = datetime.strptime(settle_date_str, "%Y-%m-%d")
    base     = sd - timedelta(days=1) if prev_day else sd
    entry_ist = IST.localize(datetime(base.year, base.month, base.day, entry_h, entry_m))
    entry_ts  = int(entry_ist.astimezone(timezone.utc).timestamp())
    expiry_ist = IST.localize(datetime(sd.year, sd.month, sd.day, 17, 30))
    expiry_ts  = int(expiry_ist.astimezone(timezone.utc).timestamp())
    btc = btc_at(entry_ts)
    if not btc: return None
    cands = []
    for p in by_settle.get(settle_date_str, []):
        cs = all_1m.get(p["symbol"], [])
        if cs and cs[0]["t"] <= entry_ts + 3600:
            cands.append((abs(p["strike"] - btc), p["strike"], p, cs))
    if not cands: return None
    cands.sort()
    _, strike, atm, candles = cands[0]
    tc = sorted([c for c in candles if c["t"] >= entry_ts - 300], key=lambda x: x["t"])
    ec = next((c for c in tc if c["t"] >= entry_ts - 300 and c["c"] > 0), None)
    if not ec: return None
    return {
        "settle": settle_date_str, "symbol": atm["symbol"], "strike": strike,
        "btc": round(btc, 0), "entry_price": ec["c"],
        "entry_ts": ec["t"], "candles": tc, "expiry_ts": expiry_ts,
    }


def simulate_short(setup, tp_drop=300, sl_rise=500):
    """Short straddle: profit when price drops, loss when price rises."""
    ep, entry_ts, expiry_ts = setup["entry_price"], setup["entry_ts"], setup["expiry_ts"]
    tp_lvl = ep - tp_drop   # exit when straddle falls to here
    sl_lvl = ep + sl_rise   # exit when straddle rises to here
    last_p, last_dt = ep, ""
    for c in setup["candles"]:
        if c["t"] < entry_ts: continue
        px = c["c"]
        if px <= 0: continue
        dt = datetime.fromtimestamp(c["t"], tz=timezone.utc).astimezone(IST).strftime("%H:%M")
        last_p, last_dt = px, dt
        if c["t"] > expiry_ts: break
        if px <= tp_lvl:
            return {"type": "TP", "exit": tp_lvl, "pnl": tp_drop, "time": dt}
        if px >= sl_lvl:
            return {"type": "SL", "exit": sl_lvl, "pnl": -sl_rise, "time": dt}
    lc  = [c for c in setup["candles"] if c["t"] <= expiry_ts and c["c"] > 0]
    ep2 = lc[-1]["c"] if lc else last_p
    dt2 = datetime.fromtimestamp(lc[-1]["t"], tz=timezone.utc).astimezone(IST).strftime("%H:%M") if lc else last_dt
    return {"type": "EXP", "exit": round(ep2, 0), "pnl": round(ep - ep2, 0), "time": dt2}


def simulate_long(setup, tp_gain=700, sl_loss=400):
    """Long straddle: profit when price rises, loss when price falls."""
    ep, entry_ts, expiry_ts = setup["entry_price"], setup["entry_ts"], setup["expiry_ts"]
    tp_lvl = ep + tp_gain
    sl_lvl = ep - sl_loss
    last_p, last_dt = ep, ""
    for c in setup["candles"]:
        if c["t"] < entry_ts: continue
        px = c["c"]
        if px <= 0: continue
        dt = datetime.fromtimestamp(c["t"], tz=timezone.utc).astimezone(IST).strftime("%H:%M")
        last_p, last_dt = px, dt
        if c["t"] > expiry_ts: break
        if px >= tp_lvl:
            return {"type": "TP", "exit": tp_lvl, "pnl": tp_gain, "time": dt}
        if px <= sl_lvl:
            return {"type": "SL", "exit": sl_lvl, "pnl": -sl_loss, "time": dt}
    lc  = [c for c in setup["candles"] if c["t"] <= expiry_ts and c["c"] > 0]
    ep2 = lc[-1]["c"] if lc else last_p
    dt2 = datetime.fromtimestamp(lc[-1]["t"], tz=timezone.utc).astimezone(IST).strftime("%H:%M") if lc else last_dt
    return {"type": "EXP", "exit": round(ep2, 0), "pnl": round(ep2 - ep, 0), "time": dt2}


# ── Build trades ───────────────────────────────────────────────────────────────
SHORT_TP, SHORT_SL = 500, 800
LONG_TP,  LONG_SL  = 700, 400
EXP_WIN_MIN = 50

fri_trades, sun_trades = [], []

for d in sat_dates:
    s = get_setup(d, by_settle_sat, 22, 0, prev_day=True)  # Friday 10PM
    if s:
        r = simulate_short(s, SHORT_TP, SHORT_SL)
        fri_trades.append((d, s, r))

for d in sun_dates:
    s = get_setup(d, by_settle_sun, 8, 30, prev_day=False)  # Sunday 8:30AM
    if s:
        r = simulate_long(s, LONG_TP, LONG_SL)
        sun_trades.append((d, s, r))


def summarise(trades, label, tp, sl, direction):
    n = len(trades)
    pnls = [r["pnl"] for _, _, r in trades]
    tps  = sum(1 for _, _, r in trades if r["type"] == "TP")
    sls  = sum(1 for _, _, r in trades if r["type"] == "SL")
    exps = sum(1 for _, _, r in trades if r["type"] == "EXP")
    if direction == "short":
        wins = sum(1 for p in pnls if p > 0)
    else:
        exp_wins = sum(1 for _, _, r in trades if r["type"] == "EXP" and r["pnl"] >= EXP_WIN_MIN)
        wins = tps + exp_wins
    losses = n - wins
    total  = sum(pnls)
    avg    = total / n
    # running equity for max drawdown
    run = 0; peak = 0; max_dd = 0
    for p in pnls:
        run += p
        if run > peak: peak = run
        dd = peak - run
        if dd > max_dd: max_dd = dd
    return {
        "label": label, "n": n, "wins": wins, "losses": losses,
        "tps": tps, "sls": sls, "exps": exps,
        "total": total, "avg": avg,
        "best": max(pnls), "worst": min(pnls), "max_dd": max_dd,
        "wr": wins/n*100,
    }


A = summarise(fri_trades, f"SHORT Sat expiry | Entry Fri 10PM | TP +${SHORT_TP} | SL -${SHORT_SL}", SHORT_TP, SHORT_SL, "short")
B = summarise(sun_trades, f"LONG  Sun expiry | Entry Sun 8:30AM | TP +${LONG_TP} | SL -${LONG_SL}", LONG_TP, LONG_SL, "long")

W = 70
print(f"\n{'═'*W}")
print(f"  STRATEGY COMPARISON")
print(f"{'═'*W}")

rows = [
    ("Strategy",   A["label"],          B["label"]),
    ("Direction",  "SHORT straddle",    "LONG straddle"),
    ("Entry",      "Friday 10:00 PM",   "Sunday 8:30 AM"),
    ("Expiry",     "Saturday 5:30 PM",  "Sunday 5:30 PM"),
    ("TP / SL",    f"+${SHORT_TP} / -${SHORT_SL}", f"+${LONG_TP} / -${LONG_SL}"),
    ("───",        "───",               "───"),
    ("Trades",     str(A["n"]),         str(B["n"])),
    ("Win Rate",   f"{A['wr']:.0f}%  ({A['wins']}W / {A['losses']}L)",
                   f"{B['wr']:.0f}%  ({B['wins']}W / {B['losses']}L)"),
    ("TPs / SLs / EXPs", f"{A['tps']} / {A['sls']} / {A['exps']}", f"{B['tps']} / {B['sls']} / {B['exps']}"),
    ("Total PnL",  f"${A['total']:+,.0f}", f"${B['total']:+,.0f}"),
    ("Avg / Trade",f"${A['avg']:+,.0f}", f"${B['avg']:+,.0f}"),
    ("Best Trade", f"${A['best']:+,.0f}", f"${B['best']:+,.0f}"),
    ("Worst Trade",f"${A['worst']:+,.0f}", f"${B['worst']:+,.0f}"),
    ("Max Drawdown",f"${A['max_dd']:,.0f}", f"${B['max_dd']:,.0f}"),
]

print(f"  {'Metric':<18}  {'SHORT (Fri 10PM)':<26}  {'LONG (Sun 8:30AM)'}")
print(f"  {'─'*65}")
for row in rows:
    metric, va, vb = row
    if metric == "───":
        print(f"  {'─'*65}")
        continue
    if metric == "Strategy":
        continue
    print(f"  {metric:<18}  {va:<26}  {vb}")

print(f"{'═'*W}")

# ── Per-trade journal (side by side by week) ──────────────────────────────────
print(f"\n{'═'*W}")
print(f"  WEEKLY BREAKDOWN")
print(f"{'═'*W}")
print(f"  {'Week':>4}  {'Fri Date':>10}  {'Fri PnL':>9}  {'Sun Date':>10}  {'Sun PnL':>9}  {'Combined':>10}")
print(f"  {'─'*60}")

fri_by_week = {}
for d, s, r in fri_trades:
    dt = datetime.strptime(d, "%Y-%m-%d")
    # key = the Saturday date
    fri_by_week[d] = r["pnl"]

sun_by_week = {}
for d, s, r in sun_trades:
    sun_by_week[d] = r["pnl"]

# Align: Sat date -> next day = Sun date
all_weeks = []
for sat_d in sat_dates:
    sat_dt = datetime.strptime(sat_d, "%Y-%m-%d")
    sun_d  = (sat_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    # fri entry date
    fri_d  = (sat_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    fp = fri_by_week.get(sat_d)
    sp = sun_by_week.get(sun_d)
    all_weeks.append((fri_d, sat_d, sun_d, fp, sp))

run = 0
for i, (fri_d, sat_d, sun_d, fp, sp) in enumerate(all_weeks, 1):
    fp_str = f"${fp:>+6,.0f}" if fp is not None else "  N/A  "
    sp_str = f"${sp:>+6,.0f}" if sp is not None else "  N/A  "
    combined = (fp or 0) + (sp or 0)
    run += combined
    fp_icon = ("✅" if fp and fp > 0 else "❌") if fp is not None else "  "
    sp_icon = ("✅" if sp and sp > 0 else "❌") if sp is not None else "  "
    comb_icon = "✅" if combined > 0 else "❌"
    print(f"  {i:>4}  {fri_d:>10}  {fp_icon}{fp_str}  {sun_d:>10}  {sp_icon}{sp_str}  {comb_icon}${combined:>+7,.0f}  run=${run:>+7,.0f}")

combined_total = sum((fp or 0) + (sp or 0) for _,_,_,fp,sp in all_weeks)
atot = f"${A['total']:+,.0f}"
btot = f"${B['total']:+,.0f}"
ctot = f"${combined_total:+,.0f}"
print(f"  {'─'*60}")
print(f"  {'TOTAL':>4}  {'':>10}  {atot:>9}  {'':>10}  {btot:>9}  {ctot:>10}")
print(f"{'═'*W}")
