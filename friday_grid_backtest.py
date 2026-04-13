"""
friday_grid_backtest.py
------------------------
Grid search: SL = $200–$800 (step $100) x 5 entry slots x TP = $300 fixed
Shows full journal table + summary grid.
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

TP_PROFIT  = 300
SL_LEVELS  = [200, 300, 400, 500, 600, 700, 800]

ENTRY_SLOTS = [
    ("8PM",  20, 0, 0),
    ("9PM",  21, 0, 0),
    ("10PM", 22, 0, 0),
    ("11PM", 23, 0, 0),
    ("12AM",  0, 0, 1),
]

# ── Load data ──────────────────────────────────────────────────────────────────
print("Loading data...", flush=True)
all_products, all_1m, all_btc_1m = [], {}, {}

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

sat_products = [p for p in all_products
                if datetime.strptime(p["settlement_date_ist"], "%Y-%m-%d").strftime("%A") == "Saturday"]
by_settle = defaultdict(list)
for p in sat_products:
    by_settle[p["settlement_date_ist"]].append(p)

SKIP_DATES = {"2025-10-11"}  # Oct 10 entry — Trump 100% China tariff black swan
all_dates = sorted(d for d in by_settle.keys() if d not in SKIP_DATES)
btc_ts_sorted = sorted(all_btc_1m.keys())
print(f"  {len(all_products)} products | {len(all_1m)} symbols | {len(all_btc_1m):,} BTC 1m bars | {len(all_dates)} Saturdays")

def btc_at(ts):
    lo, hi = 0, len(btc_ts_sorted)-1
    while lo < hi:
        mid = (lo+hi)//2
        if btc_ts_sorted[mid] < ts: lo = mid+1
        else: hi = mid
    best = btc_ts_sorted[lo]
    return all_btc_1m[best]["c"] if abs(best-ts) <= 600 else 0.0

def get_setup(settle_date_str, h, m, nd):
    settle_dt  = datetime.strptime(settle_date_str, "%Y-%m-%d")
    friday_dt  = settle_dt - timedelta(days=1)
    base_dt    = settle_dt if nd else friday_dt
    entry_ist  = IST.localize(datetime(base_dt.year, base_dt.month, base_dt.day, h, m))
    entry_ts   = int(entry_ist.astimezone(timezone.utc).timestamp())
    expiry_ist = IST.localize(datetime(settle_dt.year, settle_dt.month, settle_dt.day, 17, 30))
    expiry_ts  = int(expiry_ist.astimezone(timezone.utc).timestamp())
    btc = btc_at(entry_ts)
    if not btc: return None
    cands = []
    for p in by_settle.get(settle_date_str, []):
        cs = all_1m.get(p["symbol"], [])
        if cs and cs[0]["t"] <= entry_ts + 3600:
            cands.append((abs(p["strike"]-btc), p["strike"], p, cs))
    if not cands: return None
    cands.sort()
    _, strike, atm, candles = cands[0]
    tc = sorted([c for c in candles if c["t"] >= entry_ts-300], key=lambda x: x["t"])
    ec = next((c for c in tc if c["t"] >= entry_ts-300 and c["c"] > 0), None)
    if not ec: return None
    return {"settle": settle_date_str, "friday": friday_dt.strftime("%Y-%m-%d"),
            "symbol": atm["symbol"], "strike": strike, "btc": round(btc,0),
            "entry_price": ec["c"], "entry_ts": ec["t"],
            "candles": tc, "expiry_ts": expiry_ts}

def simulate(setup, sl_loss):
    ep, entry_ts, expiry_ts = setup["entry_price"], setup["entry_ts"], setup["expiry_ts"]
    tp_lvl, sl_lvl = ep - TP_PROFIT, ep + sl_loss
    max_dd, min_p, last_p, last_dt = 0.0, ep, ep, ""
    for c in setup["candles"]:
        if c["t"] < entry_ts: continue
        px = c["c"]
        if px <= 0: continue
        dt = datetime.fromtimestamp(c["t"], tz=timezone.utc).astimezone(IST).strftime("%H:%M")
        max_dd = max(max_dd, px - ep)
        min_p  = min(min_p, px)
        last_p, last_dt = px, dt
        if c["t"] > expiry_ts: break
        if px <= tp_lvl:
            return {"type":"TP",  "exit":round(px,0), "time":dt, "pnl":round(ep-px,0),
                    "max_dd":round(max_dd,0), "max_win":round(ep-min_p,0)}
        if px >= sl_lvl:
            return {"type":"SL",  "exit":round(px,0), "time":dt, "pnl":round(ep-px,0),
                    "max_dd":round(max_dd,0), "max_win":round(ep-min_p,0)}
    lc = [c for c in setup["candles"] if c["t"] <= expiry_ts and c["c"] > 0]
    ep2 = lc[-1]["c"] if lc else last_p
    dt2 = datetime.fromtimestamp(lc[-1]["t"],tz=timezone.utc).astimezone(IST).strftime("%H:%M") if lc else last_dt
    return {"type":"EXP", "exit":round(ep2,0), "time":dt2, "pnl":round(ep-ep2,0),
            "max_dd":round(max_dd,0), "max_win":round(ep-min_p,0)}

# ── Pre-build setups ────────────────────────────────────────────────────────────
print("Building setups...", flush=True)
setups = {}   # setups[(date, slot_label)] = setup dict
for d in all_dates:
    for slabel, h, m, nd in ENTRY_SLOTS:
        s = get_setup(d, h, m, nd)
        if s:
            setups[(d, slabel)] = s

# ── Pre-simulate for all (date, slot, sl_level) ────────────────────────────────
results = {}  # results[(date, slot, sl)] = result dict
for (d, slabel), setup in setups.items():
    for sl in SL_LEVELS:
        results[(d, slabel, sl)] = simulate(setup, sl)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Per-entry-time journal tables (one per slot)
# ══════════════════════════════════════════════════════════════════════════════
for slabel, h, m, nd in ENTRY_SLOTS:
    print(f"\n{'═'*120}")
    print(f"  ENTRY: {slabel} IST  |  TP = +$300  |  SL grid: {', '.join('$'+str(s) for s in SL_LEVELS)}")
    print(f"{'═'*120}")

    # Header
    hdr = f"{'Date':12} {'Symbol':30} {'BTC':>8} {'Entry':>7}"
    for sl in SL_LEVELS:
        hdr += f"  SL${sl:>3} {'PnL':>5} {'DD':>5} {'T':>5}"
    hdr += f"  {'Exp PnL':>8}"
    print(hdr)
    print(f"{'─'*120}")

    for d in all_dates:
        key = (d, slabel)
        if key not in setups:
            continue
        s = setups[key]
        friday = s["friday"]
        ep = s["entry_price"]

        # expiry pnl (no TP/SL)
        lc = [c for c in s["candles"] if c["t"] <= s["expiry_ts"] and c["c"] > 0]
        exp_pnl = round(ep - lc[-1]["c"], 0) if lc else 0

        line = f"{friday:12} {s['symbol']:30} ${s['btc']:>7,.0f} ${ep:>6.0f}"
        for sl in SL_LEVELS:
            r = results.get((d, slabel, sl))
            if not r:
                line += f"  {'N/A':>16}"
                continue
            icon = "✅" if r["pnl"] > 0 else "❌"
            tp_tag = "T" if r["type"]=="TP" else ("S" if r["type"]=="SL" else "E")
            line += f"  {icon}{r['pnl']:>+5.0f} ${r['max_dd']:>4.0f} {r['time']:>5}"
        exp_icon = "✅" if exp_pnl > 0 else "❌"
        line += f"  {exp_icon}${exp_pnl:>+7.0f}"
        print(line)

    print(f"{'─'*120}")

    # Per-SL summary for this slot
    print(f"  {'SL':>6} {'WR':>5} {'TPs':>4} {'SLs':>4} {'Exp':>4} {'TotalPnL':>10} {'Avg':>8} {'Best':>7} {'Worst':>7} {'AvgMaxDD':>9} {'AvgMaxWin':>10}")
    print(f"  {'─'*85}")
    for sl in SL_LEVELS:
        pnls = [results[(d,slabel,sl)]["pnl"] for d in all_dates if (d,slabel,sl) in results]
        dds  = [results[(d,slabel,sl)]["max_dd"] for d in all_dates if (d,slabel,sl) in results]
        mws  = [results[(d,slabel,sl)]["max_win"] for d in all_dates if (d,slabel,sl) in results]
        tps  = sum(1 for d in all_dates if results.get((d,slabel,sl),{}).get("type")=="TP")
        sls  = sum(1 for d in all_dates if results.get((d,slabel,sl),{}).get("type")=="SL")
        exps = sum(1 for d in all_dates if results.get((d,slabel,sl),{}).get("type")=="EXP")
        n    = len(pnls) or 1
        wins = sum(1 for p in pnls if p > 0)
        tot  = sum(pnls)
        print(f"  SL${sl:<4} {wins/n*100:>4.0f}%  {tps:>3}  {sls:>4}  {exps:>3}  ${tot:>+9,.0f}  ${tot/n:>+7,.0f}  ${max(pnls):>+6,.0f}  ${min(pnls):>+6,.0f}  ${sum(dds)/n:>8.0f}  ${sum(mws)/n:>9.0f}")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Master summary grid (all slots x all SL levels)
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n\n{'═'*130}")
print(f"  MASTER GRID — TP=$300 fixed  |  All entry slots x All SL levels  |  23 Saturdays (excl. Oct 10 black swan)")
print(f"{'═'*130}")
print(f"{'Entry':>7} {'SL':>6} {'Trades':>7} {'WR%':>5} {'TPs':>4} {'SLs':>4} {'Exp':>4} "
      f"{'TotalPnL':>11} {'Avg/Trade':>10} {'Best':>8} {'Worst':>8} "
      f"{'AvgMaxDD':>9} {'WorstDD':>8} {'AvgMaxWin':>10} {'BestWin':>8} {'Risk:Reward':>12}")
print(f"{'─'*130}")

best_configs = []
for slabel, h, m, nd in ENTRY_SLOTS:
    first = True
    for sl in SL_LEVELS:
        pnls = [results[(d,slabel,sl)]["pnl"] for d in all_dates if (d,slabel,sl) in results]
        dds  = [results[(d,slabel,sl)]["max_dd"] for d in all_dates if (d,slabel,sl) in results]
        mws  = [results[(d,slabel,sl)]["max_win"] for d in all_dates if (d,slabel,sl) in results]
        tps  = sum(1 for d in all_dates if results.get((d,slabel,sl),{}).get("type")=="TP")
        sls  = sum(1 for d in all_dates if results.get((d,slabel,sl),{}).get("type")=="SL")
        exps = sum(1 for d in all_dates if results.get((d,slabel,sl),{}).get("type")=="EXP")
        n    = len(pnls) or 1
        wins = sum(1 for p in pnls if p > 0)
        tot  = sum(pnls)
        avg  = tot/n
        wr   = wins/n*100
        avg_dd   = sum(dds)/n if dds else 0
        worst_dd = max(dds) if dds else 0
        avg_mw   = sum(mws)/n if mws else 0
        best_mw  = max(mws) if mws else 0
        rr = f"{TP_PROFIT}/{sl} = {TP_PROFIT/sl:.2f}" if sl else "—"

        slot_disp = slabel if first else ""
        first = False
        marker = " ◀ BEST" if wr >= 65 and tot > 2000 else ""
        print(f"{slot_disp:>7} SL${sl:<4} {n:>7} {wr:>4.0f}%  {tps:>3}  {sls:>4}  {exps:>3}"
              f"  ${tot:>+9,.0f}  ${avg:>+8,.0f}  ${max(pnls):>+7,.0f}  ${min(pnls):>+7,.0f}"
              f"  ${avg_dd:>8.0f}  ${worst_dd:>7.0f}  ${avg_mw:>9.0f}  ${best_mw:>7.0f}"
              f"  {rr:>12}{marker}")
        best_configs.append((wr, tot, slabel, sl, n, tps, sls, avg, max(pnls), min(pnls)))
    print(f"{'─'*130}")

# ── Top 10 configs by total PnL ────────────────────────────────────────────────
print(f"\n{'═'*90}")
print(f"  TOP CONFIGS BY TOTAL PnL  (TP=$300 fixed)")
print(f"{'─'*90}")
print(f"  {'#':>2} {'Entry':>7} {'SL':>6} {'WR%':>5} {'TPs':>4} {'SLs':>4} "
      f"{'TotalPnL':>11} {'Avg/Trade':>10} {'Best':>8} {'Worst':>8}")
print(f"  {'─'*85}")
best_configs.sort(key=lambda x: x[1], reverse=True)
for i,(wr,tot,slabel,sl,n,tps,sls,avg,best,worst) in enumerate(best_configs[:10],1):
    print(f"  {i:>2}  {slabel:>7}  SL${sl:<4}  {wr:>4.0f}%  {tps:>3}  {sls:>4}"
          f"  ${tot:>+9,.0f}  ${avg:>+8,.0f}  ${best:>+7,.0f}  ${worst:>+7,.0f}")

# ── Top 10 by win rate ─────────────────────────────────────────────────────────
print(f"\n  TOP CONFIGS BY WIN RATE")
print(f"  {'─'*85}")
best_configs.sort(key=lambda x: (x[0], x[1]), reverse=True)
for i,(wr,tot,slabel,sl,n,tps,sls,avg,best,worst) in enumerate(best_configs[:10],1):
    print(f"  {i:>2}  {slabel:>7}  SL${sl:<4}  {wr:>4.0f}%  {tps:>3}  {sls:>4}"
          f"  ${tot:>+9,.0f}  ${avg:>+8,.0f}  ${best:>+7,.0f}  ${worst:>+7,.0f}")
