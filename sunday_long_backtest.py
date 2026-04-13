"""
sunday_long_backtest.py
------------------------
Sunday expiry — LONG straddle, same-day entry.
Entry slots : 5:30 AM to 1:00 PM IST every 30 min (16 slots)
TP grid     : $100 $150 $200 $250 $300 $350 $400 $500 $600 $700 $800
SL grid     : $50  $75  $100 $150 $200 $250 $300 $400 $500

Win rule:
  TP  → WIN always
  SL  → LOSS always
  EXP → WIN if pnl ≥ +$50, else LOSS
  EXP PnL shown per-row as  +$X(W)  or  +$X(~)  or  -$X(L)
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

TP_LEVELS  = [100, 150, 200, 250, 300, 350, 400, 500, 600, 700, 800]
SL_LEVELS  = [50,  75,  100, 150, 200, 250, 300, 400, 500]
EXP_WIN_MIN = 50

# 16 slots: 5:30, 6:00, 6:30 … 13:00
ENTRY_SLOTS = []
for h in range(5, 14):
    for m in (30, 0) if h == 5 else (0, 30):
        if h == 13 and m == 30: break
        label = f"{h}:{'30' if m==30 else '00'}"
        ENTRY_SLOTS.append((label, h, m))
# clean up: 5:30, 6:00, 6:30, ..., 13:00
ENTRY_SLOTS = []
t_h, t_m = 5, 30
while (t_h, t_m) <= (13, 0):
    label = f"{t_h}:{'30' if t_m==30 else '00'}"
    ENTRY_SLOTS.append((label, t_h, t_m))
    t_m += 30
    if t_m == 60:
        t_m = 0
        t_h += 1

SKIP_DATES = {"2025-10-11"}

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

sun_products = [p for p in all_products
                if datetime.strptime(p["settlement_date_ist"], "%Y-%m-%d").strftime("%A") == "Sunday"]
by_settle = defaultdict(list)
for p in sun_products:
    by_settle[p["settlement_date_ist"]].append(p)

all_dates     = sorted(d for d in by_settle.keys() if d not in SKIP_DATES)
btc_ts_sorted = sorted(all_btc_1m.keys())
print(f"  {len(all_products)} products | {len(all_1m)} symbols | "
      f"{len(all_btc_1m):,} BTC 1m bars | {len(all_dates)} Sundays | "
      f"{len(ENTRY_SLOTS)} slots x {len(TP_LEVELS)} TPs x {len(SL_LEVELS)} SLs = "
      f"{len(ENTRY_SLOTS)*len(TP_LEVELS)*len(SL_LEVELS)*len(all_dates):,} simulations")


def btc_at(ts):
    lo, hi = 0, len(btc_ts_sorted) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if btc_ts_sorted[mid] < ts: lo = mid + 1
        else: hi = mid
    best = btc_ts_sorted[lo]
    return all_btc_1m[best]["c"] if abs(best - ts) <= 600 else 0.0


def get_setup(settle_date_str, h, m):
    settle_dt  = datetime.strptime(settle_date_str, "%Y-%m-%d")
    entry_ist  = IST.localize(datetime(settle_dt.year, settle_dt.month, settle_dt.day, h, m))
    entry_ts   = int(entry_ist.astimezone(timezone.utc).timestamp())
    expiry_ist = IST.localize(datetime(settle_dt.year, settle_dt.month, settle_dt.day, 17, 30))
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
        "settle": settle_date_str,
        "symbol": atm["symbol"], "strike": strike, "btc": round(btc, 0),
        "entry_price": ec["c"], "entry_ts": ec["t"],
        "candles": tc, "expiry_ts": expiry_ts,
        "tte_hrs": round((expiry_ist - entry_ist).total_seconds() / 3600, 1),
    }


def simulate_long(setup, tp_gain, sl_loss):
    ep, entry_ts, expiry_ts = setup["entry_price"], setup["entry_ts"], setup["expiry_ts"]
    tp_lvl = ep + tp_gain
    sl_lvl = ep - sl_loss
    max_up, max_down, last_p, last_dt = 0.0, 0.0, ep, ""
    for c in setup["candles"]:
        if c["t"] < entry_ts: continue
        px = c["c"]
        if px <= 0: continue
        dt = datetime.fromtimestamp(c["t"], tz=timezone.utc).astimezone(IST).strftime("%H:%M")
        max_up   = max(max_up,   px - ep)
        max_down = max(max_down, ep - px)
        last_p, last_dt = px, dt
        if c["t"] > expiry_ts: break
        if px >= tp_lvl:
            return {"type": "TP", "exit": round(px, 0), "time": dt,
                    "pnl": round(px - ep, 0),
                    "max_gain": round(max_up, 0), "max_loss": round(max_down, 0)}
        if px <= sl_lvl:
            return {"type": "SL", "exit": round(px, 0), "time": dt,
                    "pnl": round(px - ep, 0),
                    "max_gain": round(max_up, 0), "max_loss": round(max_down, 0)}
    lc  = [c for c in setup["candles"] if c["t"] <= expiry_ts and c["c"] > 0]
    ep2 = lc[-1]["c"] if lc else last_p
    dt2 = datetime.fromtimestamp(lc[-1]["t"], tz=timezone.utc).astimezone(IST).strftime("%H:%M") if lc else last_dt
    return {"type": "EXP", "exit": round(ep2, 0), "time": dt2,
            "pnl": round(ep2 - ep, 0),
            "max_gain": round(max_up, 0), "max_loss": round(max_down, 0)}


def is_win(r):
    if r["type"] == "TP":  return True
    if r["type"] == "SL":  return False
    return r["pnl"] >= EXP_WIN_MIN


# ── Pre-build all setups ────────────────────────────────────────────────────────
print("Building setups...", flush=True)
setups = {}
for d in all_dates:
    for slabel, h, m in ENTRY_SLOTS:
        s = get_setup(d, h, m)
        if s:
            setups[(d, slabel)] = s

# ── Simulate all combinations ──────────────────────────────────────────────────
print("Simulating...", flush=True)
results = {}
for (d, slabel), setup in setups.items():
    for tp in TP_LEVELS:
        for sl in SL_LEVELS:
            results[(d, slabel, tp, sl)] = simulate_long(setup, tp, sl)


def slot_stats(slabel, tp, sl):
    rs    = {d: results[(d, slabel, tp, sl)] for d in all_dates if (d, slabel, tp, sl) in results}
    pnls  = [r["pnl"]      for r in rs.values()]
    gains = [r["max_gain"] for r in rs.values()]
    n     = len(pnls) or 1
    tps_  = sum(1 for r in rs.values() if r["type"] == "TP")
    sls_  = sum(1 for r in rs.values() if r["type"] == "SL")
    exp_rs   = [r for r in rs.values() if r["type"] == "EXP"]
    exp_wins = sum(1 for r in exp_rs if r["pnl"] >= EXP_WIN_MIN)
    exp_loss = len(exp_rs) - exp_wins
    exp_pnl_w = sum(r["pnl"] for r in exp_rs if r["pnl"] >= EXP_WIN_MIN)
    exp_pnl_l = sum(r["pnl"] for r in exp_rs if r["pnl"] < EXP_WIN_MIN)
    wins = tps_ + exp_wins
    tot  = sum(pnls)
    tte  = next((setups[(d, slabel)]["tte_hrs"] for d in all_dates if (d, slabel) in setups), 0)
    return dict(n=n, tps=tps_, sls=sls_,
                exp_wins=exp_wins, exp_loss=exp_loss,
                exp_pnl_w=exp_pnl_w, exp_pnl_l=exp_pnl_l,
                wins=wins, tot=tot, wr=wins/n*100,
                avg=tot/n, best=max(pnls) if pnls else 0,
                worst=min(pnls) if pnls else 0,
                avg_gain=sum(gains)/n if gains else 0, tte=tte)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Journal for best config per slot
# ══════════════════════════════════════════════════════════════════════════════
for slabel, h, m in ENTRY_SLOTS:
    # find best config for this slot (highest total pnl)
    best_tp, best_sl, best_tot = TP_LEVELS[0], SL_LEVELS[0], -999999
    for tp in TP_LEVELS:
        for sl in SL_LEVELS:
            st = slot_stats(slabel, tp, sl)
            if st["tot"] > best_tot:
                best_tot, best_tp, best_sl = st["tot"], tp, sl
    st = slot_stats(slabel, best_tp, best_sl)
    tte_s = next((setups[(d, slabel)]["tte_hrs"] for d in all_dates if (d, slabel) in setups), "?")

    print(f"\n{'═'*110}")
    print(f"  LONG {slabel} IST | TTE ~{tte_s}h | Best: TP=+${best_tp} SL=-${best_sl} | "
          f"WR={st['wr']:.0f}% | Total=${st['tot']:+,.0f} | Avg=${st['avg']:+.0f}/trade")
    print(f"  Breakdown: {st['tps']} TPs  {st['sls']} SLs  "
          f"{st['exp_wins']} ExpW(≥+$50)={st['exp_pnl_w']:+,.0f}  "
          f"{st['exp_loss']} ExpL(<+$50)={st['exp_pnl_l']:+,.0f}")
    print(f"{'─'*110}")
    print(f"  {'Date':12} {'Symbol':26} {'BTC':>8} {'Entry':>6} {'Exit':>5} {'Type':>4} "
          f"{'PnL':>7} {'Time':>6} {'ExpNote':>14} {'MaxGain':>8} {'MaxLoss':>8}")
    print(f"  {'─'*105}")

    for d in all_dates:
        key = (d, slabel)
        if key not in setups: continue
        s  = setups[key]
        ep = s["entry_price"]
        r  = results.get((d, slabel, best_tp, best_sl))
        if not r: continue
        win  = is_win(r)
        icon = "✅" if win else "❌"

        exp_note = ""
        if r["type"] == "EXP":
            tag = "W" if r["pnl"] >= EXP_WIN_MIN else ("~0" if r["pnl"] >= 0 else "L")
            exp_note = f"EXP {r['pnl']:+.0f}({tag})"

        print(f"  {d:12} {s['symbol']:26} ${s['btc']:>7,.0f} ${ep:>5.0f} "
              f"${r['exit']:>5.0f} {icon}{r['type']:>3} "
              f"{r['pnl']:>+7.0f} {r['time']:>6} "
              f"{exp_note:>14} "
              f"${r['max_gain']:>+7.0f} ${r['max_loss']:>+7.0f}")

    print(f"  {'─'*105}")
    # summary across all TP levels for best SL
    print(f"  All TP levels (SL=-${best_sl}):")
    print(f"  {'TP':>6} {'WR%':>5} {'TPs':>4} {'SLs':>4} {'ExpW':>5} {'ExpWpnl':>8} "
          f"{'ExpL':>5} {'ExpLpnl':>8} {'Total':>10} {'Avg':>8} {'Best':>7} {'Worst':>7}")
    print(f"  {'─'*90}")
    for tp in TP_LEVELS:
        st2 = slot_stats(slabel, tp, best_sl)
        print(f"  TP+${tp:<4} {st2['wr']:>4.0f}%  {st2['tps']:>3}  {st2['sls']:>4}  "
              f"{st2['exp_wins']:>4}  ${st2['exp_pnl_w']:>+7,.0f}  "
              f"{st2['exp_loss']:>4}  ${st2['exp_pnl_l']:>+7,.0f}  "
              f"${st2['tot']:>+9,.0f}  ${st2['avg']:>+7,.0f}  "
              f"${st2['best']:>+6,.0f}  ${st2['worst']:>+6,.0f}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Master grid (all slot x TP x SL combos)
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n\n{'═'*175}")
print(f"  MASTER GRID — LONG Sunday Expiry | 5:30AM–1PM | {len(all_dates)} Sundays | EXP≥+$50=win")
print(f"{'═'*175}")
print(f"  {'Entry':>6} {'TP':>5} {'SL':>5} {'N':>4} {'WR%':>5} "
      f"{'TPs':>4} {'SLs':>4} {'ExpW':>5} {'ExpWpnl':>8} {'ExpL':>5} {'ExpLpnl':>8} "
      f"{'TotalPnL':>11} {'Avg':>9} {'Best':>8} {'Worst':>8} {'AvgMxGn':>8} {'TTE':>6}")
print(f"  {'─'*170}")

all_configs = []
for slabel, h, m in ENTRY_SLOTS:
    first = True
    for tp in TP_LEVELS:
        for sl in SL_LEVELS:
            st = slot_stats(slabel, tp, sl)
            if not st["n"]: continue
            slot_disp = slabel if first else ""
            first = False
            marker = " ◀" if st["wr"] >= 65 and st["tot"] > 2000 else ""
            print(f"  {slot_disp:>6} +${tp:<4} -${sl:<4} {st['n']:>4} {st['wr']:>4.0f}%  "
                  f"{st['tps']:>3}  {st['sls']:>4}  {st['exp_wins']:>4}  "
                  f"${st['exp_pnl_w']:>+7,.0f}  {st['exp_loss']:>4}  ${st['exp_pnl_l']:>+7,.0f}  "
                  f"${st['tot']:>+9,.0f}  ${st['avg']:>+8,.0f}  "
                  f"${st['best']:>+7,.0f}  ${st['worst']:>+7,.0f}  "
                  f"${st['avg_gain']:>7.0f}  {st['tte']:>5.1f}h{marker}")
            all_configs.append((st["wr"], st["tot"], slabel, tp, sl,
                                 st["n"], st["tps"], st["sls"],
                                 st["exp_wins"], st["exp_loss"],
                                 st["exp_pnl_w"], st["exp_pnl_l"],
                                 st["avg"], st["best"], st["worst"], st["tte"]))
    print(f"  {'─'*170}")


# ── Top 20 by total PnL ────────────────────────────────────────────────────────
def print_top(title, configs, key_idx, top_n=20):
    W = 100
    print(f"\n{'═'*W}")
    print(f"  {title}")
    print(f"{'═'*W}")

    configs_s = sorted(configs, key=lambda x: x[key_idx], reverse=True)
    for i, c in enumerate(configs_s[:top_n], 1):
        wr,tot,slabel,tp,sl,n,tps,sls,expw,expl,expwp,explp,avg,best,worst,tte = c
        exp_str = f"ExpW={expw}({expwp:+,.0f})  ExpL={expl}({explp:+,.0f})"
        exits   = f"TPs={tps}  SLs={sls}  {exp_str}"
        print(f"  {i:>2}.  {slabel} IST  |  TP +${tp}  SL -${sl}  |  WR {wr:.0f}%  |  Total ${tot:+,.0f}  Avg ${avg:+,.0f}/trade")
        print(f"        {exits}  |  Best ${best:+,.0f}  Worst ${worst:+,.0f}  |  TTE {tte:.1f}h")
        print(f"  {'─'*W}" if i < top_n else "")

print_top("TOP 20 BY TOTAL PnL  (LONG straddle, Sunday, EXP≥+$50=win)", all_configs, 1)
print_top("TOP 20 BY WIN RATE   (LONG straddle, Sunday, EXP≥+$50=win)", all_configs, 0)
