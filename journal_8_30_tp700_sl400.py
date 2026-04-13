"""
journal_8_30_tp700_sl400.py
-----------------------------
Sunday long straddle trade journal
Entry : 8:30 AM IST (same day as Sunday expiry)
TP    : +$700  (exit when straddle rises $700 above entry)
SL    : -$400  (exit when straddle falls $400 below entry)
Expiry: 5:30 PM IST
EXP win rule: held to expiry, pnl >= +$50 counts as win

Dataset: All available Sundays, Q3-2025 to Q1-2026 (Jul 2025 – Mar 2026)
"""

import json
from datetime import datetime, timezone
from collections import defaultdict
import pytz

IST         = pytz.timezone("Asia/Kolkata")
DATA_DIR    = "data"
ENTRY_H     = 8
ENTRY_M     = 30
TP_GAIN     = 700
SL_LOSS     = 400
EXP_WIN_MIN = 50
SKIP_DATES  = {"2025-10-11"}

PERIODS = [
    ("Q3-2025", "2025-06-28_2025-09-27"),
    ("Q4-2025", "2025-09-28_2025-12-27"),
    ("Q1-2026", "2025-12-28_2026-03-28"),
]

# ── Load data ──────────────────────────────────────────────────────────────────
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

sun_products = [
    p for p in all_products
    if datetime.strptime(p["settlement_date_ist"], "%Y-%m-%d").strftime("%A") == "Sunday"
]
by_settle = defaultdict(list)
for p in sun_products:
    by_settle[p["settlement_date_ist"]].append(p)

all_dates     = sorted(d for d in by_settle.keys() if d not in SKIP_DATES)
btc_ts_sorted = sorted(all_btc_1m.keys())


# ── Helpers ────────────────────────────────────────────────────────────────────
def btc_at(ts):
    lo, hi = 0, len(btc_ts_sorted) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if btc_ts_sorted[mid] < ts:
            lo = mid + 1
        else:
            hi = mid
    best = btc_ts_sorted[lo]
    return all_btc_1m[best]["c"] if abs(best - ts) <= 600 else 0.0


def get_setup(settle_date_str):
    sd       = datetime.strptime(settle_date_str, "%Y-%m-%d")
    entry_ist = IST.localize(datetime(sd.year, sd.month, sd.day, ENTRY_H, ENTRY_M))
    entry_ts  = int(entry_ist.astimezone(timezone.utc).timestamp())
    expiry_ist = IST.localize(datetime(sd.year, sd.month, sd.day, 17, 30))
    expiry_ts  = int(expiry_ist.astimezone(timezone.utc).timestamp())

    btc = btc_at(entry_ts)
    if not btc:
        return None

    cands = []
    for p in by_settle.get(settle_date_str, []):
        cs = all_1m.get(p["symbol"], [])
        if cs and cs[0]["t"] <= entry_ts + 3600:
            cands.append((abs(p["strike"] - btc), p["strike"], p, cs))
    if not cands:
        return None
    cands.sort()
    _, strike, atm, candles = cands[0]

    tc = sorted([c for c in candles if c["t"] >= entry_ts - 300], key=lambda x: x["t"])
    ec = next((c for c in tc if c["t"] >= entry_ts - 300 and c["c"] > 0), None)
    if not ec:
        return None

    return {
        "settle":      settle_date_str,
        "symbol":      atm["symbol"],
        "strike":      strike,
        "btc":         round(btc, 0),
        "entry_price": ec["c"],
        "entry_ts":    ec["t"],
        "candles":     tc,
        "expiry_ts":   expiry_ts,
        "tte_hrs":     round((expiry_ist - entry_ist).total_seconds() / 3600, 1),
    }


def simulate(setup):
    ep, entry_ts, expiry_ts = setup["entry_price"], setup["entry_ts"], setup["expiry_ts"]
    tp_lvl = ep + TP_GAIN
    sl_lvl = ep - SL_LOSS

    max_gain = max_loss = 0.0
    min_gain = min_loss = None        # smallest positive / smallest negative excursion
    last_p, last_dt = ep, ""

    for c in setup["candles"]:
        if c["t"] < entry_ts:
            continue
        px = c["c"]
        if px <= 0:
            continue

        dt   = datetime.fromtimestamp(c["t"], tz=timezone.utc).astimezone(IST).strftime("%H:%M")
        up   = px - ep
        down = ep - px

        max_gain = max(max_gain, up)
        max_loss = max(max_loss, down)
        if up   > 0: min_gain = up   if min_gain is None else min(min_gain, up)
        if down > 0: min_loss = down if min_loss is None else min(min_loss, down)

        last_p, last_dt = px, dt

        if c["t"] > expiry_ts:
            break
        if px >= tp_lvl:
            return {
                "type":     "TP",
                "exit":     tp_lvl,
                "pnl":      TP_GAIN,
                "time":     dt,
                "max_gain": round(max_gain, 0),
                "min_gain": round(min_gain or 0, 0),
                "max_loss": round(max_loss, 0),
                "min_loss": round(min_loss or 0, 0),
            }
        if px <= sl_lvl:
            return {
                "type":     "SL",
                "exit":     sl_lvl,
                "pnl":      -SL_LOSS,
                "time":     dt,
                "max_gain": round(max_gain, 0),
                "min_gain": round(min_gain or 0, 0),
                "max_loss": round(max_loss, 0),
                "min_loss": round(min_loss or 0, 0),
            }

    lc  = [c for c in setup["candles"] if c["t"] <= expiry_ts and c["c"] > 0]
    ep2 = lc[-1]["c"] if lc else last_p
    dt2 = datetime.fromtimestamp(lc[-1]["t"], tz=timezone.utc).astimezone(IST).strftime("%H:%M") if lc else last_dt

    return {
        "type":     "EXP",
        "exit":     round(ep2, 0),
        "pnl":      round(ep2 - ep, 0),
        "time":     dt2,
        "max_gain": round(max_gain, 0),
        "min_gain": round(min_gain or 0, 0),
        "max_loss": round(max_loss, 0),
        "min_loss": round(min_loss or 0, 0),
    }


def is_win(r):
    if r["type"] == "TP":  return True
    if r["type"] == "SL":  return False
    return r["pnl"] >= EXP_WIN_MIN


# ── Build trades ───────────────────────────────────────────────────────────────
trades = []
for d in all_dates:
    s = get_setup(d)
    if s:
        r = simulate(s)
        trades.append((d, s, r))

# ── Print journal ──────────────────────────────────────────────────────────────
W = 155
DIV = "═" * W
div = "─" * W

print(f"\n{DIV}")
print(f"  SUNDAY LONG STRADDLE — TRADE JOURNAL")
print(f"  Entry: 8:30 AM IST  |  TP: +${TP_GAIN}  |  SL: -${SL_LOSS}  |  Expiry: 5:30 PM IST  |  {len(trades)} trades")
print(f"  Exit rules: TP hit = win | SL hit = loss | EXP = win if pnl ≥ +${EXP_WIN_MIN}")
print(f"  MaxGain = peak above entry | MinGain = smallest positive excursion")
print(f"  MaxLoss = peak below entry | MinLoss = shallowest dip below entry")
print(f"{DIV}")

# header
print(
    f"  {'#':>2}  {'Date':>10}  {'Symbol':>28}  {'Strike':>8}  {'BTC':>10}  "
    f"{'Entry':>6}  {'TP@':>6}  {'SL@':>6}  "
    f"{'Exit':>6}  {'Time':>5}  {'Result':>10}  {'PnL':>7}  "
    f"{'MaxGain':>8}  {'MinGain':>8}  {'MaxLoss':>8}  {'MinLoss':>8}  {'RunPnL':>8}"
)
print(f"  {div}")

running = wins = losses = total_pnl = 0

for i, (d, s, r) in enumerate(trades, 1):
    ep     = s["entry_price"]
    tp_tgt = ep + TP_GAIN
    sl_tgt = ep - SL_LOSS
    win    = is_win(r)
    running   += r["pnl"]
    total_pnl += r["pnl"]
    if win: wins   += 1
    else:   losses += 1

    if r["type"] == "TP":
        result = "✅   TP"
    elif r["type"] == "SL":
        result = "❌   SL"
    else:
        tag    = "W" if r["pnl"] >= EXP_WIN_MIN else "L"
        result = f"{'✅' if win else '❌'} EXP({tag})"

    print(
        f"  {i:>2}  {d:>10}  {s['symbol']:>28}  ${s['strike']:>7,.0f}  ${s['btc']:>9,.0f}  "
        f"${ep:>5.0f}  ${tp_tgt:>5.0f}  ${sl_tgt:>5.0f}  "
        f"${r['exit']:>5.0f}  {r['time']:>5}  {result:>10}  ${r['pnl']:>+6,.0f}  "
        f"${r['max_gain']:>+7,.0f}  ${r['min_gain']:>+7,.0f}  "
        f"${r['max_loss']:>7,.0f}  ${r['min_loss']:>7,.0f}  ${running:>+7,.0f}"
    )

# ── Summary ────────────────────────────────────────────────────────────────────
n          = len(trades)
pnls       = [r["pnl"]      for _, _, r in trades]
max_gains  = [r["max_gain"] for _, _, r in trades]
min_gains  = [r["min_gain"] for _, _, r in trades]
max_losses = [r["max_loss"] for _, _, r in trades]
min_losses = [r["min_loss"] for _, _, r in trades]
tps        = sum(1 for _, _, r in trades if r["type"] == "TP")
sls        = sum(1 for _, _, r in trades if r["type"] == "SL")
exps       = sum(1 for _, _, r in trades if r["type"] == "EXP")
exp_wins   = sum(1 for _, _, r in trades if r["type"] == "EXP" and r["pnl"] >= EXP_WIN_MIN)

print(f"\n{DIV}")
print(f"  SUMMARY  —  8:30 AM  |  TP +${TP_GAIN}  |  SL -${SL_LOSS}  |  {n} Sundays")
print(f"  {div}")
print(f"  Trades         : {n}")
print(f"  Win Rate       : {wins/n*100:.0f}%  ({wins}W / {losses}L)")
print(f"  Exit breakdown : {tps} TPs  |  {sls} SLs  |  {exp_wins} EXP-Win  |  {exps-exp_wins} EXP-Loss  ({exps} total EXP)")
print(f"  Total PnL      : ${total_pnl:+,.0f}")
print(f"  Avg / Trade    : ${total_pnl/n:+,.0f}")
print(f"  Best trade     : ${max(pnls):+,.0f}")
print(f"  Worst trade    : ${min(pnls):+,.0f}")
print(f"  {div}")
print(f"  Avg MaxGain    : ${sum(max_gains)/n:,.0f}  |  Peak MaxGain    : ${max(max_gains):,.0f}")
print(f"  Avg MinGain    : ${sum(min_gains)/n:,.0f}  |  Floor MinGain   : ${min(g for g in min_gains if g > 0):,.0f}")
print(f"  Avg MaxLoss    : ${sum(max_losses)/n:,.0f}  |  Worst MaxLoss   : ${max(max_losses):,.0f}")
print(f"  Avg MinLoss    : ${sum(min_losses)/n:,.0f}  |  Shallowest dip  : ${min(l for l in min_losses if l > 0):,.0f}")
print(f"{DIV}")
