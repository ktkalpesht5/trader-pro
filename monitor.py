"""
monitor.py — Live position monitor for short straddle
Fetches every 30s, prints full update every 3min, immediate alert on breach.
"""
import time, json, urllib.request
from datetime import datetime
import pytz

IST       = pytz.timezone("Asia/Kolkata")
SYMBOL    = "MV-BTC-73000-110426"
ENTRY     = 568.0
TP        = ENTRY * 0.35   # 198.80
SL        = ENTRY * 2.00   # 1136.00
DELTA_MAX = 0.45
HARD_EXIT_H, HARD_EXIT_M = 17, 0  # 5:00 PM IST

BASE = "https://api.india.delta.exchange"

def fetch(sym):
    url = f"{BASE}/v2/tickers/{sym}"
    with urllib.request.urlopen(url, timeout=8) as r:
        d = json.loads(r.read())
    res = d.get("result") or {}
    g   = res.get("greeks") or {}
    return {
        "mark":  float(res.get("mark_price") or 0),
        "delta": float(g.get("delta") or 0),
        "theta": float(g.get("theta") or 0),
        "btc":   float(res.get("spot_price") or 0),
    }

def check(t):
    now = datetime.now(IST)
    hard_exit = now.replace(hour=HARD_EXIT_H, minute=HARD_EXIT_M, second=0, microsecond=0)
    mins_left = (hard_exit - now).total_seconds() / 60

    mark  = t["mark"]
    delta = t["delta"]
    btc   = t["btc"]
    pnl   = ENTRY - mark
    pnl_pct = pnl / ENTRY * 100

    alerts = []
    if mark <= TP:
        alerts.append(f"🎯 TP HIT — straddle=${mark:.2f} ≤ TP=${TP:.2f} → EXIT NOW")
    if mark >= SL:
        alerts.append(f"🛑 SL HIT — straddle=${mark:.2f} ≥ SL=${SL:.2f} → EXIT NOW")
    if abs(delta) >= DELTA_MAX:
        alerts.append(f"⚠️  DELTA BREACH — |delta|={abs(delta):.3f} ≥ 0.45 → EXIT NOW")
    if mins_left <= 0:
        alerts.append(f"⏰ HARD EXIT — past 5:00 PM IST → EXIT NOW")
    elif mins_left <= 15:
        alerts.append(f"⏰ 15-min warning — {mins_left:.0f} min to hard exit")

    return {
        "now": now,
        "mark": mark, "delta": delta, "btc": btc,
        "pnl": pnl, "pnl_pct": pnl_pct,
        "mins_left": mins_left,
        "alerts": alerts,
    }

def print_full(c):
    now_str = c["now"].strftime("%H:%M:%S IST")
    bar = "─" * 52
    print(f"\n{bar}")
    print(f"  {now_str}  |  {SYMBOL}")
    print(f"{bar}")
    print(f"  Mark:      ${c['mark']:.2f}   (entry ${ENTRY:.0f})")
    print(f"  PnL:       ${c['pnl']:+.2f}  ({c['pnl_pct']:+.1f}%)")
    print(f"  TP:        ${TP:.2f}  |  SL: ${SL:.2f}")
    print(f"  Delta:     {c['delta']:+.3f}   (limit ±0.45)")
    print(f"  BTC:       ${c['btc']:,.0f}  (strike $73,000  dist {c['btc']-73000:+,.0f})")
    print(f"  Hard exit: {c['mins_left']:.0f} min away")
    if c["alerts"]:
        for a in c["alerts"]:
            print(f"\n  *** {a} ***")
    print(f"{bar}", flush=True)

def print_tick(c, n):
    now_str = c["now"].strftime("%H:%M:%S")
    flag = " ⚠️ " if c["alerts"] else ""
    print(f"  [{now_str}] mark=${c['mark']:.2f}  pnl={c['pnl_pct']:+.1f}%  δ={c['delta']:+.3f}  BTC=${c['btc']:,.0f}{flag}", flush=True)

def main():
    print(f"\n{'═'*52}")
    print(f"  MONITOR START — {datetime.now(IST).strftime('%H:%M IST')}")
    print(f"  {SYMBOL}")
    print(f"  Entry=${ENTRY:.0f}  TP=${TP:.2f}  SL=${SL:.2f}")
    print(f"{'═'*52}\n")

    tick = 0
    RUN_SECS = 540  # 9 minutes per invocation
    start = time.time()

    while time.time() - start < RUN_SECS:
        try:
            t = fetch(SYMBOL)
            c = check(t)
            tick += 1

            # Immediate alert on any breach
            if c["alerts"]:
                print_full(c)
                if any("EXIT NOW" in a for a in c["alerts"]):
                    print("\n*** ACTION REQUIRED — see above ***\n", flush=True)
                    return

            # Full update every 3 minutes (every 6th 30s tick)
            elif tick % 6 == 0:
                print_full(c)
            else:
                print_tick(c, tick)

        except Exception as e:
            print(f"  [fetch error] {e}", flush=True)

        time.sleep(30)

    print(f"\n  [9-min batch done — restart monitor to continue]", flush=True)

if __name__ == "__main__":
    main()
