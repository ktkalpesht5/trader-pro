"""
backtest_grid.py
----------------
Grid search over TP% and SL% to find the optimal parameters for the
BTC daily straddle short strategy.

Methodology:
  1. Fetch data once (same pipeline as backtest.py), cache to /tmp/bt_grid_cache.json
  2. Re-run simulation in-memory for every TP × SL combination — no extra API calls
  3. Print ranked table + heatmap grid

TP  range: 10%, 15%, 20%, 25%, 30%, 35%, 40%, 50%, 60%
SL  range: 120%, 140%, 160%, 170%, 200%, 250%, 300%, no-SL (∞)

That's 9 × 8 = 72 combinations.
"""

import asyncio
import bisect
import json
import math
import httpx
import pytz
from datetime import datetime, timedelta, timezone
from collections import defaultdict

IST      = pytz.timezone("Asia/Kolkata")
BASE_URL = "https://api.india.delta.exchange"
CACHE    = "/tmp/bt_grid_cache.json"

# ── Grid parameters ────────────────────────────────────────────────────────────
TP_VALUES  = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60]
SL_VALUES  = [1.20, 1.40, 1.60, 1.70, 2.00, 2.50, 3.00, None]  # None = no SL

# Fixed params
DELTA_EXIT       = 0.45
BTC_MOVE_EXIT    = 700
MIN_HOURS        = 4.0
HARD_EXIT_HOUR   = 16
HARD_EXIT_MINUTE = 30
N_CONTRACTS      = 1_000   # 1 BTC
CONTRACT_BTC     = 0.001

# ── Black-Scholes helpers ──────────────────────────────────────────────────────

def _norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def _iv_from_straddle(price, spot, strike, hours):
    if hours <= 0 or price <= 0 or spot <= 0:
        return 40.0
    T = hours / (365 * 24)
    denom = 2 * spot * math.sqrt(T / (2 * math.pi))
    return min((price / denom) * 100, 500.0) if denom > 0 else 40.0

def _bs_delta(spot, strike, iv_pct, hours):
    if hours <= 0 or iv_pct <= 0 or spot <= 0:
        return 0.0
    T = hours / (365 * 24)
    sigma = iv_pct / 100
    d1 = (math.log(spot / strike) + 0.5 * sigma**2 * T) / (sigma * math.sqrt(T))
    return 2 * _norm_cdf(d1) - 1

def _bs_theta_hourly(spot, strike, iv_pct, hours):
    if hours <= 0 or iv_pct <= 0 or spot <= 0:
        return 0.0
    T = hours / (365 * 24)
    sigma = iv_pct / 100
    d1 = (math.log(spot / strike) + 0.5 * sigma**2 * T) / (sigma * math.sqrt(T))
    nprime = math.exp(-d1**2 / 2) / math.sqrt(2 * math.pi)
    return spot * sigma * nprime / math.sqrt(T) / (365 * 24)

def _bs_vega(spot, strike, iv_pct, hours):
    if hours <= 0 or iv_pct <= 0 or spot <= 0:
        return 0.0
    T = hours / (365 * 24)
    sigma = iv_pct / 100
    d1 = (math.log(spot / strike) + 0.5 * sigma**2 * T) / (sigma * math.sqrt(T))
    nprime = math.exp(-d1**2 / 2) / math.sqrt(2 * math.pi)
    return 2 * spot * math.sqrt(T) * nprime / 100

def _calc_rv(btc_hourly, btc_hourly_times, before_ts):
    hi = bisect.bisect_left(btc_hourly_times, before_ts)
    hourly = btc_hourly[max(0, hi - 25):hi]
    if len(hourly) < 10:
        return 0.0
    closes = [c["close"] for c in hourly[-25:]]
    rets = [math.log(closes[i] / closes[i-1]) for i in range(1, len(closes))]
    if not rets:
        return 0.0
    mean = sum(rets) / len(rets)
    var  = sum((r - mean)**2 for r in rets) / len(rets)
    return math.sqrt(var) * math.sqrt(365 * 24) * 100


# ── Entry filter ───────────────────────────────────────────────────────────────

def apply_entry_filters(noon_ts, entry_price, btc_noon, strike, hours,
                        btc_5m, btc_by_ts, btc_times, btc_hourly, btc_hourly_times):
    """Returns (skip: bool, half_size: bool)"""
    # A2: 4-hr BTC move
    ts_4h = noon_ts - 4 * 3600
    r4 = (ts_4h // 300) * 300
    btc_4h = (btc_by_ts.get(r4) or btc_by_ts.get(r4+300) or
              btc_by_ts.get(r4-300) or btc_by_ts.get(r4+600) or
              btc_by_ts.get(r4-600))
    move_4h = abs(btc_noon - btc_4h) if btc_4h else 0.0
    if btc_4h and move_4h >= 800:
        return True, False

    # A3: 24-hr range
    ts_24h = noon_ts - 24 * 3600
    lo = bisect.bisect_left(btc_times, ts_24h)
    hi = bisect.bisect_right(btc_times, noon_ts)
    range_24h = 0.0
    if lo < hi:
        window    = btc_5m[lo:hi]
        range_24h = max(c["high"] for c in window) - min(c["low"] for c in window)
    if range_24h >= 2500:
        return True, False

    # Greeks
    iv    = _iv_from_straddle(entry_price, btc_noon, strike, hours)
    delta = _bs_delta(btc_noon, strike, iv, hours)
    theta = _bs_theta_hourly(btc_noon, strike, iv, hours)
    vega  = _bs_vega(btc_noon, strike, iv, hours)
    rv    = _calc_rv(btc_hourly, btc_hourly_times, noon_ts)
    theta_ratio = theta / entry_price * 100 if entry_price > 0 else 0

    if abs(delta) >= 0.15:
        return True, False
    if theta_ratio <= 2.5:
        return True, False

    b1    = 1 if iv < 55 else 0
    b2    = 1 if (rv > 0 and (iv - rv) > -10) else 0
    b5    = 1 if vega < 18 else 0
    b7    = 1 if move_4h < 400 else 0
    score = b1 + b2 + b5 + b7
    if score < 2:
        return True, False

    return False, (score < 3)


# ── Single straddle simulation (parametric TP/SL) ─────────────────────────────

def simulate_straddle(meta, straddle_5m, btc_by_ts, btc_5m, tp_pct, sl_mult):
    """Returns list of (entry_price, exit_price, net_pnl_pct, half_size, mfe_pct, mae_pct).
    mfe_pct = max favourable excursion % (best unrealised gain, straddle dropped this much)
    mae_pct = max adverse excursion % (worst unrealised loss, straddle rose this much)
    """
    symbol = meta["symbol"]
    parts  = symbol.split("-")
    try:
        strike = float(parts[2])
    except (IndexError, ValueError):
        return []

    st_str = meta.get("settlement_time", "")
    try:
        dt_utc = datetime.fromisoformat(st_str.replace("Z", "+00:00"))
    except Exception:
        return []

    dt_ist        = dt_utc.astimezone(IST)
    hard_exit_ist = dt_ist.replace(hour=HARD_EXIT_HOUR, minute=HARD_EXIT_MINUTE, second=0, microsecond=0)
    hard_exit_ts  = int(hard_exit_ist.astimezone(timezone.utc).timestamp())
    expiry_ts     = int(dt_utc.timestamp())

    btc_times        = [c["time"] for c in btc_5m]
    btc_hourly       = [c for c in btc_5m if c["time"] % 3600 == 0]
    btc_hourly_times = [c["time"] for c in btc_hourly]

    trades = []
    bars   = straddle_5m
    i      = 0

    while i < len(bars):
        bar    = bars[i]
        bar_ts = bar["time"]

        hours_left = (expiry_ts - bar_ts) / 3600
        if hours_left < MIN_HOURS or bar_ts >= hard_exit_ts:
            break
        if bar["close"] <= 0:
            i += 1
            continue

        btc_bar = btc_by_ts.get(bar_ts, 0)
        if btc_bar == 0:
            i += 1
            continue

        skip, half = apply_entry_filters(
            bar_ts, bar["close"], btc_bar, strike, hours_left,
            btc_5m, btc_by_ts, btc_times, btc_hourly, btc_hourly_times,
        )
        if skip:
            i += 1
            continue

        entry_price   = bar["close"]
        tp_target     = entry_price * (1 - tp_pct)
        sl_target     = entry_price * sl_mult if sl_mult is not None else None
        exit_price    = entry_price
        exit_idx      = i
        min_low       = entry_price   # lowest straddle price seen = best gain for seller
        max_high      = entry_price   # highest straddle price seen = worst loss for seller

        j = i + 1
        while j < len(bars):
            eb    = bars[j]
            eb_ts = eb["time"]

            if eb["low"]  < min_low:  min_low  = eb["low"]
            if eb["high"] > max_high: max_high = eb["high"]

            if eb_ts >= hard_exit_ts:
                exit_price = eb["close"]
                exit_idx   = j
                break

            if eb["low"] <= tp_target:
                exit_price = tp_target
                exit_idx   = j
                break

            if sl_target is not None and eb["high"] >= sl_target:
                exit_price = sl_target
                exit_idx   = j
                break

            btc_eb     = btc_by_ts.get(eb_ts, btc_bar)
            hours_exit = max((expiry_ts - eb_ts) / 3600, 0)
            if hours_exit > 0 and eb["close"] > 0 and btc_eb > 0:
                iv_est    = _iv_from_straddle(eb["close"], btc_eb, strike, hours_exit)
                delta_est = _bs_delta(btc_eb, strike, iv_est, hours_exit)
                if abs(delta_est) > DELTA_EXIT:
                    exit_price = eb["close"]
                    exit_idx   = j
                    break

            if btc_eb > 0 and abs(btc_eb - strike) > BTC_MOVE_EXIT:
                exit_price = eb["close"]
                exit_idx   = j
                break

            j += 1

        gross   = (entry_price - exit_price) / entry_price * 100
        net     = (gross - 0.2) * (0.5 if half else 1.0)
        mfe_pct = (entry_price - min_low)  / entry_price * 100   # positive = straddle fell
        mae_pct = (max_high - entry_price) / entry_price * 100   # positive = straddle rose
        trades.append((entry_price, exit_price, net, half, mfe_pct, mae_pct))
        i = exit_idx + 1

    return trades


# ── Metrics ────────────────────────────────────────────────────────────────────

def compute_metrics(all_trades: list) -> dict:
    if not all_trades:
        return {"n": 0, "pnl": 0, "wr": 0, "mdd": 0, "sharpe": 0, "expectancy": 0,
                "avg_w": 0, "avg_l": 0,
                "avg_mfe": 0, "avg_mae": 0, "best_mfe": 0, "worst_mae": 0}

    dpnl  = [net / 100 * ep * N_CONTRACTS * CONTRACT_BTC
             for ep, _, net, _, _, _ in all_trades]
    n     = len(dpnl)
    total = sum(dpnl)
    wins  = sum(1 for d in dpnl if d > 0)
    wr    = wins / n * 100

    cum  = []
    run  = 0.0
    for d in dpnl:
        run += d
        cum.append(run)
    peak = cum[0]
    mdd  = 0.0
    for v in cum:
        if v > peak: peak = v
        dd = peak - v
        if dd > mdd: mdd = dd

    mean = total / n
    std  = math.sqrt(sum((d - mean)**2 for d in dpnl) / n) if n > 1 else 0
    sharpe = (mean / std * math.sqrt(n / (365 * 0.7))) if std > 0 else 0

    avg_w = sum(d for d in dpnl if d > 0) / wins if wins else 0
    avg_l = sum(d for d in dpnl if d <= 0) / (n - wins) if (n - wins) > 0 else 0
    expectancy = (wr/100) * avg_w + (1 - wr/100) * avg_l

    # MFE/MAE in dollar terms (using full-size equivalent for comparability)
    mfe_dollars = [mfe / 100 * ep * N_CONTRACTS * CONTRACT_BTC
                   for ep, _, _, _, mfe, _ in all_trades]
    mae_dollars = [mae / 100 * ep * N_CONTRACTS * CONTRACT_BTC
                   for ep, _, _, _, _, mae in all_trades]

    return {"n": n, "pnl": total, "wr": wr, "mdd": mdd, "sharpe": sharpe,
            "expectancy": expectancy, "avg_w": avg_w, "avg_l": avg_l,
            "avg_mfe": sum(mfe_dollars) / n,
            "avg_mae": sum(mae_dollars) / n,
            "best_mfe": max(mfe_dollars),    # single best gain seen in-trade
            "worst_mae": max(mae_dollars),   # single worst loss seen in-trade
            }


# ── API helpers ────────────────────────────────────────────────────────────────

async def _get(client, path, params=None):
    r = await client.get(path, params=params or {})
    r.raise_for_status()
    return r.json()

async def fetch_candles(client, symbol, start, end, resolution="5m"):
    secs = {"1m":60,"5m":300,"15m":900,"1h":3600,"4h":14400,"1d":86400}.get(resolution, 300)
    out = []
    chunk = start
    while chunk < end:
        chunk_end = min(chunk + secs * 500, end)
        try:
            data = await _get(client, "/v2/history/candles", {
                "symbol": symbol, "resolution": resolution,
                "start": str(chunk), "end": str(chunk_end),
            })
            raw = data.get("result") or []
            for c in raw:
                if isinstance(c, dict):
                    out.append({"time": int(c["time"]), "open": float(c["open"]),
                                "high": float(c["high"]), "low": float(c["low"]),
                                "close": float(c["close"])})
                elif isinstance(c, list) and len(c) >= 5:
                    out.append({"time": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                                "low": float(c[3]), "close": float(c[4])})
        except Exception:
            pass
        chunk = chunk_end
    out.sort(key=lambda x: x["time"])
    seen = set()
    deduped = []
    for c in out:
        if c["time"] not in seen:
            seen.add(c["time"])
            deduped.append(c)
    return deduped

async def fetch_data(days_back=365):
    try:
        with open(CACHE) as f:
            data = json.load(f)
        # btc_by_ts keys are strings in JSON — convert back to int
        data["btc_by_ts"] = {int(k): v for k, v in data["btc_by_ts"].items()}
        print(f"  Loaded cached data ({len(data['straddles'])} straddles) from {CACHE}")
        return data
    except Exception:
        pass

    print("  No cache found — fetching from API (this takes ~2 min)...")
    now_utc     = datetime.now(timezone.utc)
    cutoff_date = (now_utc - timedelta(days=days_back)).date()
    today_ist   = datetime.now(IST).date()

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        # 1. Expired straddles
        print("  Step 1/4 — Expired straddle list...")
        results, after, page = [], None, 0
        while True:
            params = {"contract_types": "move_options", "states": "expired", "page_size": 200}
            if after:
                params["after"] = after
            data = await _get(client, "/v2/products", params)
            products = data.get("result", [])
            if not products:
                break
            btc = [p for p in products if p.get("symbol","").startswith("MV-BTC-")]
            results.extend(btc)
            page += 1
            after = data.get("meta", {}).get("after")
            print(f"    Page {page}: {len(btc)} BTC ({len(results)} total)")
            if not after:
                break

        by_date = defaultdict(list)
        for s in results:
            try:
                dt_utc  = datetime.fromisoformat(s.get("settlement_time","").replace("Z","+00:00"))
                exp_date = dt_utc.astimezone(IST).date()
            except Exception:
                continue
            if cutoff_date <= exp_date < today_ist:
                by_date[exp_date].append(s)
        print(f"  {len(by_date)} expiry dates in range\n")

        # 2. BTC 5-min candles
        print("  Step 2/4 — BTC 5-min candles...")
        start_ts = int((now_utc - timedelta(days=days_back+1)).timestamp())
        end_ts   = int(now_utc.timestamp())
        btc_5m   = await fetch_candles(client, "BTCUSD", start_ts, end_ts, "5m")
        btc_by_ts = {c["time"]: c["close"] for c in btc_5m}
        print(f"  {len(btc_5m)} BTC bars\n")

        # 3. ATM selection
        print("  Step 3/4 — ATM straddle per day...")
        selections = {}
        for exp_date in sorted(by_date.keys()):
            candidates = by_date[exp_date]
            day_ist = IST.localize(datetime(exp_date.year, exp_date.month, exp_date.day, 12, 0))
            noon_ts = int(day_ist.astimezone(timezone.utc).timestamp())
            btc_noon = 0.0
            best_gap = float("inf")
            for ts, price in btc_by_ts.items():
                gap = abs(ts - noon_ts)
                if gap < best_gap and gap <= 600:
                    best_gap = gap
                    btc_noon = price
            if btc_noon == 0:
                continue
            try:
                atm = min(candidates, key=lambda s: abs(float(s.get("strike_price", 0) or 0) - btc_noon))
            except Exception:
                continue
            selections[exp_date] = (atm, btc_noon)
        print(f"  {len(selections)} valid days\n")

        # 4. Straddle candles
        print("  Step 4/4 — Straddle intraday candles...")
        async def _fetch_day(exp_date, meta, _btc):
            st = meta.get("settlement_time", "")
            try:
                dt_utc = datetime.fromisoformat(st.replace("Z", "+00:00"))
            except Exception:
                return exp_date, meta, []
            dt_ist = dt_utc.astimezone(IST)
            prev = dt_ist.replace(hour=17, minute=30, second=0, microsecond=0) - timedelta(days=1)
            candles = await fetch_candles(client, meta["symbol"],
                                          int(prev.astimezone(timezone.utc).timestamp()),
                                          int(dt_utc.timestamp()), "5m")
            return exp_date, meta, candles

        items = list(selections.items())
        raw   = []
        for i in range(0, len(items), 20):
            batch = items[i:i+20]
            res   = await asyncio.gather(*[_fetch_day(d, m, b) for d,(m,b) in batch],
                                          return_exceptions=True)
            raw.extend(res)
            print(f"    Fetched {min(i+20,len(items))}/{len(items)}...")
            if i + 20 < len(items):
                await asyncio.sleep(0.3)

        straddles = []
        for r in raw:
            if not isinstance(r, Exception):
                exp_date, meta, candles = r
                if candles:
                    straddles.append({"meta": meta, "candles": candles})

        cache_data = {
            "btc_5m":    btc_5m,
            "btc_by_ts": btc_by_ts,
            "straddles": straddles,
        }
        with open(CACHE, "w") as f:
            json.dump(cache_data, f)
        print(f"\n  Cached {len(straddles)} straddles to {CACHE}")
        return cache_data


# ── Grid search ────────────────────────────────────────────────────────────────

def run_grid(cache_data: dict) -> list[dict]:
    btc_5m    = cache_data["btc_5m"]
    btc_by_ts = cache_data["btc_by_ts"]
    straddles = cache_data["straddles"]

    total = len(TP_VALUES) * len(SL_VALUES)
    print(f"\n  Running {total} combinations × {len(straddles)} straddles...")

    results = []
    combo   = 0
    for tp in TP_VALUES:
        for sl in SL_VALUES:
            combo += 1
            all_trades = []
            for item in straddles:
                trades = simulate_straddle(item["meta"], item["candles"],
                                           btc_by_ts, btc_5m, tp, sl)
                all_trades.extend(trades)
            m = compute_metrics(all_trades)
            sl_label = f"{(sl-1)*100:.0f}%" if sl is not None else "no-SL"
            results.append({"tp": tp, "sl": sl,
                             "tp_label": f"{tp*100:.0f}%",
                             "sl_label": sl_label, **m})
            print(f"    [{combo:2d}/{total}] TP={tp*100:.0f}%  SL={sl_label:<6}  "
                  f"n={m['n']:3d}  WR={m['wr']:5.1f}%  P&L=${m['pnl']:>+8,.0f}  "
                  f"MDD=${-m['mdd']:>7,.0f}")

    return sorted(results, key=lambda r: r["pnl"], reverse=True)


# ── Output ─────────────────────────────────────────────────────────────────────

def format_results(results: list[dict]) -> str:
    lines = []
    w = 105

    def h(title):
        lines.append(f"\n{'='*w}")
        lines.append(f"  {title}")
        lines.append(f"{'='*w}")

    # Full ranked table
    h("GRID SEARCH — ALL 72 COMBINATIONS  (sorted by Total P&L, 1 BTC position)")
    lines.append(f"  {'Rank':<4} {'TP':>4} {'SL':>6}  {'Trades':>6}  {'WR%':>5}  "
                 f"{'Total P&L':>10}  {'MDD':>9}  {'Sharpe':>7}  "
                 f"{'Expect$/tr':>10}  {'AvgWin$':>8}  {'AvgLoss$':>9}  "
                 f"{'AvgMaxGain$':>11}  {'AvgMaxLoss$':>11}  "
                 f"{'BestGainSeen$':>13}  {'WorstLossSeen$':>14}")
    lines.append(f"  {'-'*130}")
    for i, r in enumerate(results, 1):
        mdd_s = f"${-r['mdd']:>8,.0f}" if r['mdd'] > 0 else "       $0"
        lines.append(
            f"  {i:<4} {r['tp_label']:>4} {r['sl_label']:>6}  "
            f"{r['n']:>6}  {r['wr']:>5.1f}%  "
            f"${r['pnl']:>+9,.0f}  {mdd_s}  "
            f"{r['sharpe']:>7.2f}  "
            f"${r['expectancy']:>+9,.0f}  "
            f"${r['avg_w']:>7,.0f}  "
            f"${r['avg_l']:>8,.0f}  "
            f"${r['avg_mfe']:>10,.0f}  "
            f"${r['avg_mae']:>10,.0f}  "
            f"${r['best_mfe']:>12,.0f}  "
            f"${r['worst_mae']:>13,.0f}"
        )

    sl_labels = [r["sl_label"] for r in results if r["tp"] == TP_VALUES[0]]

    def heatmap(title, key, fmt):
        h(f"HEATMAP — {title}")
        by = {(r["tp"], r["sl_label"]): r[key] for r in results}
        lines.append(f"  {'TP↓ / SL→':<10}" + "".join(f" {l:>9}" for l in sl_labels))
        lines.append(f"  {'-'*10}" + "".join(f" {'-'*9}" for _ in sl_labels))
        for tp in TP_VALUES:
            row = f"  {tp*100:.0f}%{'':<7}"
            for sl_label in sl_labels:
                val = by.get((tp, sl_label), 0)
                row += f" {fmt(val):>9}"
            lines.append(row)

    heatmap("Total P&L ($, 1 BTC)", "pnl",
            lambda v: f"${v:>+6,.0f}")
    heatmap("Win Rate (%)", "wr",
            lambda v: f"{v:.1f}%")
    heatmap("Max Drawdown ($)", "mdd",
            lambda v: f"${-v:>6,.0f}")
    heatmap("Sharpe Ratio", "sharpe",
            lambda v: f"{v:.2f}")
    heatmap("Avg Max Gain Seen per Trade ($)  [MFE — straddle fell this far from entry on avg]",
            "avg_mfe", lambda v: f"${v:>6,.0f}")
    heatmap("Avg Max Loss Seen per Trade ($)  [MAE — straddle rose this far from entry on avg]",
            "avg_mae", lambda v: f"${v:>6,.0f}")

    # Top 10 Sharpe
    h("TOP 10 BY RISK-ADJUSTED RETURN (Sharpe)")
    top_sharpe = sorted(results, key=lambda r: r["sharpe"], reverse=True)[:10]
    lines.append(f"  {'Rank':<4} {'TP':>4} {'SL':>6}  {'Sharpe':>7}  {'P&L':>10}  {'MDD':>9}  "
                 f"{'WR%':>5}  {'Trades':>6}  {'AvgMaxGain$':>11}  {'AvgMaxLoss$':>11}")
    lines.append(f"  {'-'*90}")
    for i, r in enumerate(top_sharpe, 1):
        lines.append(f"  {i:<4} {r['tp_label']:>4} {r['sl_label']:>6}  "
                     f"{r['sharpe']:>7.2f}  ${r['pnl']:>+9,.0f}  "
                     f"${-r['mdd']:>8,.0f}  {r['wr']:>5.1f}%  {r['n']:>6}  "
                     f"${r['avg_mfe']:>10,.0f}  ${r['avg_mae']:>10,.0f}")

    # Top 10 lowest MDD (profitable only)
    profitable = [r for r in results if r["pnl"] > 0]
    h(f"TOP 10 LOWEST DRAWDOWN (profitable combos only, {len(profitable)} total)")
    by_mdd = sorted(profitable, key=lambda r: r["mdd"])[:10]
    lines.append(f"  {'Rank':<4} {'TP':>4} {'SL':>6}  {'MDD':>9}  {'P&L':>10}  {'Sharpe':>7}  "
                 f"{'WR%':>5}  {'AvgMaxGain$':>11}  {'AvgMaxLoss$':>11}")
    lines.append(f"  {'-'*85}")
    for i, r in enumerate(by_mdd, 1):
        lines.append(f"  {i:<4} {r['tp_label']:>4} {r['sl_label']:>6}  "
                     f"${-r['mdd']:>8,.0f}  ${r['pnl']:>+9,.0f}  "
                     f"{r['sharpe']:>7.2f}  {r['wr']:>5.1f}%  "
                     f"${r['avg_mfe']:>10,.0f}  ${r['avg_mae']:>10,.0f}")

    lines.append(f"\n  Notes:")
    lines.append(f"  • SL=no-SL means only TP, delta breach (|Δ|>0.45), BTC move ($700), and hard 4:30 PM exits.")
    lines.append(f"  • Costs: 0.2% slippage round-trip per trade.")
    lines.append(f"  • Half-size (×0.5) applied for 2/4 B-score entries.")
    lines.append(f"  • 1 BTC = 1,000 contracts × 0.001 BTC each.")
    lines.append(f"  • AvgMaxGain$  = average best unrealised profit seen per trade (straddle's lowest point from entry).")
    lines.append(f"  • AvgMaxLoss$  = average worst unrealised loss seen per trade (straddle's highest point from entry).")
    lines.append(f"  • BestGainSeen$ = single trade with the largest unrealised gain ever reached.")
    lines.append(f"  • WorstLossSeen$ = single trade with the largest unrealised loss ever reached.\n")

    return "\n".join(lines)


# ── Entry point ────────────────────────────────────────────────────────────────

async def main():
    print(f"\n{'='*100}")
    print(f"  BTC Straddle Grid Search  —  TP × SL Parameter Optimisation")
    print(f"  TP tested: {[f'{t*100:.0f}%' for t in TP_VALUES]}")
    print(f"  SL tested: {['no-SL' if s is None else f'{(s-1)*100:.0f}%' for s in SL_VALUES]}")
    print(f"{'='*100}\n")

    cache_data = await fetch_data(days_back=365)
    results    = run_grid(cache_data)
    output     = format_results(results)

    print(output)

    out_path = "/Users/biingo/projects/trader-pro/grid_search_results.txt"
    with open(out_path, "w") as f:
        f.write(output)
    print(f"  Saved to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
