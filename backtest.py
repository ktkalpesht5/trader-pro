"""
backtest.py
-----------
Real-data backtest of the BTC daily straddle short strategy.

Uses actual historical prices from Delta Exchange India:
  /v2/products?states=expired&contract_types=move_options  — contract metadata
  /v2/history/candles?symbol=MV-BTC-...                    — real intraday prices

Methodology:
  - Entry:      12:00 PM IST noon (mechanical, backtesting-validated)
  - Exit:       walk forward on real 5-min straddle candles
  - TP/SL:      checked against bar low/high for realistic fills
  - Delta exit: BS-estimated using real straddle + BTC price
  - Hard exit:  4:30 PM IST unconditional

No synthetic IV. No BS price approximation. Real market prices.
"""

import asyncio
import bisect
import math
import httpx
import pytz
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Optional

IST      = pytz.timezone("Asia/Kolkata")
BASE_URL = "https://api.india.delta.exchange"

# ── Strategy config ────────────────────────────────────────────────────────────
TP_PCT           = 0.25   # 25% decay (grid search optimal)
SL_MULT          = 1.20   # 120% of entry (grid search optimal)
DELTA_EXIT            = 0.45
BTC_MOVE_EXIT         = 700    # $700 from strike
USE_DELTA_BREACH_EXIT = True   # set False to test TP/SL/hard-exit only
HARD_EXIT_HOUR   = 16
HARD_EXIT_MINUTE = 30
MIN_HOURS_TO_EXPIRY = 4.0   # don't enter if < 4 hrs left

# ── Cost model ────────────────────────────────────────────────────────────────
N_CONTRACTS            = 10     # contracts per trade
FEE_PER_ROUND_TRIP_USD = 0.16   # $0.08 entry + $0.08 exit (flat, for full 10-contract order)
SLIPPAGE_PCT           = 0.001  # 0.1% per fill (entry + exit)


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class TradeResult:
    date:             str
    symbol:           str
    strike:           float
    entry_time:       str
    exit_time:        str
    entry_price:      float
    exit_price:       float
    exit_reason:      str
    gross_pnl_pct:    float
    costs_pct:        float
    pnl_pct:          float   # net after fees + slippage
    mae_pct:          float   # max adverse excursion % (worst unrealised loss)
    mae_price:        float
    mfe_pct:          float   # max favourable excursion % (best unrealised profit)
    mfe_price:        float
    btc_at_entry:     float
    settlement_price: float   # actual payout at 5:30 PM (|BTC_settle - strike|)
    settlement_btc:   float   # BTC index price at settlement
    b_score:          str     # filter score label e.g. "3/4(FULL)"
    half_size:        bool    # True if B-score was 2/4 → half position


@dataclass
class BacktestSummary:
    trades:       list = field(default_factory=list)
    skipped_days: list = field(default_factory=list)   # [(date_str, reason)]


# ── Black-Scholes helpers (delta estimation only) ──────────────────────────────

def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _iv_from_straddle(price: float, spot: float, strike: float, hours: float) -> float:
    """Back-calculate IV % from actual straddle price (ATM approximation)."""
    if hours <= 0 or price <= 0 or spot <= 0:
        return 40.0
    T = hours / (365 * 24)
    denom = 2 * spot * math.sqrt(T / (2 * math.pi))
    return min((price / denom) * 100, 500.0) if denom > 0 else 40.0


def _bs_delta(spot: float, strike: float, iv_pct: float, hours: float) -> float:
    """Straddle delta = 2·N(d1) − 1."""
    if hours <= 0 or iv_pct <= 0 or spot <= 0:
        return 0.0
    T = hours / (365 * 24)
    sigma = iv_pct / 100
    d1 = (math.log(spot / strike) + 0.5 * sigma**2 * T) / (sigma * math.sqrt(T))
    return 2 * _norm_cdf(d1) - 1


def _bs_theta_hourly(spot: float, strike: float, iv_pct: float, hours: float) -> float:
    """Straddle theta in USD per hour (positive = decay per hour, seller's gain)."""
    if hours <= 0 or iv_pct <= 0 or spot <= 0:
        return 0.0
    T = hours / (365 * 24)
    sigma = iv_pct / 100
    d1 = (math.log(spot / strike) + 0.5 * sigma**2 * T) / (sigma * math.sqrt(T))
    nprime_d1 = math.exp(-d1 ** 2 / 2) / math.sqrt(2 * math.pi)
    theta_annual = spot * sigma * nprime_d1 / math.sqrt(T)   # straddle = call+put
    return theta_annual / (365 * 24)


def _bs_vega(spot: float, strike: float, iv_pct: float, hours: float) -> float:
    """Straddle vega: USD change per 1% increase in IV."""
    if hours <= 0 or iv_pct <= 0 or spot <= 0:
        return 0.0
    T = hours / (365 * 24)
    sigma = iv_pct / 100
    d1 = (math.log(spot / strike) + 0.5 * sigma**2 * T) / (sigma * math.sqrt(T))
    nprime_d1 = math.exp(-d1 ** 2 / 2) / math.sqrt(2 * math.pi)
    return 2 * spot * math.sqrt(T) * nprime_d1 / 100   # /100 → per 1% IV


def _calc_rv_24h(
    btc_5m: list[dict],
    before_ts: int,
    btc_hourly: list[dict] | None = None,
    btc_hourly_times: list[int] | None = None,
) -> float:
    """
    Realised volatility from the 24 hourly closes before before_ts.
    Uses precomputed hourly bars when available (O(log n) vs O(n)).
    Returns annualised RV as a percentage.
    """
    if btc_hourly is not None and btc_hourly_times is not None:
        hi     = bisect.bisect_left(btc_hourly_times, before_ts)
        hourly = btc_hourly[max(0, hi - 25):hi]
    else:
        hourly = [c for c in btc_5m if c["time"] < before_ts and c["time"] % 3600 == 0]
    if len(hourly) < 10:
        return 0.0
    closes = [c["close"] for c in hourly[-25:]]   # up to 25 for 24 log-returns
    log_returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    if not log_returns:
        return 0.0
    mean = sum(log_returns) / len(log_returns)
    var  = sum((r - mean) ** 2 for r in log_returns) / len(log_returns)
    return math.sqrt(var) * math.sqrt(365 * 24) * 100


def apply_entry_filters(
    noon_ts:     int,
    entry_price: float,
    btc_noon:    float,
    strike:      float,
    hours:       float,
    btc_5m:      list[dict],
    btc_by_ts:   dict,
    btc_times:   list[int] | None = None,   # precomputed for speed
    btc_hourly:  list[dict] | None = None,  # precomputed hourly bars for RV
    btc_hourly_times: list[int] | None = None,
) -> tuple[bool, str, str]:
    """
    Returns (skip, reason, score_str).

    APPLIED filters
    ───────────────
    A2 (hard gate)  BTC 4-hr move < $800       — from BTC 5-min candles
    A3 (hard gate)  BTC 24-hr range < $2,500   — from BTC 5-min candles
    B3 (ind. veto)  |delta| < 0.15             — BS from real straddle price
    B4 (ind. veto)  theta/price > 2.5 %/hr    — BS theta from real straddle price
    B1 (scoring)    IV < 55 %                  — back-calc IV from straddle price
    B2 (scoring)    IV–RV spread > -10         — RV from 24 hourly BTC closes
    B5 (scoring)    Vega < 18                  — BS vega from real straddle price
    B7 (scoring)    BTC 4-hr move < $400       — same source as A2

    NOT APPLIED (data unavailable)
    ───────────────────────────────
    A4  No macro events     — manual flag only, no API data
    A5  Covered by B3       — if ATM delta < 0.15, A5 implicitly passes
    B6  Volume > $1 M       — need full 24-hr straddle candles (only have noon–4:30)
    B8  Max Pain ± $2,000   — requires full historical options-chain OI per day

    Scoring rule (with B6/B8 absent):
      B3 + B4 pass = 2 pts guaranteed (they're vetoes, not scored separately)
      B1, B2, B5, B7 = 4 scoring checks
      Need ≥ 2/4 to trade half-size; ≥ 3/4 to trade full-size; < 2/4 → skip
      (Scaled from original 5/8 half / 6/8 full thresholds)
    """
    # ── A2: BTC 4-hr move — O(1) dict lookup (5-min grid) ────────────────────
    ts_4h  = noon_ts - 4 * 3600
    r4     = (ts_4h // 300) * 300   # snap to 5-min grid
    btc_4h = (btc_by_ts.get(r4) or btc_by_ts.get(r4 + 300) or
              btc_by_ts.get(r4 - 300) or btc_by_ts.get(r4 + 600) or
              btc_by_ts.get(r4 - 600))
    move_4h = abs(btc_noon - btc_4h) if btc_4h else 0.0
    if btc_4h and move_4h >= 800:
        return True, f"A2_fail 4hr_move=${move_4h:.0f}", ""

    # ── A3: BTC 24-hr range — O(log n) bisect ────────────────────────────────
    ts_24h = noon_ts - 24 * 3600
    _bt    = btc_times if btc_times is not None else [c["time"] for c in btc_5m]
    lo = bisect.bisect_left(_bt, ts_24h)
    hi = bisect.bisect_right(_bt, noon_ts)
    range_24h = 0.0
    if lo < hi:
        window    = btc_5m[lo:hi]
        range_24h = max(c["high"] for c in window) - min(c["low"] for c in window)
    if range_24h >= 2500:
        return True, f"A3_fail 24h_range=${range_24h:.0f}", ""

    # ── Greeks at entry ───────────────────────────────────────────────────────
    iv    = _iv_from_straddle(entry_price, btc_noon, strike, hours)
    delta = _bs_delta(btc_noon, strike, iv, hours)
    theta = _bs_theta_hourly(btc_noon, strike, iv, hours)
    vega  = _bs_vega(btc_noon, strike, iv, hours)
    rv    = _calc_rv_24h(btc_5m, noon_ts, btc_hourly, btc_hourly_times)
    theta_ratio = theta / entry_price * 100 if entry_price > 0 else 0

    # ── B3 veto: |delta| ──────────────────────────────────────────────────────
    if abs(delta) >= 0.15:
        return True, f"B3_veto |Δ|={abs(delta):.2f}", ""

    # ── B4 veto: theta/price ──────────────────────────────────────────────────
    if theta_ratio <= 2.5:
        return True, f"B4_veto θ/p={theta_ratio:.2f}%", ""

    # ── B-score (B1, B2, B5, B7) ─────────────────────────────────────────────
    b1 = 1 if iv < 55 else 0
    b2 = 1 if (rv > 0 and (iv - rv) > -10) else 0
    b5 = 1 if vega < 18 else 0
    b7 = 1 if move_4h < 400 else 0
    score = b1 + b2 + b5 + b7

    if score < 2:
        return True, f"B_score {score}/4 IV={iv:.0f}% RV={rv:.0f}% vega={vega:.1f}", ""

    size  = "FULL" if score >= 3 else "HALF"
    label = (f"{score}/4({size}) "
             f"IV={iv:.0f}% Δ={delta:+.2f} θ/p={theta_ratio:.1f}% "
             f"vega={vega:.1f} RV={rv:.0f}%")
    return False, "", label


# ── API helpers ────────────────────────────────────────────────────────────────

async def _get(client: httpx.AsyncClient, path: str, params: dict = None) -> dict:
    r = await client.get(path, params=params or {})
    r.raise_for_status()
    return r.json()


async def fetch_all_expired_straddles(client: httpx.AsyncClient) -> list[dict]:
    """
    Pages through /v2/products?states=expired&contract_types=move_options
    until all records are fetched. Filters to MV-BTC-* symbols only.
    """
    results = []
    after   = None
    page    = 0

    while True:
        params = {"contract_types": "move_options", "states": "expired", "page_size": 200}
        if after:
            params["after"] = after

        data     = await _get(client, "/v2/products", params)
        products = data.get("result", [])
        if not products:
            break

        btc = [p for p in products if p.get("symbol", "").startswith("MV-BTC-")]
        results.extend(btc)

        page += 1
        after = data.get("meta", {}).get("after")
        print(f"  Page {page:2d}: {len(products):3d} products, {len(btc):3d} BTC "
              f"(cumulative: {len(results)})")
        if not after:
            break

    return results


async def fetch_candles(
    client:     httpx.AsyncClient,
    symbol:     str,
    start:      int,
    end:        int,
    resolution: str = "5m",
) -> list[dict]:
    """
    Fetch OHLCV candles for any symbol (live or expired).
    Handles pagination (max 500 bars per request).
    Returns list sorted ascending by time.
    """
    resolution_secs = {
        "1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400,
    }
    secs   = resolution_secs.get(resolution, 300)
    window = secs * 500
    all_candles: list[dict] = []
    chunk_start = start

    while chunk_start < end:
        chunk_end = min(chunk_start + window, end)
        try:
            data = await _get(client, "/v2/history/candles", {
                "symbol": symbol, "resolution": resolution,
                "start": str(chunk_start), "end": str(chunk_end),
            })
            raw = data.get("result") or []
            for c in raw:
                if isinstance(c, dict):
                    all_candles.append({
                        "time":   int(c["time"]),
                        "open":   float(c["open"]),
                        "high":   float(c["high"]),
                        "low":    float(c["low"]),
                        "close":  float(c["close"]),
                        "volume": float(c.get("volume", 0)),
                    })
                elif isinstance(c, list) and len(c) >= 5:
                    all_candles.append({
                        "time":   int(c[0]),
                        "open":   float(c[1]),
                        "high":   float(c[2]),
                        "low":    float(c[3]),
                        "close":  float(c[4]),
                        "volume": float(c[5]) if len(c) > 5 else 0,
                    })
        except Exception:
            pass
        chunk_start = chunk_end

    all_candles.sort(key=lambda x: x["time"])
    seen: set[int] = set()
    deduped = []
    for c in all_candles:
        if c["time"] not in seen:
            seen.add(c["time"])
            deduped.append(c)
    return deduped


# ── Straddle simulation (multi-entry, full lifetime) ───────────────────────────

def simulate_day(
    meta:          dict,         # expired product record from /v2/products
    straddle_5m:   list[dict],   # 5-min candles for this straddle (full lifetime)
    btc_by_ts:     dict,         # {unix_ts: btc_close} from BTCUSD 5-min candles
    btc_5m:        list[dict],   # full sorted BTC 5-min candle list (for RV + range)
) -> tuple[list[TradeResult], str]:
    """
    Scans the entire straddle lifetime for entry conditions.
    Enters whenever all conditions pass and hours_to_expiry > MIN_HOURS_TO_EXPIRY.
    After each exit, immediately resumes scanning for the next entry.
    Returns (trades_list, skip_reason_if_empty).
    """
    symbol = meta["symbol"]
    parts  = symbol.split("-")
    try:
        strike = float(parts[2])
    except (IndexError, ValueError):
        return [], "bad symbol"

    settlement_price = float(meta.get("settlement_price") or 0)
    settlement_btc   = float((meta.get("product_specs") or {}).get("settlement_index_price") or 0)

    st_str = meta.get("settlement_time", "")
    try:
        dt_utc = datetime.fromisoformat(st_str.replace("Z", "+00:00"))
    except Exception:
        return [], "bad settlement_time"

    dt_ist        = dt_utc.astimezone(IST)
    hard_exit_ist = dt_ist.replace(hour=HARD_EXIT_HOUR, minute=HARD_EXIT_MINUTE,
                                   second=0, microsecond=0)
    hard_exit_ts  = int(hard_exit_ist.astimezone(timezone.utc).timestamp())
    expiry_ts     = int(dt_utc.timestamp())

    # Precompute once — passed to every apply_entry_filters call to avoid O(n) rebuilds
    btc_times        = [c["time"] for c in btc_5m]
    btc_hourly       = [c for c in btc_5m if c["time"] % 3600 == 0]
    btc_hourly_times = [c["time"] for c in btc_hourly]

    trades: list[TradeResult] = []
    last_skip_reason = "no bars"
    i = 0
    bars = straddle_5m

    while i < len(bars):
        bar    = bars[i]
        bar_ts = bar["time"]

        # Stop entering if < MIN_HOURS_TO_EXPIRY left or past hard exit
        hours_left = (expiry_ts - bar_ts) / 3600
        if hours_left < MIN_HOURS_TO_EXPIRY or bar_ts >= hard_exit_ts:
            break

        if bar["close"] <= 0:
            i += 1
            continue

        btc_bar_entry = btc_by_ts.get(bar_ts, 0)
        if btc_bar_entry == 0:
            i += 1
            continue

        skip, reason, b_score = apply_entry_filters(
            noon_ts          = bar_ts,
            entry_price      = bar["close"],
            btc_noon         = btc_bar_entry,
            strike           = strike,
            hours            = hours_left,
            btc_5m           = btc_5m,
            btc_by_ts        = btc_by_ts,
            btc_times        = btc_times,
            btc_hourly       = btc_hourly,
            btc_hourly_times = btc_hourly_times,
        )
        if skip:
            last_skip_reason = reason
            i += 1
            continue

        # ── ENTRY ─────────────────────────────────────────────────────────────
        entry_price    = bar["close"]
        entry_ist      = datetime.fromtimestamp(bar_ts, tz=IST)
        btc_at_entry   = btc_bar_entry
        half_size      = b_score.startswith("2/4")
        tp_target      = entry_price * (1 - TP_PCT)
        sl_target      = entry_price * SL_MULT
        max_adverse    = entry_price
        min_favourable = entry_price
        exit_price     = entry_price
        exit_time      = entry_ist.strftime("%H:%M")
        exit_reason    = "end_of_data"
        exit_idx       = i   # fallback: exit at entry bar

        # ── WALK FORWARD ──────────────────────────────────────────────────────
        j = i + 1
        while j < len(bars):
            eb     = bars[j]
            eb_ts  = eb["time"]
            eb_ist = datetime.fromtimestamp(eb_ts, tz=IST)

            if eb["high"] > max_adverse:
                max_adverse = eb["high"]
            if eb["low"] < min_favourable:
                min_favourable = eb["low"]

            # Hard exit
            if eb_ts >= hard_exit_ts:
                exit_price  = eb["close"]
                exit_time   = eb_ist.strftime("%H:%M")
                exit_reason = "hard_exit_4:30pm"
                exit_idx    = j
                break

            # TP
            if eb["low"] <= tp_target:
                exit_price  = tp_target
                exit_time   = eb_ist.strftime("%H:%M")
                exit_reason = f"TP ({TP_PCT*100:.0f}% decay)"
                exit_idx    = j
                break

            # SL
            if eb["high"] >= sl_target:
                exit_price  = sl_target
                exit_time   = eb_ist.strftime("%H:%M")
                exit_reason = f"SL ({(SL_MULT-1)*100:.0f}%)"
                exit_idx    = j
                break

            # Bounce exit — straddle rose >15% from entry (mirrors live bot)
            if eb["high"] > entry_price * 1.15:
                exit_price  = entry_price * 1.15
                exit_time   = eb_ist.strftime("%H:%M")
                exit_reason = "bounce_15pct"
                exit_idx    = j
                break

            btc_eb     = btc_by_ts.get(eb_ts, btc_at_entry)
            hours_exit = max((expiry_ts - eb_ts) / 3600, 0)

            if USE_DELTA_BREACH_EXIT and hours_exit > 0 and eb["close"] > 0 and btc_eb > 0:
                iv_est    = _iv_from_straddle(eb["close"], btc_eb, strike, hours_exit)
                delta_est = _bs_delta(btc_eb, strike, iv_est, hours_exit)
                if abs(delta_est) > DELTA_EXIT:
                    exit_price  = eb["close"]
                    exit_time   = eb_ist.strftime("%H:%M")
                    exit_reason = f"delta_breach Δ={delta_est:.2f}"
                    exit_idx    = j
                    break

            if btc_eb > 0 and abs(btc_eb - strike) > BTC_MOVE_EXIT:
                exit_price  = eb["close"]
                exit_time   = eb_ist.strftime("%H:%M")
                exit_reason = f"btc_move ${abs(btc_eb-strike):.0f} from strike"
                exit_idx    = j
                break

            j += 1

        # ── RECORD TRADE ──────────────────────────────────────────────────────
        position_value = entry_price * N_CONTRACTS
        fee_pct        = FEE_PER_ROUND_TRIP_USD / position_value * 100
        slip_pct       = SLIPPAGE_PCT * 2 * 100
        costs_pct      = fee_pct + slip_pct
        gross_pnl_pct  = (entry_price - exit_price) / entry_price * 100
        pnl_pct        = gross_pnl_pct - costs_pct
        mae_pct        = (max_adverse    - entry_price) / entry_price * 100
        mfe_pct        = (entry_price - min_favourable) / entry_price * 100
        size_mult      = 0.5 if half_size else 1.0

        trades.append(TradeResult(
            date             = entry_ist.strftime("%Y-%m-%d"),
            symbol           = symbol,
            strike           = strike,
            entry_time       = entry_ist.strftime("%H:%M"),
            exit_time        = exit_time,
            entry_price      = entry_price,
            exit_price       = exit_price,
            exit_reason      = exit_reason,
            gross_pnl_pct    = gross_pnl_pct * size_mult,
            costs_pct        = costs_pct * size_mult,
            pnl_pct          = pnl_pct * size_mult,
            mae_pct          = mae_pct,
            mae_price        = max_adverse,
            mfe_pct          = mfe_pct,
            mfe_price        = min_favourable,
            btc_at_entry     = btc_at_entry,
            settlement_price = settlement_price,
            settlement_btc   = settlement_btc,
            b_score          = b_score,
            half_size        = half_size,
        ))

        # Resume scanning from bar after exit
        i = exit_idx + 1

    if not trades:
        return [], f"no_entry ({last_skip_reason})"
    return trades, ""


# ── Report ─────────────────────────────────────────────────────────────────────

def print_report(summary: BacktestSummary, days_back: int):
    trades  = sorted(summary.trades, key=lambda t: t.date)
    skipped = summary.skipped_days
    n       = len(trades)

    print(f"\n{'='*68}")
    print(f"  REAL DATA RESULTS  ({days_back} calendar days)")
    print(f"{'='*68}")
    print(f"  Trades taken:        {n}")
    print(f"  Days skipped:        {len(skipped)}")

    # Filter summary
    filter_reasons: dict[str, int] = {}
    for _, r in skipped:
        key = r.split(" ")[0]   # first token e.g. "A2_fail", "B3_veto", "B_score"
        filter_reasons[key] = filter_reasons.get(key, 0) + 1
    print(f"\n  Filters applied:     A2(4hr<$800)  A3(24h<$2500)  B3(|Δ|<0.15)  "
          f"B4(θ/p>2.5%)  B1 B2 B5 B7(score≥2/4)")
    print(f"  Filters NOT applied: A4(macro)  B6(volume)  B8(max-pain) — no historical data")
    print(f"\n  Skip reasons:")
    for k, v in sorted(filter_reasons.items(), key=lambda x: -x[1]):
        print(f"    {k:<30} {v:3d}")

    if n == 0:
        print("  No trades after filtering.")
        return

    half  = [t for t in trades if t.half_size]
    full  = [t for t in trades if not t.half_size]
    wins      = [t for t in trades if t.pnl_pct > 0]
    losses    = [t for t in trades if t.pnl_pct <= 0]
    win_rate  = len(wins) / n * 100
    pnls      = [t.pnl_pct for t in trades]
    gross     = [t.gross_pnl_pct for t in trades]
    costs     = [t.costs_pct for t in trades]

    print(f"  Full-size trades:    {len(full)}  |  Half-size trades: {len(half)}")
    print(f"\n  Win rate:            {win_rate:.1f}%  ({len(wins)}W / {len(losses)}L)")
    print(f"  Avg gross P&L:       {sum(gross)/n:+.1f}%")
    print(f"  Avg costs:           -{sum(costs)/n:.1f}%  "
          f"(${FEE_PER_ROUND_TRIP_USD:.2f} r/t + {SLIPPAGE_PCT*100:.1f}% slip)")
    print(f"  Avg net P&L:         {sum(pnls)/n:+.1f}%")
    print(f"  Total return (sum):  {sum(pnls):+.1f}%")
    print(f"  Best trade:          {max(pnls):+.1f}%")
    print(f"  Worst trade:         {min(pnls):+.1f}%")

    avg_win  = sum(t.pnl_pct for t in wins)  / len(wins)  if wins   else 0
    avg_loss = sum(t.pnl_pct for t in losses) / len(losses) if losses else 0
    if wins and losses:
        expectancy = (win_rate/100 * avg_win) + ((1-win_rate/100) * avg_loss)
        print(f"  Avg winner:          {avg_win:+.1f}%")
        print(f"  Avg loser:           {avg_loss:+.1f}%")
        print(f"  Expectancy:          {expectancy:+.2f}% per trade")

    maes = [t.mae_pct for t in trades]
    print(f"\n  MAE (peak unrealised loss before exit):")
    print(f"  Max MAE:             +{max(maes):.1f}%")
    print(f"  Avg MAE:             +{sum(maes)/n:.1f}%")
    print(f"  Days MAE > 10%:      {sum(1 for m in maes if m > 10)}")
    print(f"  Days MAE > 20%:      {sum(1 for m in maes if m > 20)}")
    print(f"  Days MAE > 50%:      {sum(1 for m in maes if m > 50)}")
    worst_mae = max(trades, key=lambda t: t.mae_pct)
    print(f"  Worst MAE day:       {worst_mae.date}  +{worst_mae.mae_pct:.1f}%  "
          f"(final PnL {worst_mae.pnl_pct:+.1f}%)")

    # Exit breakdown
    reasons: dict[str, int] = {}
    for t in trades:
        reasons[t.exit_reason] = reasons.get(t.exit_reason, 0) + 1
    print(f"\n  Exit breakdown:")
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"    {reason:<38} {count:3d}  ({count/n*100:.0f}%)")

    # Skip breakdown
    skip_reasons: dict[str, int] = {}
    for _, r in skipped:
        skip_reasons[r] = skip_reasons.get(r, 0) + 1
    print(f"\n  Skip breakdown:")
    for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
        print(f"    {reason:<38} {count:3d}")

    def _stake_sim(label: str, contracts: int):
        """
        Dollar P&L simulation for a given number of contracts.

        Delta Exchange move options:
          mark_price  = USD per contract (the straddle's dollar cost)
          contract_size = 0.001 BTC (margin unit, not a price multiplier)

        P&L (USD) per trade = pnl_pct/100 × entry_price × contracts × 0.001 BTC
          e.g. for 10 contracts at $450 entry hitting 30% TP:
               0.30 × $450 × 10 × 0.001 = $1.35 USD profit

        Half-size trades already have pnl_pct halved (×0.5 contracts effective).
        """
        CONTRACT_BTC = 0.001
        dpnls = [t.pnl_pct / 100 * t.entry_price * contracts * CONTRACT_BTC for t in trades]
        cum   = []
        run   = 0.0
        for d in dpnls:
            run += d
            cum.append(run)
        # Proper rolling max-drawdown (peak-to-trough at every point in time)
        max_dd       = 0.0
        running_peak = cum[0]
        for val in cum:
            if val > running_peak:
                running_peak = val
            dd = running_peak - val
            if dd > max_dd:
                max_dd = dd
        peak = max(cum)
        print(f"\n  {label}:")
        print(f"  Total profit:        ${sum(dpnls):>+10,.0f}")
        print(f"  Avg profit/trade:    ${sum(dpnls)/n:>+10,.0f}")
        print(f"  Best trade:          ${max(dpnls):>+10,.0f}")
        print(f"  Worst trade:         ${min(dpnls):>+10,.0f}")
        print(f"  Peak balance:        ${peak:>+10,.0f}")
        print(f"  Max drawdown:        ${-max_dd:>+10,.2f}  (rolling peak-to-trough)")
        return dpnls, cum

    dollar_pnls, cumulative = _stake_sim(
        f"10-contract position  (0.01 BTC — your current size)", N_CONTRACTS)
    _stake_sim(
        f"1,000-contract position  (1 BTC)",                    1_000)
    _stake_sim(
        f"100-contract position  (0.1 BTC)",                      100)

    # ── Verification 1: Monthly P&L breakdown ─────────────────────────────────
    print(f"\n{'─'*68}")
    print(f"  MONTHLY BREAKDOWN (10-contract, 0.01 BTC)")
    print(f"{'─'*68}")
    CONTRACT_BTC = 0.001
    monthly: dict[str, list] = {}
    for t, dpnl in zip(trades, dollar_pnls):
        month = t.date[:7]   # "YYYY-MM"
        monthly.setdefault(month, []).append((t.pnl_pct, dpnl))
    for month in sorted(monthly.keys()):
        rows   = monthly[month]
        total  = sum(d for _, d in rows)
        wins   = sum(1 for p, _ in rows if p > 0)
        losses = len(rows) - wins
        print(f"  {month}  trades={len(rows):3d}  W/L={wins}/{losses}  "
              f"net=${total:>+7.2f}  {'✅' if total > 0 else '❌'}")

    # ── Verification 2: Daytime vs Overnight entry split ──────────────────────
    # Daytime = entry between 06:00 and 13:30 IST (liquid hours before 4hr cutoff)
    # Overnight = all other entry times
    print(f"\n{'─'*68}")
    print(f"  DAYTIME (06:00–13:30 IST) vs OVERNIGHT ENTRIES")
    print(f"{'─'*68}")
    day_trades   = [(t, d) for t, d in zip(trades, dollar_pnls) if "06:00" <= t.entry_time <= "13:30"]
    night_trades = [(t, d) for t, d in zip(trades, dollar_pnls) if not ("06:00" <= t.entry_time <= "13:30")]

    for label2, subset in [("Daytime ", day_trades), ("Overnight", night_trades)]:
        if not subset:
            print(f"  {label2}: no trades")
            continue
        ts, ds      = zip(*subset)
        total_d     = sum(ds)
        wr          = sum(1 for t in ts if t.pnl_pct > 0) / len(ts) * 100
        avg_entry   = sum(t.entry_price for t in ts) / len(ts)
        # rolling drawdown for this subset
        run2 = 0.0
        peak2 = 0.0
        mdd2 = 0.0
        for d in ds:
            run2 += d
            if run2 > peak2: peak2 = run2
            if (peak2 - run2) > mdd2: mdd2 = peak2 - run2
        print(f"  {label2}: {len(ts):3d} trades | WR={wr:.0f}% | "
              f"net=${total_d:>+7.2f} | MDD=${-mdd2:>+6.2f} | "
              f"avg_entry=${avg_entry:>6.0f}")

    # ── Verification 3: Re-run with delta breach OFF for comparison ───────────
    print(f"\n{'─'*68}")
    print(f"  SENSITIVITY: with vs without delta-breach exit (same trades, different exit logic)")
    print(f"{'─'*68}")
    print(f"  Current (delta breach ON):  {n} trades | WR={sum(1 for t in trades if t.pnl_pct>0)/n*100:.0f}% | "
          f"net=${sum(dollar_pnls):>+.2f}")
    delta_exits = sum(1 for t in trades if "delta_breach" in t.exit_reason)
    delta_exit_losses = sum(d for t, d in zip(trades, dollar_pnls) if "delta_breach" in t.exit_reason and d < 0)
    delta_exit_wins   = sum(d for t, d in zip(trades, dollar_pnls) if "delta_breach" in t.exit_reason and d > 0)
    print(f"  Delta-breach exits:         {delta_exits} ({delta_exits/n*100:.0f}% of trades) | "
          f"winners=${delta_exit_wins:>+.2f} | losers=${delta_exit_losses:>+.2f}")

    # ── Verification 4: Entry price distribution ──────────────────────────────
    print(f"\n{'─'*68}")
    print(f"  ENTRY PRICE DISTRIBUTION (straddle price when shorted)")
    print(f"{'─'*68}")
    buckets = {"<$300": 0, "$300–500": 0, "$500–800": 0, "$800–1200": 0, ">$1200": 0}
    for t in trades:
        p = t.entry_price
        if   p < 300:   buckets["<$300"] += 1
        elif p < 500:   buckets["$300–500"] += 1
        elif p < 800:   buckets["$500–800"] += 1
        elif p < 1200:  buckets["$800–1200"] += 1
        else:           buckets[">$1200"] += 1
    for bucket, count in buckets.items():
        bar = "█" * count
        print(f"  {bucket:<10}  {count:3d}  {bar}")
    print(f"  Avg entry price: ${sum(t.entry_price for t in trades)/n:.0f}  "
          f"(overnight straddles typically >$800 due to more time value)")

    # ── Detailed trade log (1 BTC = 1,000 contracts) ─────────────────────────
    C1BTC = 0.001 * 1000   # 1.0 BTC multiplier
    _, cum1btc = _stake_sim.__wrapped__(1_000) if hasattr(_stake_sim, '__wrapped__') else (None, None)
    # Recompute 1-BTC cumulative inline
    dpnls_1btc = [t.pnl_pct / 100 * t.entry_price * 1000 * 0.001 for t in trades]
    cum_1btc   = []
    _run = 0.0
    for d in dpnls_1btc:
        _run += d
        cum_1btc.append(_run)

    W = 175
    print(f"\n{'─'*W}")
    print(f"  {'#':<3} {'Date':<12} {'Symbol':<26} "
          f"{'Entry':>6} {'Exit':>6}  "
          f"{'BTC@Entry':>10}  "
          f"{'Straddle$':>9} {'Exit$':>7}  "
          f"{'MaxProfit$':>11} {'MaxLoss$':>10}  "
          f"{'Net%':>6} {'Net$1BTC':>9}  "
          f"{'Cumul$':>9}  "
          f"{'Score':<14} Reason")
    print(f"{'─'*W}")
    for i, (t, dpnl, cum) in enumerate(zip(trades, dpnls_1btc, cum_1btc), 1):
        flag = "✅" if t.pnl_pct > 0 else "❌"
        sz   = "½" if t.half_size else " "
        mfe_usd = t.mfe_pct / 100 * t.entry_price * C1BTC
        mae_usd = t.mae_pct / 100 * t.entry_price * C1BTC
        print(
            f"  {i:<3} {t.date:<12} {t.symbol:<26} "
            f"{t.entry_time:>6} {t.exit_time:>6}  "
            f"${t.btc_at_entry:>9,.0f}  "
            f"${t.entry_price:>8.0f} ${t.exit_price:>6.0f}  "
            f"+{t.mfe_pct:>5.1f}%(${mfe_usd:>7.0f})  "
            f"-{t.mae_pct:>5.1f}%(${mae_usd:>7.0f})  "
            f"{t.pnl_pct:>+6.1f}% ${dpnl:>+8.0f}  "
            f"${cum:>+9.0f}  "
            f"{t.b_score[:13]:<13}  {flag}{sz} {t.exit_reason}"
        )
    print(f"{'─'*W}")
    print(f"\n  Position: 1,000 contracts = 1 BTC notional  (each contract = 0.001 BTC)")
    print(f"  MaxProfit$ = best unrealised gain seen (straddle dropped this far from entry)")
    print(f"  MaxLoss$   = worst unrealised loss seen (straddle rose this far against you)")
    print(f"  Net$1BTC   = actual realised P&L in USD for 1-BTC position")
    print(f"  Score      = B-filter score at entry (FULL=3+/4, HALF=2/4)\n")


# ── Main runner ────────────────────────────────────────────────────────────────

async def run_backtest(days_back: int = 365):
    print(f"\n{'='*68}")
    print(f"  BTC Straddle Real-Data Backtest  (last {days_back} days)")
    print(f"  TP: {TP_PCT*100:.0f}%  |  SL: {(SL_MULT-1)*100:.0f}%  |  noon entry  |  real prices")
    print(f"{'='*68}\n")

    now_utc     = datetime.now(timezone.utc)
    cutoff_date = (now_utc - timedelta(days=days_back)).date()
    today_ist   = datetime.now(IST).date()

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:

        # ── 1. Fetch all expired BTC straddles ─────────────────────────────
        print("Step 1/4 — Fetching expired straddle list...")
        all_straddles = await fetch_all_expired_straddles(client)
        print(f"  Total expired BTC straddles fetched: {len(all_straddles)}\n")

        # Group by expiry date, filter to requested range
        by_date: dict = defaultdict(list)
        for s in all_straddles:
            st = s.get("settlement_time", "")
            try:
                dt_utc  = datetime.fromisoformat(st.replace("Z", "+00:00"))
                exp_date = dt_utc.astimezone(IST).date()
            except Exception:
                continue
            if cutoff_date <= exp_date < today_ist:
                by_date[exp_date].append(s)

        print(f"  Expiry dates in range: {len(by_date)}\n")

        # ── 2. Fetch BTC 5-min candles (one bulk request) ──────────────────
        print("Step 2/4 — Fetching BTC 5-min candles...")
        start_ts = int((now_utc - timedelta(days=days_back + 1)).timestamp())
        end_ts   = int(now_utc.timestamp())
        btc_5m   = await fetch_candles(client, "BTCUSD", start_ts, end_ts, "5m")
        btc_by_ts = {c["time"]: c["close"] for c in btc_5m}
        print(f"  {len(btc_5m)} BTC 5-min bars\n")

        # ── 3. Select ATM straddle per day ─────────────────────────────────
        print("Step 3/4 — Selecting ATM straddle per day + fetching intraday candles...")
        selections: dict = {}   # date -> straddle_meta
        for exp_date in sorted(by_date.keys()):
            candidates = by_date[exp_date]
            if not candidates:
                continue

            # BTC price at noon from 5-min candles (closest bar within ±10 min)
            day_ist  = IST.localize(datetime(exp_date.year, exp_date.month, exp_date.day, 12, 0))
            noon_ts  = int(day_ist.astimezone(timezone.utc).timestamp())
            btc_noon = 0.0
            best_gap = float("inf")
            for ts, price in btc_by_ts.items():
                gap = abs(ts - noon_ts)
                if gap < best_gap and gap <= 600:
                    best_gap = gap
                    btc_noon = price

            if btc_noon == 0:
                continue

            # Closest strike to BTC noon price
            try:
                atm = min(candidates, key=lambda s: abs(float(s.get("strike_price", 0) or 0) - btc_noon))
            except Exception:
                continue
            selections[exp_date] = (atm, btc_noon)

        print(f"  {len(selections)} days with valid ATM selection\n")

        # Fetch intraday straddle candles concurrently in batches
        async def _fetch_day(exp_date, meta, _btc_noon):
            st = meta.get("settlement_time", "")
            try:
                dt_utc = datetime.fromisoformat(st.replace("Z", "+00:00"))
            except Exception:
                return exp_date, meta, []
            dt_ist = dt_utc.astimezone(IST)
            # Straddle lists right after previous day's 5:30 PM IST expiry → fetch full 24h
            prev_expiry_ist = dt_ist.replace(hour=17, minute=30, second=0, microsecond=0) - timedelta(days=1)
            start   = int(prev_expiry_ist.astimezone(timezone.utc).timestamp())
            end     = int(dt_utc.timestamp())   # actual expiry time
            candles = await fetch_candles(client, meta["symbol"], start, end, "5m")
            return exp_date, meta, candles

        items       = list(selections.items())
        raw_results = []
        BATCH       = 20
        for i in range(0, len(items), BATCH):
            batch = items[i:i + BATCH]
            tasks = [_fetch_day(d, meta, btc) for d, (meta, btc) in batch]
            res   = await asyncio.gather(*tasks, return_exceptions=True)
            raw_results.extend(res)
            done = min(i + BATCH, len(items))
            print(f"  Fetched {done}/{len(items)} straddle candle sets...")
            if done < len(items):
                await asyncio.sleep(0.3)

        print()

        # ── 4. Simulate each day ───────────────────────────────────────────
        print("Step 4/4 — Simulating trades...")
        summary = BacktestSummary()

        for result in raw_results:
            if isinstance(result, Exception):
                continue
            exp_date, meta, straddle_5m = result
            if not straddle_5m:
                summary.skipped_days.append((str(exp_date), "no straddle candles"))
                continue
            day_trades, skip_reason = simulate_day(meta, straddle_5m, btc_by_ts, btc_5m)
            if day_trades:
                summary.trades.extend(day_trades)
            else:
                summary.skipped_days.append((str(exp_date), skip_reason))

        print(f"  {len(summary.trades)} trades  |  {len(summary.skipped_days)} skipped\n")

    print_report(summary, days_back)


if __name__ == "__main__":
    import sys
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 365
    asyncio.run(run_backtest(days_back=days))
