"""
analysis_engine.py
------------------
Encodes the complete pre-trade checklist and position monitoring logic
from the BTC Straddle Short playbook into deterministic Python functions.

All thresholds match the playbook exactly. Every decision is explainable.
"""

import math
import logging
import numpy as np
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ── Thresholds (mirroring the playbook) ─────────────────────────────────────

DELTA_MAX = 0.15          # Section B: |delta| < 0.15 for entry
DELTA_WARN = 0.30         # Yellow flag during monitoring
DELTA_EXIT = 0.45         # Exit trigger during monitoring

GAMMA_MAX_ENTRY = 0.00065  # Section B: gamma < 0.00065 at entry
GAMMA_WARN = 0.00120       # Watch zone
GAMMA_EXIT = 0.00180       # Elevated but acceptable if <5hrs to expiry

THETA_RATIO_MIN = 0.025    # Section B: theta/price > 2.5%/hr
THETA_RATIO_IDEAL = 0.030  # Ideal threshold

VEGA_MAX = 18.0            # Section B: vega < 18

IV_MAX = 55.0              # Section B: IV < 55%
IV_HIGH_RISK = 70.0        # Above this: high premium but dangerous

IV_RV_SELLER_STRONG = 15.0   # IV-RV > +15: ideal for short
IV_RV_SELLER_OK = 0.0        # IV-RV 0 to +15: acceptable
IV_RV_CAUTION = -10.0        # IV-RV 0 to -10: caution for short
IV_RV_AVOID = -10.0          # IV-RV < -10: avoid short, consider long

PCR_BEARISH = 1.2           # PCR > 1.2: put heavy, good for sellers
PCR_NEUTRAL_LOW = 0.8       # PCR 0.8-1.2: neutral
PCR_BULLISH = 0.8           # PCR < 0.8: call heavy, caution

MAX_PAIN_IDEAL_GAP = 500    # Within $500 of max pain = ideal pin
MAX_PAIN_WARN_GAP = 2000    # $500-$2000 = acceptable
MAX_PAIN_BAD_GAP = 2000     # > $2000 = bad

BTC_24H_RANGE_MAX = 2500    # Section A: 24h BTC range < $2,500
BTC_4H_MOVE_MAX = 800       # Section A: 4hr BTC move < $800

TP_TARGET = 0.50            # Take profit at 50% decay
TP_WEEKEND = 0.40           # Tighter TP on weekends
SL_MULTIPLIER = 1.70        # Stop loss at 170% of entry

STRADDLE_BOUNCE_EXIT = 0.15  # If straddle rises >15% from entry → exit
PARTIAL_PROFIT_DECAY = 0.35  # 35%+ decay after 3 PM → consider partial

HARD_EXIT_HOUR = 16         # 4 PM IST hard exit hour
HARD_EXIT_MINUTE = 30       # 4:30 PM IST hard exit

ENTRY_WINDOW_START = 11     # 11 AM IST
ENTRY_WINDOW_END = 13       # 1 PM IST (exclusive)


# ── Data Structures ──────────────────────────────────────────────────────────

@dataclass
class MarketSnapshot:
    """All raw data from one market scan."""
    timestamp: str
    btc_spot: float
    hours_to_expiry: float

    # Straddle chain
    straddles: list = field(default_factory=list)  # list of straddle dicts

    # Calculated from options chain
    pcr: float = 0.0
    max_pain: float = 0.0
    total_calls_oi: float = 0.0
    total_puts_oi: float = 0.0

    # Calculated from price history
    realised_vol: float = 0.0    # annualised %
    implied_vol: float = 0.0     # from ATM straddle (annualised %)
    iv_rv_spread: float = 0.0    # IV - RV

    # BTC range metrics
    btc_24h_range: float = 0.0
    btc_24h_high: float = 0.0
    btc_24h_low: float = 0.0
    btc_4h_move: float = 0.0     # absolute move in last 4 hours

    # Day type
    day_of_week: int = 0  # 0=Mon, 5=Sat, 6=Sun


@dataclass
class StraddleCandidate:
    """A specific straddle contract being evaluated for entry."""
    symbol: str
    strike: float
    price: float
    delta: float
    gamma: float
    theta: float
    vega: float
    iv: float
    volume_24h: float
    oi: float
    theta_per_hour: float = 0.0
    theta_ratio: float = 0.0   # theta/hr as % of price

    def __post_init__(self):
        if self.price > 0:
            self.theta_per_hour = abs(self.theta) / 24
            self.theta_ratio = self.theta_per_hour / self.price


@dataclass
class ChecklistResult:
    """Output of the full pre-trade checklist."""
    verdict: str = "PASS"       # "TRADE" | "WAIT" | "PASS"
    confidence: str = "LOW"     # "HIGH" | "MEDIUM" | "LOW"
    best_candidate: Optional[StraddleCandidate] = None

    # Gate Zero
    gate_zero_pass: bool = True
    gate_zero_reason: str = ""

    # Section A scores (5 checks)
    section_a_pass: int = 0
    section_a_total: int = 5
    section_a_details: list = field(default_factory=list)

    # Section B scores (8 checks)
    section_b_pass: int = 0
    section_b_total: int = 8
    section_b_details: list = field(default_factory=list)

    # Key numbers
    btc_spot: float = 0.0
    max_pain_gap: float = 0.0
    tp_target: float = 0.0
    sl_target: float = 0.0
    suggested_entry: float = 0.0
    hours_to_expiry: float = 0.0
    regime: str = ""  # "WEEKEND" | "WEEKDAY" | "CAUTION_FRIDAY"

    summary: str = ""


@dataclass
class MonitorAlert:
    """Output of the post-entry monitoring check."""
    action: str          # "HOLD" | "PARTIAL_PROFIT" | "EXIT" | "HARD_EXIT"
    urgency: str         # "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
    reason: str

    current_price: float = 0.0
    entry_price: float = 0.0
    pnl_pct: float = 0.0       # % gain/loss from entry
    decay_from_entry: float = 0.0  # how much straddle has decayed in %
    btc_spot: float = 0.0
    delta: float = 0.0
    theta_per_hour: float = 0.0
    hours_remaining: float = 0.0
    tp_target: float = 0.0
    sl_target: float = 0.0


# ── Core Calculations ────────────────────────────────────────────────────────

def calculate_pcr_and_max_pain(options_chain: list[dict]) -> tuple[float, float, float, float]:
    """
    From raw options chain, calculate:
    - PCR (Put/Call Ratio by OI value)
    - Max Pain strike
    - Total calls OI value
    - Total puts OI value

    Max Pain = strike at which sum of all option buyers' losses is maximised.
    i.e. the strike where market makers profit most = settlement gravitates here.
    """
    if not options_chain:
        return 0.0, 0.0, 0.0, 0.0

    calls = [o for o in options_chain if o["type"] == "call" and o["oi_value"] > 0]
    puts = [o for o in options_chain if o["type"] == "put" and o["oi_value"] > 0]

    total_calls_oi = sum(c["oi_value"] for c in calls)
    total_puts_oi = sum(p["oi_value"] for p in puts)

    pcr = total_puts_oi / total_calls_oi if total_calls_oi > 0 else 0.0

    # Max Pain calculation
    all_strikes = sorted(set(
        [o["strike"] for o in options_chain if o["strike"] > 0]
    ))

    if not all_strikes:
        return pcr, 0.0, total_calls_oi, total_puts_oi

    min_loss = float("inf")
    max_pain_strike = all_strikes[len(all_strikes) // 2]

    for test_strike in all_strikes:
        total_loss = 0.0

        # Call buyers lose: all calls with strike < test_strike expire worthless
        # i.e. for each call with strike S < test_strike, loss = OI * mark_price
        # Actually: call buyer loss at settlement S = max(0, S - strike) * OI
        # We approximate with OI value since we don't have full OI in contracts

        for c in calls:
            if c["strike"] < test_strike:
                # In the money call — call buyer wins, market maker loses
                # = intrinsic value * OI
                intrinsic = test_strike - c["strike"]
                total_loss += intrinsic * (c["oi_value"] / c["mark_price"] if c["mark_price"] > 0 else 0)

        for p in puts:
            if p["strike"] > test_strike:
                # In the money put — put buyer wins
                intrinsic = p["strike"] - test_strike
                total_loss += intrinsic * (p["oi_value"] / p["mark_price"] if p["mark_price"] > 0 else 0)

        if total_loss < min_loss:
            min_loss = total_loss
            max_pain_strike = test_strike

    return pcr, float(max_pain_strike), total_calls_oi, total_puts_oi


def calculate_realised_vol(candles: list[dict], window: int = 24) -> float:
    """
    Calculates annualised Realised Volatility from hourly OHLCV candles.
    Uses log returns, standard method for options RV calculation.
    Returns percentage (e.g. 45.2 for 45.2%).
    """
    if len(candles) < 2:
        return 0.0

    closes = [c["close"] for c in candles[-window:]]
    if len(closes) < 2:
        return 0.0

    log_returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    if not log_returns:
        return 0.0

    std_hourly = float(np.std(log_returns, ddof=1))
    # Annualise: hourly std * sqrt(365 * 24)
    rv_annualised = std_hourly * math.sqrt(365 * 24) * 100
    return rv_annualised


def calculate_implied_vol_from_straddle(
    straddle_price: float,
    spot: float,
    strike: float,
    hours_to_expiry: float,
) -> float:
    """
    Back-calculate ATM implied volatility from straddle price.
    Uses the simplified ATM straddle approximation:
    Straddle ≈ 2 * S * sigma * sqrt(T / (2*pi))
    where T is in years.

    Returns annualised IV as percentage.
    """
    if hours_to_expiry <= 0 or straddle_price <= 0 or spot <= 0:
        return 0.0

    T = hours_to_expiry / (365 * 24)  # time in years
    # Rearranged: sigma = Straddle / (2 * S * sqrt(T/(2*pi)))
    denominator = 2 * spot * math.sqrt(T / (2 * math.pi))
    if denominator <= 0:
        return 0.0

    iv = (straddle_price / denominator) * 100
    return min(iv, 500.0)  # Cap at 500% to avoid numerical insanity


def find_best_strike(
    straddles: list[dict],
    btc_spot: float,
    hours_to_expiry: float,
) -> Optional[StraddleCandidate]:
    """
    From the chain, find the best straddle to short.
    Priority: |delta| < 0.15, then highest theta ratio, then volume.
    Returns None if no suitable candidate found.
    """
    candidates = []

    for s in straddles:
        if s["mark_price"] <= 0:
            continue

        strike = s["strike"]
        price = s["mark_price"]
        greeks = s["greeks"]
        delta = abs(greeks.get("delta", 99))
        gamma = greeks.get("gamma", 99)
        theta = abs(greeks.get("theta", 0))
        vega = greeks.get("vega", 99)
        iv = s.get("iv", 0) * 100 if s.get("iv", 0) < 5 else s.get("iv", 0)

        # Must-pass filters
        if delta > DELTA_MAX:
            continue
        if gamma > GAMMA_MAX_ENTRY and hours_to_expiry > 6:
            continue
        if vega > VEGA_MAX:
            continue

        theta_per_hour = theta / 24
        theta_ratio = theta_per_hour / price if price > 0 else 0

        if theta_ratio < THETA_RATIO_MIN:
            continue

        candidate = StraddleCandidate(
            symbol=s["symbol"],
            strike=strike,
            price=price,
            delta=greeks.get("delta", 0),
            gamma=gamma,
            theta=-theta,  # keep negative convention
            vega=vega,
            iv=iv,
            volume_24h=s.get("volume_24h", 0),
            oi=s.get("oi", 0),
            theta_per_hour=theta_per_hour,
            theta_ratio=theta_ratio,
        )
        candidates.append(candidate)

    if not candidates:
        return None

    # Sort: highest theta ratio first, tie-break by volume
    candidates.sort(key=lambda x: (x.theta_ratio, x.volume_24h), reverse=True)
    return candidates[0]


# ── Pre-Trade Checklist ──────────────────────────────────────────────────────

def run_pretrade_checklist(snapshot: MarketSnapshot) -> ChecklistResult:
    """
    Runs the complete pre-trade checklist against a MarketSnapshot.
    Returns a ChecklistResult with TRADE / WAIT / PASS verdict.
    """
    result = ChecklistResult()
    result.btc_spot = snapshot.btc_spot
    result.hours_to_expiry = snapshot.hours_to_expiry
    result.max_pain_gap = abs(snapshot.max_pain - snapshot.btc_spot) if snapshot.max_pain > 0 else 99999

    # ── Determine day regime ─────────────────────────────────────────────────
    dow = snapshot.day_of_week  # 0=Mon, 5=Sat, 6=Sun
    if dow in (5, 6):
        result.regime = "WEEKEND"
        tp_pct = TP_WEEKEND
    elif dow == 4:  # Friday
        result.regime = "CAUTION_FRIDAY"
        tp_pct = 0.45
    else:
        result.regime = "WEEKDAY"
        tp_pct = TP_TARGET

    # ── Gate Zero ────────────────────────────────────────────────────────────
    # Macro events check is manual (not in API) — always passes in code,
    # user must set SKIP_TODAY=true env var or use /skip command
    result.gate_zero_pass = True
    result.gate_zero_reason = "No automated macro event detection — verify manually"

    # ── Section A (5 structural checks) ─────────────────────────────────────
    a_details = []

    # A1: Time to expiry 4-5.5 hours
    a1 = 4.0 <= snapshot.hours_to_expiry <= 5.5
    a_details.append(("A1: Entry window (4-5.5hrs)", a1,
                       f"{snapshot.hours_to_expiry:.1f}hrs to expiry"))

    # A2: BTC 4hr move < $800
    a2 = snapshot.btc_4h_move < BTC_4H_MOVE_MAX
    a_details.append(("A2: BTC 4hr move <$800", a2,
                       f"${snapshot.btc_4h_move:.0f} move in last 4hrs"))

    # A3: 24h BTC range < $2,500
    a3 = snapshot.btc_24h_range < BTC_24H_RANGE_MAX
    a_details.append(("A3: 24h BTC range <$2,500", a3,
                       f"${snapshot.btc_24h_range:.0f} range (H: ${snapshot.btc_24h_high:.0f} L: ${snapshot.btc_24h_low:.0f})"))

    # A4: No macro events (manual check, always pass in code)
    a4 = True
    a_details.append(("A4: No macro events", a4, "Manual check — verify"))

    # A5: Straddle chain check (at least one valid ATM with |delta| < 0.15)
    atm_straddle = find_best_strike(snapshot.straddles, snapshot.btc_spot, snapshot.hours_to_expiry)
    a5 = atm_straddle is not None
    a_details.append(("A5: Valid ATM strike available", a5,
                       f"Best: {atm_straddle.symbol} Δ={atm_straddle.delta:.2f}" if atm_straddle else "No strike with |delta| < 0.15"))

    result.section_a_pass = sum(1 for _, p, _ in a_details if p)
    result.section_a_total = len(a_details)
    result.section_a_details = a_details

    # If Section A fails hard (< 4 passes), immediate PASS
    hard_fails = [not a1, not a2, not a3]  # A1, A2, A3 are hard gates
    if any(hard_fails) or not a5:
        failures = []
        if not a1:
            failures.append(f"time window ({snapshot.hours_to_expiry:.1f}hrs)")
        if not a2:
            failures.append(f"4hr BTC move (${snapshot.btc_4h_move:.0f})")
        if not a3:
            failures.append(f"24h range (${snapshot.btc_24h_range:.0f})")
        if not a5:
            failures.append("no neutral delta strike")

        result.verdict = "PASS"
        result.confidence = "HIGH"
        result.summary = f"🚫 Section A hard fail: {', '.join(failures)}"
        return result

    # ── Section B (8 quality checks) ────────────────────────────────────────
    b_details = []
    c = atm_straddle

    # B1: IV < 55%
    b1 = c.iv < IV_MAX if c.iv > 0 else True  # pass if IV not available
    b_details.append(("B1: IV < 55%", b1, f"IV = {c.iv:.1f}%"))

    # B2: IV-RV spread
    b2 = snapshot.iv_rv_spread > IV_RV_CAUTION  # > -10 acceptable
    b_details.append(("B2: IV-RV spread > -10", b2,
                       f"IV-RV = {snapshot.iv_rv_spread:+.1f} (IV={snapshot.implied_vol:.0f}% RV={snapshot.realised_vol:.0f}%)"))

    # B3: |Delta| < 0.15
    b3 = abs(c.delta) < DELTA_MAX
    b_details.append(("B3: |Delta| < 0.15", b3, f"Δ = {c.delta:.3f}"))

    # B4: Theta/Price > 2.5%/hr
    b4 = c.theta_ratio > THETA_RATIO_MIN
    b_details.append(("B4: Theta/price > 2.5%/hr", b4,
                       f"{c.theta_ratio * 100:.2f}%/hr (${c.theta_per_hour:.1f}/hr on ${c.price:.0f})"))

    # B5: Vega < 18
    b5 = c.vega < VEGA_MAX
    b_details.append(("B5: Vega < 18", b5, f"Vega = {c.vega:.2f}"))

    # B6: Volume at target strike (want > $1M volume as liquidity signal)
    b6 = c.volume_24h > 1_000_000
    b_details.append(("B6: Adequate volume", b6,
                       f"${c.volume_24h / 1e6:.2f}M 24h vol"))

    # B7: BTC flat last 60min (approximated by 4hr move < $400 for this check)
    b7 = snapshot.btc_4h_move < 400
    b_details.append(("B7: BTC relatively flat", b7,
                       f"${snapshot.btc_4h_move:.0f} 4hr move"))

    # B8: Max Pain proximity (within $2,000)
    b8 = result.max_pain_gap < MAX_PAIN_BAD_GAP
    pain_dir = "above" if snapshot.max_pain > snapshot.btc_spot else "below"
    b_details.append(("B8: Max Pain within $2,000", b8,
                       f"Max Pain ${snapshot.max_pain:.0f} (${result.max_pain_gap:.0f} {pain_dir} BTC)"))

    result.section_b_pass = sum(1 for _, p, _ in b_details if p)
    result.section_b_total = len(b_details)
    result.section_b_details = b_details

    # ── Final Verdict ────────────────────────────────────────────────────────
    # B3 and B4 are non-negotiable
    if not b3:
        result.verdict = "PASS"
        result.confidence = "HIGH"
        result.summary = f"🚫 Delta too high ({c.delta:.2f}) — no neutral entry available"
        return result

    if not b4:
        result.verdict = "PASS"
        result.confidence = "MEDIUM"
        result.summary = f"🚫 Theta ratio too low ({c.theta_ratio * 100:.2f}%/hr) — not enough decay"
        return result

    # B8 (max pain) is a strong signal
    if not b8:
        result.verdict = "PASS"
        result.confidence = "HIGH"
        result.summary = (
            f"🚫 Max pain gap too wide (${result.max_pain_gap:,.0f}) — "
            f"no pin gravity at current BTC ${snapshot.btc_spot:,.0f}"
        )
        return result

    # Scoring
    if result.section_b_pass >= 6:
        result.verdict = "TRADE"
        result.confidence = "HIGH" if result.section_b_pass >= 7 else "MEDIUM"
    elif result.section_b_pass == 5:
        result.verdict = "TRADE"
        result.confidence = "LOW"  # half size
    else:
        result.verdict = "WAIT"
        result.confidence = "MEDIUM"

    # Set entry parameters
    if atm_straddle:
        result.best_candidate = atm_straddle
        result.suggested_entry = atm_straddle.price
        result.tp_target = round(atm_straddle.price * (1 - tp_pct))
        result.sl_target = round(atm_straddle.price * SL_MULTIPLIER)

    # Summary
    emoji = "✅" if result.verdict == "TRADE" else "⏳"
    size_note = " — HALF SIZE" if result.section_b_pass == 5 else ""
    result.summary = (
        f"{emoji} {result.verdict} [{result.confidence}]{size_note}\n"
        f"Section B: {result.section_b_pass}/8 | {result.regime}\n"
        f"Strike: {atm_straddle.symbol if atm_straddle else 'N/A'}"
    )

    return result


# ── Position Monitor ─────────────────────────────────────────────────────────

def monitor_position(
    entry_price: float,
    current_price: float,
    entry_symbol: str,
    btc_spot: float,
    btc_strike: float,
    delta: float,
    theta: float,
    hours_remaining: float,
    tp_target: float,
    sl_target: float,
    is_weekend: bool = False,
) -> MonitorAlert:
    """
    Evaluates an open short straddle position and returns a monitoring alert.
    All thresholds mirror the playbook exit rules.
    """
    decay_from_entry = (entry_price - current_price) / entry_price  # positive = profit
    pnl_pct = decay_from_entry * 100
    theta_per_hour = abs(theta) / 24
    btc_distance_from_strike = abs(btc_spot - btc_strike)

    # ── Critical Exits ───────────────────────────────────────────────────────

    # Hard time exit — 4:15 PM warning, 4:30 PM absolute
    from datetime import datetime
    import pytz
    ist = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist)
    minutes_to_hard_exit = (HARD_EXIT_HOUR * 60 + HARD_EXIT_MINUTE) - (now_ist.hour * 60 + now_ist.minute)

    if minutes_to_hard_exit <= 0:
        return MonitorAlert(
            action="HARD_EXIT",
            urgency="CRITICAL",
            reason="⏰ 4:30 PM IST — MANDATORY HARD EXIT. Close at market NOW.",
            current_price=current_price,
            entry_price=entry_price,
            pnl_pct=pnl_pct,
            decay_from_entry=decay_from_entry,
            btc_spot=btc_spot,
            delta=delta,
            theta_per_hour=theta_per_hour,
            hours_remaining=hours_remaining,
            tp_target=tp_target,
            sl_target=sl_target,
        )

    if minutes_to_hard_exit <= 15:
        return MonitorAlert(
            action="HARD_EXIT",
            urgency="CRITICAL",
            reason=f"⏰ {minutes_to_hard_exit}min to 4:30 PM hard exit. Prepare to close.",
            current_price=current_price,
            entry_price=entry_price,
            pnl_pct=pnl_pct,
            decay_from_entry=decay_from_entry,
            btc_spot=btc_spot,
            delta=delta,
            theta_per_hour=theta_per_hour,
            hours_remaining=hours_remaining,
            tp_target=tp_target,
            sl_target=sl_target,
        )

    # SL hit
    if current_price >= sl_target:
        return MonitorAlert(
            action="EXIT",
            urgency="CRITICAL",
            reason=f"🛑 SL HIT: ${current_price:.0f} ≥ ${sl_target:.0f}. Exit immediately.",
            current_price=current_price,
            entry_price=entry_price,
            pnl_pct=pnl_pct,
            decay_from_entry=decay_from_entry,
            btc_spot=btc_spot,
            delta=delta,
            theta_per_hour=theta_per_hour,
            hours_remaining=hours_remaining,
            tp_target=tp_target,
            sl_target=sl_target,
        )

    # TP hit
    if current_price <= tp_target:
        return MonitorAlert(
            action="EXIT",
            urgency="HIGH",
            reason=f"🎯 TP HIT: ${current_price:.0f} ≤ ${tp_target:.0f} — Take profit. Close now.",
            current_price=current_price,
            entry_price=entry_price,
            pnl_pct=pnl_pct,
            decay_from_entry=decay_from_entry,
            btc_spot=btc_spot,
            delta=delta,
            theta_per_hour=theta_per_hour,
            hours_remaining=hours_remaining,
            tp_target=tp_target,
            sl_target=sl_target,
        )

    # Delta breach
    if abs(delta) > DELTA_EXIT:
        return MonitorAlert(
            action="EXIT",
            urgency="HIGH",
            reason=f"⚡ Delta breach: |Δ| = {abs(delta):.2f} > 0.45. BTC has moved far from strike.",
            current_price=current_price,
            entry_price=entry_price,
            pnl_pct=pnl_pct,
            decay_from_entry=decay_from_entry,
            btc_spot=btc_spot,
            delta=delta,
            theta_per_hour=theta_per_hour,
            hours_remaining=hours_remaining,
            tp_target=tp_target,
            sl_target=sl_target,
        )

    # BTC moved >$700 from strike
    if btc_distance_from_strike > 700:
        return MonitorAlert(
            action="EXIT",
            urgency="HIGH",
            reason=f"⚡ BTC ${btc_spot:,.0f} moved ${btc_distance_from_strike:,.0f} from strike ${btc_strike:,.0f}. Breakout.",
            current_price=current_price,
            entry_price=entry_price,
            pnl_pct=pnl_pct,
            decay_from_entry=decay_from_entry,
            btc_spot=btc_spot,
            delta=delta,
            theta_per_hour=theta_per_hour,
            hours_remaining=hours_remaining,
            tp_target=tp_target,
            sl_target=sl_target,
        )

    # Straddle bounced >15% from entry
    if current_price > entry_price * (1 + STRADDLE_BOUNCE_EXIT):
        return MonitorAlert(
            action="EXIT",
            urgency="HIGH",
            reason=f"⚠️ Straddle up {pnl_pct:.1f}% from entry. Position moving against you.",
            current_price=current_price,
            entry_price=entry_price,
            pnl_pct=pnl_pct,
            decay_from_entry=decay_from_entry,
            btc_spot=btc_spot,
            delta=delta,
            theta_per_hour=theta_per_hour,
            hours_remaining=hours_remaining,
            tp_target=tp_target,
            sl_target=sl_target,
        )

    # ── Opportunistic Partial Profit ─────────────────────────────────────────
    if decay_from_entry > PARTIAL_PROFIT_DECAY and now_ist.hour >= 15:
        return MonitorAlert(
            action="PARTIAL_PROFIT",
            urgency="LOW",
            reason=f"💰 {pnl_pct:.1f}% decay after 3 PM. Consider closing 50% to lock gains.",
            current_price=current_price,
            entry_price=entry_price,
            pnl_pct=pnl_pct,
            decay_from_entry=decay_from_entry,
            btc_spot=btc_spot,
            delta=delta,
            theta_per_hour=theta_per_hour,
            hours_remaining=hours_remaining,
            tp_target=tp_target,
            sl_target=sl_target,
        )

    # ── Hold ─────────────────────────────────────────────────────────────────
    delta_status = "✅" if abs(delta) < DELTA_WARN else "⚠️"
    return MonitorAlert(
        action="HOLD",
        urgency="LOW",
        reason=(
            f"📊 {pnl_pct:.1f}% decay | {delta_status} Δ={delta:.2f} | "
            f"θ=${theta_per_hour:.1f}/hr | {hours_remaining:.1f}hrs left | "
            f"TP ${tp_target:.0f} | SL ${sl_target:.0f}"
        ),
        current_price=current_price,
        entry_price=entry_price,
        pnl_pct=pnl_pct,
        decay_from_entry=decay_from_entry,
        btc_spot=btc_spot,
        delta=delta,
        theta_per_hour=theta_per_hour,
        hours_remaining=hours_remaining,
        tp_target=tp_target,
        sl_target=sl_target,
    )