"""
formatter.py
------------
Converts analysis engine outputs into clean Telegram messages.
Uses Telegram MarkdownV2 formatting.
All dynamic values go through _escape() or _ef() to prevent parse errors.
"""

from datetime import datetime
import pytz
from analysis_engine import ChecklistResult, MonitorAlert, MarketSnapshot

IST = pytz.timezone("Asia/Kolkata")


def _now_ist_str() -> str:
    return datetime.now(IST).strftime("%I:%M %p IST")


def _escape(text: str) -> str:
    """Escape special chars for Telegram MarkdownV2."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in special else c for c in str(text))


def _ef(val: float, fmt: str = ".1f") -> str:
    """Format a float then escape it. Use for ALL numeric values in messages."""
    return _escape(format(val, fmt))


def format_hourly_snapshot(snapshot: MarketSnapshot) -> str:
    now = _now_ist_str()
    hours = snapshot.hours_to_expiry

    atm = None
    if snapshot.straddles:
        btc = snapshot.btc_spot
        nearest = min(snapshot.straddles, key=lambda s: abs(s["strike"] - btc))
        atm = nearest

    iv_rv_sign = "+" if snapshot.iv_rv_spread >= 0 else ""
    iv_rv_label = "seller edge" if snapshot.iv_rv_spread > 0 else "buyer edge"
    pcr_label = "put heavy ✅" if snapshot.pcr > 1.2 else "call heavy ⚠️" if snapshot.pcr < 0.8 else "neutral"

    lines = [
        f"📡 *Hourly Scan* — {_escape(now)}",
        "",
        f"🪙 BTC: *${snapshot.btc_spot:,.0f}*",
        f"⏳ To expiry: *{_ef(hours)}hrs*",
        f"📊 24h range: ${snapshot.btc_24h_range:,.0f}  "
        f"\\(H: ${snapshot.btc_24h_high:,.0f}  L: ${snapshot.btc_24h_low:,.0f}\\)",
        "",
        f"📈 IV: *{_ef(snapshot.implied_vol, '.0f')}%*  RV: *{_ef(snapshot.realised_vol, '.0f')}%*",
        f"📉 IV\\-RV: *{_escape(iv_rv_sign + format(snapshot.iv_rv_spread, '.1f'))}pp*  "
        f"\\({iv_rv_label}\\)",
        "",
        f"🎯 Max Pain: *${snapshot.max_pain:,.0f}*  "
        f"\\(gap: ${abs(snapshot.max_pain - snapshot.btc_spot):,.0f}\\)",
        f"📊 PCR: *{_ef(snapshot.pcr, '.2f')}*  \\({pcr_label}\\)",
    ]

    if atm:
        g = atm.get("greeks", {})
        theta_hr = abs(g.get("theta", 0)) / 24
        delta_val = g.get("delta", 0)
        lines += [
            "",
            f"🔑 ATM: *{_escape(atm['symbol'])}*",
            f"   Price: ${atm['mark_price']:,.0f}  \\|  Δ\\={_ef(delta_val, '.2f')}  "
            f"\\|  θ\\=${_ef(theta_hr, '.1f')}/hr",
        ]

    if 0 < hours < 6:
        lines += ["", "⚡ Entry window opens soon — next scan at 11 AM"]
    elif hours < 10:
        lines.append("\n🔔 Entry window opens at 11 AM IST")

    return "\n".join(lines)


def format_pretrade_report(result: ChecklistResult, snapshot: MarketSnapshot) -> str:
    now = _now_ist_str()
    verdict_emoji = {"TRADE": "✅", "WAIT": "⏳", "PASS": "🚫"}.get(result.verdict, "❓")
    confidence_emoji = {"HIGH": "🔥", "MEDIUM": "👍", "LOW": "⚠️"}.get(result.confidence, "")
    iv_rv_sign = "+" if snapshot.iv_rv_spread >= 0 else ""

    lines = [
        f"🎯 *Entry Window Scan* — {_escape(now)}",
        "",
        f"{verdict_emoji} *{result.verdict}* {confidence_emoji} \\[{result.confidence}\\]",
        "",
        f"🪙 BTC: *${snapshot.btc_spot:,.0f}*  \\|  ⏳ {_ef(result.hours_to_expiry)}hrs to expiry",
        f"📊 Regime: *{_escape(result.regime)}*",
        "",
        f"━━━ *Section A* {result.section_a_pass}/{result.section_a_total} ━━━",
    ]

    for name, passed, detail in result.section_a_details:
        icon = "✅" if passed else "❌"
        lines.append(f"{icon} {_escape(name)}")
        lines.append(f"   `{_escape(detail)}`")

    lines += ["", f"━━━ *Section B* {result.section_b_pass}/{result.section_b_total} ━━━"]

    for name, passed, detail in result.section_b_details:
        icon = "✅" if passed else "❌"
        lines.append(f"{icon} {_escape(name)}")
        lines.append(f"   `{_escape(detail)}`")

    iv_rv_label = (
        "🟢 Seller edge" if snapshot.iv_rv_spread > 15 else
        "🟡 Marginal seller edge" if snapshot.iv_rv_spread > 0 else
        "🟠 Buyer edge — caution" if snapshot.iv_rv_spread > -10 else
        "🔴 Strong buyer edge — avoid short"
    )
    lines += [
        "",
        "━━━ *Analytics* ━━━",
        f"📊 IV\\-RV: *{_escape(iv_rv_sign + format(snapshot.iv_rv_spread, '.1f'))}pp* — {_escape(iv_rv_label)}",
        f"🎯 Max Pain: *${snapshot.max_pain:,.0f}*  gap: ${abs(snapshot.max_pain - snapshot.btc_spot):,.0f}",
        f"📊 PCR: *{_ef(snapshot.pcr, '.2f')}*",
    ]

    if result.verdict == "TRADE" and result.best_candidate:
        c = result.best_candidate
        size_note = " \\(HALF SIZE\\)" if result.section_b_pass == 5 else ""
        lines += [
            "",
            f"━━━ *TRADE PARAMETERS*{size_note} ━━━",
            f"📋 Contract: `{_escape(c.symbol)}`",
            f"💰 Entry \\(limit\\): *${c.price:,.0f}*",
            f"🎯 TP trigger: *${result.tp_target:,.0f}*",
            f"🛑 SL trigger: *${result.sl_target:,.0f}*",
            "⏰ Hard exit: *4:30 PM IST*",
            "",
            f"Δ\\={_ef(c.delta, '.3f')}  γ\\={_ef(c.gamma, '.5f')}  "
            f"θ\\=${_ef(c.theta_per_hour, '.1f')}/hr  ν\\={_ef(c.vega, '.2f')}",
            f"θ/price ratio: *{_ef(c.theta_ratio * 100, '.2f')}%/hr*",
        ]
    elif result.verdict == "WAIT":
        lines += [
            "",
            "⏳ *Conditions not ideal yet* — rescan in 15 minutes",
            f"Key issues: {_escape(result.summary.split(chr(10))[0])}",
        ]
    else:
        lines += [
            "",
            f"🚫 *Skip today* — {_escape(result.summary)}",
        ]

    return "\n".join(lines)


def format_monitor_alert(alert: MonitorAlert, entry_symbol: str, strike: float) -> str:
    now = _now_ist_str()
    action_emoji = {
        "HOLD": "📊", "PARTIAL_PROFIT": "💰", "EXIT": "🚨", "HARD_EXIT": "🚨",
    }.get(alert.action, "❓")
    urgency_emoji = {
        "LOW": "", "MEDIUM": "⚠️", "HIGH": "🔴", "CRITICAL": "🆘",
    }.get(alert.urgency, "")

    pnl_emoji = "📈" if alert.pnl_pct > 0 else "📉"
    pnl_sign = "+" if alert.pnl_pct > 0 else ""
    pnl_display = _escape(f"{pnl_sign}{alert.pnl_pct:.1f}%")

    if alert.entry_price > 0 and alert.tp_target > 0:
        total_needed = alert.entry_price - alert.tp_target
        achieved = alert.entry_price - alert.current_price
        progress = min(max(achieved / total_needed, 0), 1) if total_needed > 0 else 0
        bar_filled = int(progress * 10)
        bar = "█" * bar_filled + "░" * (10 - bar_filled)
        progress_pct = progress * 100
    else:
        bar = "░" * 10
        progress_pct = 0

    lines = [
        f"{action_emoji} *Position Monitor* {urgency_emoji} — {_escape(now)}",
        "",
        f"📋 `{_escape(entry_symbol)}` \\| strike ${strike:,.0f}",
        "",
        f"{pnl_emoji} PnL: *{pnl_display}*  \\|  Decay: ${alert.entry_price - alert.current_price:,.0f}",
        f"💲 Entry: ${alert.entry_price:,.0f}  →  Now: *${alert.current_price:,.0f}*",
        "",
        f"Progress to TP:  `{bar}` {_ef(progress_pct, '.0f')}%",
        f"🎯 TP: ${alert.tp_target:,.0f}  \\|  🛑 SL: ${alert.sl_target:,.0f}",
        "",
        f"🪙 BTC: ${alert.btc_spot:,.0f}  \\|  Δ\\={_ef(alert.delta, '.2f')}",
        f"θ: ${_ef(alert.theta_per_hour, '.1f')}/hr  \\|  {_ef(alert.hours_remaining, '.1f')}hrs left",
        "",
        "━━━ *ACTION* ━━━",
        f"*{_escape(alert.action)}* — {_escape(alert.reason)}",
    ]

    return "\n".join(lines)


def format_skip_notification(reason: str) -> str:
    now = _now_ist_str()
    return (
        f"🚫 *Skip Day* — {_escape(now)}\n"
        f"\n"
        f"No trading today: {_escape(reason)}\n"
        f"Next assessment tomorrow at 11 AM IST\\."
    )


def format_startup_message() -> str:
    now = _now_ist_str()
    return (
        f"🤖 *Straddle Bot Online* — {_escape(now)}\n"
        f"\n"
        f"Monitoring: BTC Daily Straddles on Delta Exchange India\n"
        f"Schedule:\n"
        f"• Hourly scan: all day\n"
        f"• Entry window: 11 AM – 1 PM IST \\(every 15 min\\)\n"
        f"• Post\\-entry monitor: every 10 min\n"
        f"\n"
        f"Commands:\n"
        f"`/status` — current market snapshot\n"
        f"`/entry PRICE SYMBOL` — log entry \\(e\\.g\\. `/entry 601 MV\\-BTC\\-70600\\-200326`\\)\n"
        f"`/exit` — clear active position\n"
        f"`/skip REASON` — skip today's trading\n"
        f"`/resume` — re\\-enable after skip\n"
        f"`/help` — show this message"
    )


def format_error(context: str, error: str) -> str:
    return (
        f"⚠️ *Bot Error*\n"
        f"Context: {_escape(context)}\n"
        f"Error: `{_escape(str(error)[:200])}`\n"
        f"Bot continues running\\."
    )