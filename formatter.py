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


def format_hourly_snapshot(snapshot: MarketSnapshot, checklist_result=None) -> str:
    """
    checklist_result: optional ChecklistResult — adds A/B score line at bottom.
    Pass it from job_hourly_scan so the message is searchable by verdict.
    """
    now = _now_ist_str()
    hours = snapshot.hours_to_expiry

    iv_rv_sign = "+" if snapshot.iv_rv_spread >= 0 else ""
    iv_rv_label = "seller edge" if snapshot.iv_rv_spread > 0 else "buyer edge"
    pcr_label = "put heavy ✅" if snapshot.pcr > 1.2 else "call heavy ⚠️" if snapshot.pcr < 0.8 else "neutral"

    iv_str = f"*{_ef(snapshot.implied_vol, '.1f')}%*" if snapshot.implied_vol else "*N/A*"
    max_pain_str = (
        f"*${snapshot.max_pain:,.0f}*  \\(gap: ${abs(snapshot.max_pain - snapshot.btc_spot):,.0f}\\)"
        if snapshot.max_pain else "*N/A* \\(options chain not yet listed\\)"
    )
    pcr_str = f"*{_ef(snapshot.pcr, '.2f')}*  \\({pcr_label}\\)" if snapshot.pcr else "*N/A*"

    lines = [
        f"📡 *Hourly Scan* — {_escape(now)}",
        "",
        f"🪙 BTC: *${snapshot.btc_spot:,.0f}*  "
        f"\\|  4h move: *${snapshot.btc_4h_move:,.0f}*",
        f"⏳ To expiry: *{_ef(hours)}hrs*",
        f"📊 24h range: ${snapshot.btc_24h_range:,.0f}  "
        f"\\(H: ${snapshot.btc_24h_high:,.0f}  L: ${snapshot.btc_24h_low:,.0f}\\)",
        "",
        f"📈 IV: {iv_str}  \\|  RV \\(30d\\): *{_ef(snapshot.realised_vol, '.1f')}%*",
        f"📉 IV\\-RV spread: *{_escape(iv_rv_sign + format(snapshot.iv_rv_spread, '.1f'))}pp*  — {_escape(iv_rv_label)}",
        "",
        f"🎯 Max Pain: {max_pain_str}",
        f"📊 PCR: {pcr_str}",
    ]

    # ── Straddle chain: top 3 by |delta| (most neutral first) ────────────────
    best_symbol = checklist_result.best_candidate.symbol if (
        checklist_result and checklist_result.best_candidate
    ) else None

    if snapshot.straddles:
        # Sort by |delta| ascending so most neutral comes first
        chain = sorted(
            [s for s in snapshot.straddles if s["mark_price"] > 0],
            key=lambda s: abs(s["greeks"].get("delta", 99)),
        )[:3]

        lines += ["", "🔗 *Straddle Chain* \\(top 3 by \\|Δ\\|\\):"]
        for s in chain:
            g = s["greeks"]
            delta_val = g.get("delta", 0)
            theta_hr = abs(g.get("theta", 0)) / 24
            theta_ratio = (theta_hr / s["mark_price"] * 100) if s["mark_price"] > 0 else 0
            gamma_val = g.get("gamma", 0)
            vega_val = g.get("vega", 0)
            iv_val = s.get("iv", 0)
            iv_pct = iv_val * 100 if iv_val < 5 else iv_val
            vol = s.get("volume_24h", 0)
            marker = "  ← *bot pick*" if s["symbol"] == best_symbol else ""

            lines += [
                f"",
                f"  `{_escape(s['symbol'])}`{marker}",
                f"  Strike ${s['strike']:,.0f}  \\|  Price *${s['mark_price']:,.0f}*  \\|  IV {_ef(iv_pct, '.1f')}%",
                f"  Δ\\={_ef(delta_val, '.3f')}  γ\\={_ef(gamma_val, '.6f')}  "
                f"θ\\=${_ef(theta_hr, '.2f')}/hr  ν\\={_ef(vega_val, '.2f')}",
                f"  Vol ${vol:,.0f}",
            ]

    # ── Checklist score summary ───────────────────────────────────────────────
    if checklist_result:
        r = checklist_result
        verdict_emoji = {"TRADE": "✅", "WAIT": "⏳", "PASS": "🚫"}.get(r.verdict, "❓")
        risk_emoji = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴"}.get(r.risk_label, "⚪")
        lines += [
            "",
            f"📋 Checklist: A {r.section_a_pass}/{r.section_a_total}  \\|  "
            f"B {r.section_b_pass}/{r.section_b_total}  →  "
            f"*{r.verdict}* {verdict_emoji}  \\|  "
            f"Risk: {risk_emoji} *{_escape(r.risk_label)}* \\({r.risk_score}\\)",
        ]

    if 0 < hours < 6:
        lines += ["", "⚡ Entry window opens soon — next scan at 8:30 AM"]
    elif hours < 14:
        lines.append("\n🔔 Entry window opens at 8:30 AM IST")

    return "\n".join(lines)


def format_pretrade_report(result: ChecklistResult, snapshot: MarketSnapshot) -> str:
    now = _now_ist_str()
    verdict_emoji = {"TRADE": "✅", "WAIT": "⏳", "PASS": "🚫"}.get(result.verdict, "❓")
    # Only show confidence emoji for TRADE — PASS/WAIT don't get 🔥
    confidence_emoji = {"HIGH": "🔥", "MEDIUM": "👍", "LOW": "⚠️"}.get(result.confidence, "") if result.verdict == "TRADE" else ""
    iv_rv_sign = "+" if snapshot.iv_rv_spread >= 0 else ""

    lines = [
        f"🎯 *Entry Window Scan* — {_escape(now)}",
        "",
        f"{verdict_emoji} *{result.verdict}*{' ' + confidence_emoji if confidence_emoji else ''} \\[{result.confidence}\\]",
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

    # ── Risk score block ──────────────────────────────────────────────────────
    risk_emoji = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴"}.get(result.risk_label, "⚪")
    risk_desc = {
        "LOW":    "Conditions within normal range",
        "MEDIUM": "Elevated risk — size down or monitor closely",
        "HIGH":   "High risk — consider skipping or paper-trade only",
    }.get(result.risk_label, "")
    lines += [
        "",
        f"━━━ *Risk Score* ━━━",
        f"{risk_emoji} *{_escape(result.risk_label)}* — {result.risk_score}/100",
        f"_{_escape(risk_desc)}_",
    ]

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
        f"🎯 Max Pain: " + (f"*${snapshot.max_pain:,.0f}*  gap: ${abs(snapshot.max_pain - snapshot.btc_spot):,.0f}" if snapshot.max_pain else "*N/A*"),
        f"📊 PCR: " + (f"*{_ef(snapshot.pcr, '.2f')}*" if snapshot.pcr else "*N/A*"),
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


def format_noon_signal(snapshot: MarketSnapshot, candidate, checklist_result=None) -> str:
    """
    Trade signal posted at 12:00 PM IST.
    checklist_result: optional ChecklistResult — if verdict is PASS, the header
    changes to ⚠️ CAUTION and the Section A failures are listed prominently.
    candidate is a StraddleCandidate (may be None if chain is empty).
    """
    now = _now_ist_str()
    tp_pct = 30
    sl_pct = 70
    iv_rv_sign = "+" if snapshot.iv_rv_spread >= 0 else ""

    # Determine header based on checklist verdict
    is_caution = checklist_result is not None and checklist_result.verdict == "PASS"

    if is_caution:
        # Collect the specific Section A failures to surface them
        a_failures = [
            detail for name, passed, detail in checklist_result.section_a_details
            if not passed and "A4" not in name
        ]
        lines = [
            f"⚠️ *CAUTION — Review before shorting* — {_escape(now)}",
            f"_Mechanical entry \\| 30% TP \\| hard exit 4:30 PM IST_",
            "",
            f"🚩 *Checklist PASS* — hard gate failures:",
        ]
        for f in a_failures:
            lines.append(f"  • {_escape(f)}")
        lines += [""]
    else:
        lines = [
            f"🚨 *SHORT NOW* — {_escape(now)}",
            f"_Mechanical entry \\| 30% TP \\| hard exit 4:30 PM IST_",
            "",
        ]

    lines += [
        f"🪙 BTC: *${snapshot.btc_spot:,.0f}*  \\|  ⏳ *{_ef(snapshot.hours_to_expiry, '.1f')}hrs* to expiry",
        f"📊 4h move: ${snapshot.btc_4h_move:,.0f}  \\|  24h range: ${snapshot.btc_24h_range:,.0f}",
    ]

    if candidate:
        tp_price = round(candidate.price * (1 - tp_pct / 100))
        sl_price = round(candidate.price * (1 + sl_pct / 100))
        lines += [
            "",
            f"━━━ *TRADE* ━━━",
            f"📋 *{_escape(candidate.symbol)}*",
            f"Strike: *${candidate.strike:,.0f}*  \\|  Entry: *~${candidate.price:,.0f}*",
            f"🎯 TP: *${tp_price:,.0f}*  \\({tp_pct}% decay\\)",
            f"🛑 SL: *${sl_price:,.0f}*  \\({sl_pct}% rise\\)",
            f"⏰ Hard exit: *4:30 PM IST*",
            "",
            f"Δ\\={_ef(candidate.delta, '.3f')}  "
            f"γ\\={_ef(candidate.gamma, '.6f')}",
            f"θ\\=${_ef(candidate.theta_per_hour, '.2f')}/hr",
            f"ν\\={_ef(candidate.vega, '.2f')}  \\|  Vol ${candidate.volume_24h:,.0f}",
        ]

        # Full chain for context — all straddles sorted by |delta|
        if snapshot.straddles:
            chain = sorted(
                [s for s in snapshot.straddles if s["mark_price"] > 0],
                key=lambda s: abs(s["greeks"].get("delta", 99)),
            )
            lines += ["", "━━━ *Full Chain* ━━━"]
            for s in chain:
                g = s["greeks"]
                d = g.get("delta", 0)
                th = abs(g.get("theta", 0)) / 24
                tr = (th / s["mark_price"] * 100) if s["mark_price"] > 0 else 0
                marker = " ←" if s["symbol"] == candidate.symbol else "  "
                lines.append(
                    f"`{_escape(s['symbol'])}`{marker}  "
                    f"${s['mark_price']:,.0f}  Δ\\={_ef(d, '.3f')}  "
                    f"θ\\=${_ef(th, '.2f')}/hr"
                )

        lines += [
            "",
            f"━━━ *Confirm entry* ━━━",
            f"`/entry {candidate.price:.0f} {_escape(candidate.symbol)}`",
        ]
    else:
        lines += [
            "",
            "⚠️ *No valid straddle found in chain*",
            "Check Delta Exchange manually — chain may not be live yet\\.",
        ]

    lines += [
        "",
        f"📈 IV: " + (f"*{_ef(snapshot.implied_vol, '.1f')}%*" if snapshot.implied_vol else "*N/A*") +
        f"  \\|  RV: *{_ef(snapshot.realised_vol, '.1f')}%*  "
        f"\\|  Spread: *{_escape(iv_rv_sign + format(snapshot.iv_rv_spread, '.1f'))}pp*",
        f"🎯 Max Pain: " + (f"${snapshot.max_pain:,.0f}" if snapshot.max_pain else "N/A") +
        f"  \\|  PCR: " + (_ef(snapshot.pcr, '.2f') if snapshot.pcr else "N/A"),
    ]

    return "\n".join(lines)


def format_skip_notification(reason: str) -> str:
    now = _now_ist_str()
    return (
        f"🚫 *Skip Day* — {_escape(now)}\n"
        f"\n"
        f"No trading today: {_escape(reason)}\n"
        f"Next assessment tomorrow at 8:30 AM IST\\."
    )


def format_startup_message() -> str:
    now = _now_ist_str()
    return (
        f"🤖 *Straddle Bot Online* — {_escape(now)}\n"
        f"\n"
        f"Monitoring: BTC Daily Straddles on Delta Exchange India\n"
        f"Schedule:\n"
        f"• Market scan: every 5 min, 24/7 \\(all tenors\\)\n"
        f"• Entry gate: A1 = 4–5\\.5 hrs to expiry \\(per\\-straddle\\)\n"
        f"• Post\\-entry monitor: every 1 min \\(while in trade\\)\n"
        f"\n"
        f"Commands:\n"
        f"`/status` — current market snapshot\n"
        f"`/entry PRICE SYMBOL` — log entry \\(e\\.g\\. `/entry 601 MV\\-BTC\\-70600\\-200326`\\)\n"
        f"`/exit` — clear active position\n"
        f"`/tp PCT` — set TP decay target \\(e\\.g\\. `/tp 30`\\), `/tp reset` for default\n"
        f"`/skip REASON` — skip today's trading\n"
        f"`/resume` — re\\-enable after skip\n"
        f"`/dryrun on\\|off` — toggle paper trade mode\n"
        f"`/help` — show this message"
    )


def format_auto_entry(result, candidate, snapshot: MarketSnapshot, checklist_result=None) -> str:
    """
    Posted when the bot autonomously enters a trade.
    result: ExecutionResult  candidate: StraddleCandidate
    checklist_result: optional ChecklistResult — adds risk score line
    """
    from execution_engine import PAPER_TRADE
    now    = _now_ist_str()
    symbol = candidate.symbol
    # Derive actual TP/SL from fill price using engine constants
    from execution_engine import TP_PCT, SL_MULT
    tp_price = round(result.fill_price * (1 - TP_PCT))
    sl_price = round(result.fill_price * SL_MULT)

    lines = [
        f"🤖 *AUTO\\-ENTERED* \\| {_escape(now)}",
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"`{_escape(symbol)}`",
        f"Fill:  *${result.fill_price:,.0f}*  ×  {result.contracts} contracts",
        f"🎯 TP:  *${tp_price:,.0f}*  \\(−{TP_PCT*100:.0f}%\\)",
        f"🛑 SL:  *${sl_price:,.0f}*  \\(\\+{(SL_MULT-1)*100:.0f}%\\)",
        "",
        f"🪙 BTC: *${snapshot.btc_spot:,.0f}*  \\|  Δ\\={_ef(candidate.delta, '.3f')}",
        f"θ/hr: *{_ef(candidate.theta_ratio * 100, '.1f')}%*  \\|  "
        f"Vega\\={_ef(candidate.vega, '.2f')}",
    ]
    if checklist_result:
        risk_emoji = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴"}.get(checklist_result.risk_label, "⚪")
        lines += [
            "",
            f"Risk: {risk_emoji} *{_escape(checklist_result.risk_label)}* "
            f"\\({checklist_result.risk_score}/100\\)  \\|  "
            f"B score: {checklist_result.section_b_pass}/{checklist_result.section_b_total}",
        ]
    if PAPER_TRADE:
        lines += ["", "_\\[PAPER TRADE\\]_"]
    return "\n".join(lines)


def format_auto_exit(result, alert, symbol: str) -> str:
    """
    Posted when the bot autonomously exits a trade.
    result: ExecutionResult  alert: MonitorAlert
    """
    from execution_engine import PAPER_TRADE
    now = _now_ist_str()

    pnl_sign  = "+" if result.pnl_pct >= 0 else ""
    pnl_emoji = "✅" if result.pnl_pct >= 0 else "🔴"

    lines = [
        f"{pnl_emoji} *CLOSED* \\| {_escape(now)}",
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"`{_escape(symbol)}`",
        f"Exit:  *${result.fill_price:,.0f}*   Reason: {_escape(alert.reason)}",
        f"P&L:   *{_escape(pnl_sign + f'{result.pnl_pct:.1f}%')}*"
        f"  \\(${_escape(pnl_sign + f'{result.pnl_usd:.2f}')}\\)",
        f"Entry: ${alert.entry_price:,.0f}  →  Exit: ${result.fill_price:,.0f}",
    ]
    if PAPER_TRADE:
        lines += ["", "_\\[PAPER TRADE\\]_"]
    return "\n".join(lines)


def format_error(context: str, error: str) -> str:
    return (
        f"⚠️ *Bot Error*\n"
        f"Context: {_escape(context)}\n"
        f"Error: `{_escape(str(error)[:200])}`\n"
        f"Bot continues running\\."
    )