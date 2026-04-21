"""
bot.py
------
Main bot orchestrator.
- Runs the scheduler for hourly scans, entry window scans, and position monitoring
- Handles Telegram commands
- Maintains simple in-memory state (active position, skip flag)

Deploy on Railway or Render as a Python worker (no web server needed).
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Optional

import pytz
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from telegram import Update, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from telegram.constants import ParseMode

from delta_client import DeltaClient
from state_store import load_position, save_position, clear_position
from execution_engine import enter_trade, exit_trade, ExecutionResult, PAPER_TRADE
import trader as _trader
import overnight_trader as _overnight_trader
from analysis_engine import (
    MarketSnapshot,
    run_pretrade_checklist,
    monitor_position,
    find_best_strike,
    calculate_pcr_and_max_pain,
    calculate_realised_vol,
    calculate_implied_vol_from_straddle,
)
from formatter import (
    format_hourly_snapshot,
    format_pretrade_report,
    format_monitor_alert,
    format_skip_notification,
    format_startup_message,
    format_error,
    format_auto_entry,
    format_auto_exit,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# ── Config ────────────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHANNEL_ID = os.environ["TELEGRAM_CHANNEL_ID"]
ENTRY_WINDOW_START        = int(os.getenv("ENTRY_WINDOW_START", "8"))    # 8 AM IST (entry from 8:30)
ENTRY_WINDOW_START_MINUTE = int(os.getenv("ENTRY_WINDOW_START_MINUTE", "30"))  # :30 start
ENTRY_WINDOW_END          = int(os.getenv("ENTRY_WINDOW_END", "13"))    # 1 PM IST
MONITOR_INTERVAL_MINUTES = int(os.getenv("MONITOR_INTERVAL_MINUTES", "1"))
HARD_EXIT_HOUR = int(os.getenv("HARD_EXIT_HOUR", "16"))
HARD_EXIT_MINUTE = int(os.getenv("HARD_EXIT_MINUTE", "30"))
SCAN_INTERVAL_MINUTES = int(os.getenv("SCAN_INTERVAL_MINUTES", "5"))
HOLD_UPDATE_MINUTES   = int(os.getenv("HOLD_UPDATE_MINUTES", "10"))  # how often to post HOLD updates
# Comma-separated tenor names: "daily", "weekly", "monthly" — or "all" for no filter
_tenors_raw = os.getenv("CONTRACT_TENORS", "daily,weekly")
CONTRACT_TENORS: list[str] | None = (
    None if _tenors_raw.strip().lower() == "all"
    else [t.strip() for t in _tenors_raw.split(",") if t.strip()]
)


# ── Bot State ─────────────────────────────────────────────────────────────────

class BotState:
    """
    Simple in-memory state. 
    Note: resets on restart — this is intentional for daily trading use.
    """
    def __init__(self):
        self.skip_today: bool = False
        self.skip_reason: str = ""

        # Active position (set by /entry command or auto-execution)
        self.position_active: bool = False
        self.entry_price: float = 0.0
        self.entry_symbol: str = ""
        self.entry_strike: float = 0.0
        self.tp_target: float = 0.0
        self.sl_target: float = 0.0
        self.entry_time: Optional[datetime] = None
        self.entry_contracts: int = 0
        self.entry_product_id: int = 0

        # TP override — set by /tp command (e.g. /tp 30 sets 30% decay target)
        self.tp_pct_override: Optional[float] = None

        # Last known market data (for commands like /status)
        self.last_snapshot: Optional[MarketSnapshot] = None


state = BotState()


# ── Data Fetcher ──────────────────────────────────────────────────────────────

async def fetch_full_snapshot() -> Optional[MarketSnapshot]:
    """
    Fetches all required market data and assembles a MarketSnapshot.
    Returns None if critical data is unavailable or fetch exceeds 45 seconds.
    """
    try:
        return await asyncio.wait_for(_fetch_snapshot_inner(), timeout=45.0)
    except asyncio.TimeoutError:
        logger.error("fetch_full_snapshot timed out after 45s")
        return None
    except Exception as e:
        logger.error(f"fetch_full_snapshot failed: {e}", exc_info=True)
        return None


async def _fetch_snapshot_inner() -> Optional[MarketSnapshot]:
    try:
        async with DeltaClient() as client:
            # Fetch straddles first so we can align options chain to same expiry date
            straddles = await client.get_all_straddles(CONTRACT_TENORS)

            # Derive the target expiry date from the soonest live straddle
            expiry_date_str: str | None = None
            if straddles:
                settlement = straddles[0].get("settlement_time", "")
                if settlement:
                    try:
                        from datetime import timezone
                        st_utc = datetime.fromisoformat(settlement.replace("Z", "+00:00"))
                        st_ist = st_utc.astimezone(IST)
                        expiry_date_str = st_ist.strftime("%d%m%y")
                    except Exception:
                        pass

            # Remaining fetches in parallel, options chain aligned to straddle expiry
            btc_task        = asyncio.create_task(client.get_btc_spot())
            options_task    = asyncio.create_task(client.get_options_chain(expiry_date_str))
            candles_task    = asyncio.create_task(client.get_btc_candles(resolution="1h", count=720))
            candles_5m_task = asyncio.create_task(client.get_btc_candles(resolution="5m", count=100))

            btc_spot, options_chain, candles_1h, candles_5m = await asyncio.gather(
                btc_task, options_task, candles_task, candles_5m_task,
                return_exceptions=True,
            )

        # hours_to_expiry: use the soonest-expiry straddle's value, or 0 if none
        hours_to_expiry = straddles[0]["hours_to_expiry"] if straddles else 0.0

        # Handle fetch errors gracefully
        if isinstance(btc_spot, Exception):
            logger.error(f"Failed to fetch BTC spot: {btc_spot}")
            return None

        if isinstance(options_chain, Exception):
            logger.warning(f"Failed to fetch options chain: {options_chain}")
            options_chain = []
        elif not options_chain:
            logger.info(f"Options chain empty for expiry_date_str={expiry_date_str} (next-day chain not yet listed)")

        if isinstance(candles_1h, Exception):
            logger.warning(f"Failed to fetch 1h candles: {candles_1h}")
            candles_1h = []

        if isinstance(candles_5m, Exception):
            candles_5m = []

        # ── Calculate derived metrics ────────────────────────────────────────

        # PCR + Max Pain
        pcr, max_pain, calls_oi, puts_oi = calculate_pcr_and_max_pain(options_chain)

        # Realised Volatility (from 24 hours of 1hr candles)
        rv = calculate_realised_vol(candles_1h, window=720) if candles_1h else 0.0

        # Per-straddle IV fallback: Delta API often returns null implied_volatility
        # for next-day contracts. Back-calculate from straddle price when missing.
        for s in straddles:
            direct_iv = s.get("iv", 0)
            if direct_iv and direct_iv > 0:
                s["iv"] = direct_iv * 100 if direct_iv < 5 else direct_iv
            else:
                s["iv"] = calculate_implied_vol_from_straddle(
                    s["mark_price"], float(btc_spot), s["strike"],
                    s.get("hours_to_expiry", hours_to_expiry),
                )

        # Implied Vol — from the nearest ATM straddle (iv already normalised above)
        atm_iv = 0.0
        if straddles:
            nearest_atm = min(straddles, key=lambda s: abs(s["strike"] - btc_spot))
            atm_iv = nearest_atm["iv"]  # already calculated/normalised above

        iv_rv_spread = atm_iv - rv

        # BTC 24h metrics (from 24 hourly candles)
        btc_24h_high = max((c["high"] for c in candles_1h[-24:]), default=btc_spot)
        btc_24h_low = min((c["low"] for c in candles_1h[-24:]), default=btc_spot)
        btc_24h_range = btc_24h_high - btc_24h_low

        # BTC 4hr move — candles are sorted ascending (oldest first, newest last).
        # [-1] = most recent candle, [-5] = the candle that closed ~4 hours ago.
        # Use close price for a more accurate reference point.
        if len(candles_1h) >= 5:
            price_4h_ago = candles_1h[-5]["close"]
            btc_4h_move = abs(btc_spot - price_4h_ago)
        else:
            btc_4h_move = 0.0

        now_ist = datetime.now(IST)

        snapshot = MarketSnapshot(
            timestamp=now_ist.strftime("%Y-%m-%d %H:%M IST"),
            btc_spot=float(btc_spot),
            hours_to_expiry=hours_to_expiry,
            straddles=straddles if not isinstance(straddles, Exception) else [],
            pcr=pcr,
            max_pain=max_pain,
            total_calls_oi=calls_oi,
            total_puts_oi=puts_oi,
            realised_vol=rv,
            implied_vol=atm_iv,
            iv_rv_spread=iv_rv_spread,
            btc_24h_range=btc_24h_range,
            btc_24h_high=btc_24h_high,
            btc_24h_low=btc_24h_low,
            btc_4h_move=btc_4h_move,
            day_of_week=now_ist.weekday(),
        )

        state.last_snapshot = snapshot
        return snapshot

    except Exception as e:
        logger.error(f"_fetch_snapshot_inner failed: {e}", exc_info=True)
        return None


# ── Telegram Sender ───────────────────────────────────────────────────────────

def _strip_markdown(text: str) -> str:
    return text.replace("\\", "").replace("*", "").replace("`", "").replace("_", "")


async def send_message(bot: Bot, text: str):
    """Posts to the configured channel with MarkdownV2 formatting."""
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHANNEL_ID,
            text=text,
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except Exception as e:
        logger.error(f"Failed to send Telegram message (MarkdownV2): {e}")
        try:
            await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=_strip_markdown(text))
        except Exception as e2:
            logger.error(f"Plain text fallback also failed: {e2}")


async def reply_formatted(message, text: str):
    """Reply to a command message with MarkdownV2 + plain text fallback."""
    try:
        await message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"reply_text MarkdownV2 failed: {e}")
        try:
            await message.reply_text(_strip_markdown(text))
        except Exception as e2:
            logger.error(f"reply_text plain fallback also failed: {e2}")


# ── Scheduled Jobs ────────────────────────────────────────────────────────────

async def job_scan(bot: Bot):
    """
    Unified market scan — runs every SCAN_INTERVAL_MINUTES (default 5) minutes, 24/7.

    Behaviour:
    - Always fetches straddles across all configured tenors (CONTRACT_TENORS).
    - While no position is active: runs full pre-trade checklist.
      If TRADE verdict → auto-executes entry (A1 gate uses per-straddle hours_to_expiry,
      so only straddles genuinely 4-5.5 hours from expiry qualify regardless of clock time).
    - Suppresses Telegram output when checklist verdict is PASS and no alert-worthy change.
      Avoids spamming the channel with 288 identical "no trade" messages per day.
    """
    if state.skip_today:
        return

    now_ist = datetime.now(IST)
    logger.debug(f"Scan at {now_ist.strftime('%H:%M IST')}")

    snapshot = await fetch_full_snapshot()
    if snapshot is None:
        # Only post errors, don't spam
        logger.warning("Scan: failed to fetch snapshot")
        return

    state.last_snapshot = snapshot

    if not state.position_active:
        result = run_pretrade_checklist(snapshot)

        # Post to Telegram only when something worth seeing:
        # TRADE signal, WAIT (conditions close but not yet), or the first PASS after a non-PASS
        prev_verdict = getattr(state, "_last_verdict", None)
        if result.verdict in ("TRADE", "WAIT") or (
            result.verdict == "PASS" and prev_verdict in ("TRADE", "WAIT", None)
        ):
            msg = format_pretrade_report(result, snapshot)
            await send_message(bot, msg)
        state._last_verdict = result.verdict

        # Auto-execute if TRADE
        if result.verdict == "TRADE" and result.best_candidate:
            candidate = result.best_candidate
            is_half   = (result.confidence == "LOW")

            async with DeltaClient() as ex_client:
                try:
                    product_id = await ex_client.get_product_id(candidate.symbol)
                except Exception as e:
                    await send_message(bot, format_error("get_product_id", str(e)))
                    return

                exec_result = await enter_trade(
                    ex_client, candidate.symbol, candidate.strike,
                    product_id, candidate.price, is_half_size=is_half,
                )

            if exec_result.success:
                state.position_active  = True
                state.entry_price      = exec_result.fill_price
                state.entry_symbol     = candidate.symbol
                state.entry_strike     = candidate.strike
                state.tp_target        = round(exec_result.fill_price * 0.75)
                state.sl_target        = round(exec_result.fill_price * 1.20)
                state.entry_time       = datetime.now(IST)
                state.entry_contracts  = exec_result.contracts
                state.entry_product_id = product_id
                state._last_verdict    = None  # reset for next cycle
                await send_message(bot, format_auto_entry(exec_result, candidate, snapshot, result))
            else:
                await send_message(bot, format_error("Auto-entry failed", exec_result.error))


async def job_hourly_snapshot(bot: Bot):
    """
    Posts a market snapshot every hour on the :00. Always fires — no skip window.
    """
    if state.skip_today:
        return
    snapshot = await fetch_full_snapshot()
    if snapshot is None:
        return
    state.last_snapshot = snapshot
    checklist_result = run_pretrade_checklist(snapshot)
    await send_message(bot, format_hourly_snapshot(snapshot, checklist_result))


async def job_entry_window_scan(bot: Bot):
    """
    Runs every 15 min during the entry window (8:30 AM–11 AM IST).
    Always posts the full pre-trade checklist so the trader sees market conditions
    at a glance every 15 minutes.
    Auto-execution is handled by job_scan (5-min); this job is messaging-only.
    """
    if state.skip_today or state.position_active:
        return
    now_ist = datetime.now(IST)
    in_window = (
        (now_ist.hour > ENTRY_WINDOW_START or
         (now_ist.hour == ENTRY_WINDOW_START and now_ist.minute >= ENTRY_WINDOW_START_MINUTE))
        and now_ist.hour < ENTRY_WINDOW_END
    )
    if not in_window:
        return
    snapshot = await fetch_full_snapshot()
    if snapshot is None:
        return
    state.last_snapshot = snapshot
    result = run_pretrade_checklist(snapshot)
    state._last_verdict = result.verdict  # keep 24/7 scanner in sync
    await send_message(bot, format_pretrade_report(result, snapshot))


async def job_monitor_position(bot: Bot):
    """
    Runs every 10 minutes while a position is active.
    Posts hold/partial/exit alerts.
    """
    if not state.position_active:
        return

    now_ist = datetime.now(IST)
    logger.info(f"Monitoring position at {now_ist.strftime('%H:%M IST')}")

    snapshot = await fetch_full_snapshot()
    if snapshot is None:
        await send_message(bot, format_error("position monitor", "Failed to fetch data"))
        return

    # Find current price of the active straddle
    current_price = None
    current_delta = 0.0
    current_theta = 0.0

    for s in snapshot.straddles:
        if s["symbol"] == state.entry_symbol:
            current_price = s["mark_price"]
            current_delta = s["greeks"].get("delta", 0)
            current_theta = s["greeks"].get("theta", 0)
            break

    if current_price is None:
        await send_message(
            bot,
            f"⚠️ Could not find `{state.entry_symbol}` in live chain\\. Check manually\\."
        )
        return

    hours_remaining = snapshot.hours_to_expiry
    is_weekend = snapshot.day_of_week in (5, 6)

    alert = monitor_position(
        entry_price=state.entry_price,
        current_price=current_price,
        entry_symbol=state.entry_symbol,
        btc_spot=snapshot.btc_spot,
        btc_strike=state.entry_strike,
        delta=current_delta,
        theta=current_theta,
        hours_remaining=hours_remaining,
        tp_target=state.tp_target,
        sl_target=state.sl_target,
        is_weekend=is_weekend,
    )

    # Always post on action exits; throttle HOLD/PARTIAL to every HOLD_UPDATE_MINUTES
    should_post = alert.action in ("EXIT", "HARD_EXIT")
    if not should_post:
        last_post = getattr(state, "_last_monitor_post", None)
        elapsed   = (now_ist - last_post).total_seconds() / 60 if last_post else 999
        if elapsed >= HOLD_UPDATE_MINUTES:
            should_post = True
            state._last_monitor_post = now_ist

    if should_post:
        msg = format_monitor_alert(alert, state.entry_symbol, state.entry_strike)
        await send_message(bot, msg)

    # Auto-execute exit on TP/SL/HARD_EXIT
    if alert.action in ("EXIT", "HARD_EXIT") and alert.urgency in ("HIGH", "CRITICAL"):
        saved = load_position()
        product_id = state.entry_product_id or (saved.get("product_id", 0) if saved else 0)
        contracts  = state.entry_contracts  or (saved.get("contracts",  0) if saved else 0)

        if product_id and contracts:
            async with DeltaClient() as ex_client:
                exec_result = await exit_trade(
                    ex_client, state.entry_symbol, product_id,
                    contracts, state.entry_price, reason=alert.reason,
                )
            state.position_active = False
            if exec_result.success:
                await send_message(bot, format_auto_exit(exec_result, alert, state.entry_symbol))
            else:
                await send_message(
                    bot,
                    format_error("Auto-exit FAILED", exec_result.error)
                    + "\n⚠️ *Close manually on Delta Exchange\\!*"
                )
        else:
            # No product_id — just clear state (manual position via /entry)
            state.position_active = False
            clear_position()
            logger.info(f"Position cleared (no product_id — manual entry): {alert.action}")


# ── Telegram Commands ─────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        format_startup_message(),
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Returns current market snapshot on demand."""
    await update.message.reply_text("Fetching... ⏳")
    snapshot = await fetch_full_snapshot()
    if snapshot is None:
        await update.message.reply_text("❌ Failed to fetch market data. Try again.")
        return
    checklist_result = run_pretrade_checklist(snapshot)
    await reply_formatted(update.message, format_hourly_snapshot(snapshot, checklist_result))


async def cmd_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Log that you've entered a position.
    Usage: /entry PRICE SYMBOL
    Example: /entry 601 MV-BTC-70600-200326
    """
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: `/entry PRICE SYMBOL`\n"
            "Example: `/entry 601 MV\\-BTC\\-70600\\-200326`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    try:
        price = float(args[0])
        symbol = args[1]

        # Parse strike from symbol
        parts = symbol.split("-")
        strike = float(parts[2]) if len(parts) >= 3 else 0.0

        # Set TP/SL — use override if set, else grid-search optimal defaults
        now_ist = datetime.now(IST)
        if state.tp_pct_override is not None:
            tp_pct = state.tp_pct_override / 100
        else:
            tp_pct = 0.25  # 25% decay — grid search optimal

        state.position_active  = True
        state.entry_price      = price
        state.entry_symbol     = symbol
        state.entry_strike     = strike
        state.tp_target        = round(price * (1 - tp_pct))
        state.sl_target        = round(price * 1.20)
        state.entry_time       = now_ist
        state.entry_contracts  = 0   # manual — contracts unknown
        state.entry_product_id = 0   # manual — product_id unknown

        # Persist so monitoring survives a restart
        save_position({
            "symbol":      symbol,
            "strike":      int(strike),
            "product_id":  0,
            "entry_price": price,
            "entry_time":  now_ist.isoformat(),
            "contracts":   0,
            "tp_target":   state.tp_target,
            "sl_target":   state.sl_target,
            "order_id":    "MANUAL",
            "paper_trade": False,
        })

        msg = (
            f"✅ Position Logged\n\n"
            f"Symbol: {symbol}\n"
            f"Entry: ${price:,.0f}\n"
            f"Strike: ${strike:,.0f}\n"
            f"TP: ${state.tp_target:,.0f}\n"
            f"SL: ${state.sl_target:,.0f}\n"
            f"Hard exit: 4:30 PM IST\n\n"
            f"Monitoring every {MONITOR_INTERVAL_MINUTES} minutes."
        )
        await update.message.reply_text(msg)

    except (ValueError, IndexError) as e:
        await update.message.reply_text(f"❌ Error parsing entry: {e}")


async def cmd_exit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear the active position."""
    if not state.position_active:
        await update.message.reply_text("No active position to clear.")
        return

    state.position_active = False
    clear_position()
    await update.message.reply_text(
        f"✅ Position `{state.entry_symbol}` cleared\\. Monitoring stopped\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def cmd_tp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set a custom TP decay target. Usage: /tp 30  or  /tp reset"""
    args = context.args
    if not args:
        current = f"{state.tp_pct_override:.0f}%" if state.tp_pct_override is not None else "default (50% weekday / 40% weekend)"
        await update.message.reply_text(
            f"Current TP target: {current}\n\nUsage: `/tp 30` to set 30% decay\\.\n`/tp reset` to restore default\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if args[0].lower() == "reset":
        state.tp_pct_override = None
        await update.message.reply_text(
            "✅ TP target reset to default \\(50% weekday / 40% weekend\\)\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    try:
        pct = float(args[0])
        if not (5 <= pct <= 95):
            await update.message.reply_text("❌ TP% must be between 5 and 95.")
            return
        state.tp_pct_override = pct

        # Recalculate TP for active position if one exists
        recalc_msg = ""
        if state.position_active:
            state.tp_target = round(state.entry_price * (1 - pct / 100))
            recalc_msg = f"\nActive position TP updated to \\$`{state.tp_target:,.0f}`\\."

        await update.message.reply_text(
            f"✅ TP target set to *{pct:.0f}%* decay\\." + recalc_msg,
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except ValueError:
        await update.message.reply_text("❌ Invalid value. Usage: `/tp 30` or `/tp reset`")


async def cmd_skip_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Skip the automated trader for today. Usage: /skip_today FOMC meeting"""
    reason = " ".join(context.args) if context.args else "manual skip"
    _trader.skip_today(reason)
    await update.message.reply_text(f"✅ Automated trader will skip today: {reason}")


async def cmd_position(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the automated trader's current position and trail status."""
    await update.message.reply_text(_trader.get_position_status())


async def cmd_dryrun(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle paper trade mode at runtime. Usage: /dryrun on  or  /dryrun off"""
    import execution_engine as ee
    args = context.args
    if not args or args[0].lower() not in ("on", "off"):
        current = "ON \\(paper\\)" if ee.PAPER_TRADE else "OFF \\(live\\)"
        await update.message.reply_text(
            f"Paper trade mode is currently: *{current}*\n\n"
            f"Usage: `/dryrun on` or `/dryrun off`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    ee.PAPER_TRADE = args[0].lower() == "on"
    status = "ON \\(paper trade — no real orders\\)" if ee.PAPER_TRADE else "OFF \\(live trading\\)"
    await update.message.reply_text(
        f"✅ Paper trade mode: *{status}*",
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def cmd_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Skip today's trading (e.g. for macro events). Usage: /skip FOMC today"""
    reason = " ".join(context.args) if context.args else "manual skip"
    state.skip_today = True
    state.skip_reason = reason
    msg = format_skip_notification(reason)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
    await send_message(context.bot, msg)


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Re-enable trading after a skip."""
    state.skip_today = False
    state.skip_reason = ""
    await update.message.reply_text("✅ Trading resumed\\. Bot active again\\.", parse_mode=ParseMode.MARKDOWN_V2)


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run the full pre-trade checklist on demand (any time)."""
    await update.message.reply_text("Running checklist... ⏳")
    snapshot = await fetch_full_snapshot()
    if snapshot is None:
        await update.message.reply_text("❌ Failed to fetch market data. Check Render logs.")
        return
    result = run_pretrade_checklist(snapshot)
    await reply_formatted(update.message, format_pretrade_report(result, snapshot))


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        format_startup_message(),
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ── Application Setup ─────────────────────────────────────────────────────────

async def post_init(application: Application) -> None:
    """Called after the bot starts — set up scheduler and post startup message."""
    bot = application.bot
    scheduler = AsyncIOScheduler(timezone=IST)

    _tz = "Asia/Kolkata"  # use string form — more reliable across APScheduler versions

    # 24/7 silent scan — every SCAN_INTERVAL_MINUTES (default 5)
    # Posts only on TRADE/WAIT verdict; handles auto-execution
    scheduler.add_job(
        job_scan,
        IntervalTrigger(minutes=SCAN_INTERVAL_MINUTES, timezone=IST),
        args=[bot],
        id="market_scan",
        name=f"Market scan every {SCAN_INTERVAL_MINUTES}min",
        misfire_grace_time=60,
    )

    # Hourly snapshot — every :00, always fires (no skip window)
    scheduler.add_job(
        job_hourly_snapshot,
        CronTrigger(minute=0, timezone=IST),
        args=[bot],
        id="hourly_snapshot",
        name="Hourly market snapshot",
        misfire_grace_time=120,
    )

    # Entry window checklist — every 15 min, 8:30 AM–12:45 PM IST
    # Job function itself enforces the 8:30 start; CronTrigger covers hours 8–12
    scheduler.add_job(
        job_entry_window_scan,
        CronTrigger(hour="8,9,10,11,12", minute="0,15,30,45", timezone=IST),
        args=[bot],
        id="entry_window_scan",
        name="Entry window pre-trade scan (15 min, 8:30 AM–1 PM)",
        misfire_grace_time=60,
    )

    # Position monitor — every MONITOR_INTERVAL_MINUTES (default 1) while in a trade
    scheduler.add_job(
        job_monitor_position,
        IntervalTrigger(minutes=MONITOR_INTERVAL_MINUTES, timezone=IST),
        args=[bot],
        id="position_monitor",
        name="Position monitor",
        misfire_grace_time=30,
    )

    # Automated trailing-SL trader — fires at 05:45 IST on Mon/Wed/Thu/Sun
    # The job itself sleeps internally until 06:00 and handles all lifecycle.
    scheduler.add_job(
        _trader.run_trade_job,
        CronTrigger(hour=5, minute=45, day_of_week="mon,wed,thu,sun", timezone=IST),
        id="auto_trader",
        name="Automated trailing-SL trader (Mon/Wed/Thu/Sun 06:00 IST)",
        misfire_grace_time=300,
    )

    # Overnight short straddle — fires at 23:50 IST on Mon/Wed/Thu/Fri.
    # The job sleeps internally until the per-day entry time (00:00 or 00:30 IST
    # the following morning), then runs the full lifecycle through 05:30 IST.
    # No timing conflict: daytime trader exits by 16:30 IST; overnight exits by 05:30 IST.
    scheduler.add_job(
        _overnight_trader.run_overnight_job,
        CronTrigger(hour=23, minute=50, day_of_week="mon,wed,thu,fri", timezone=IST),
        id="overnight_trader",
        name="Overnight short straddle (Tue/Thu/Fri/Sat 00:00-05:30 IST)",
        misfire_grace_time=300,
    )

    scheduler.start()
    logger.info("Scheduler started")

    # Restore position state from disk (handles restarts / redeployments)
    saved = load_position()
    if saved:
        state.position_active   = True
        state.entry_price       = saved["entry_price"]
        state.entry_symbol      = saved["symbol"]
        state.entry_strike      = float(saved.get("strike", 0))
        state.tp_target         = saved["tp_target"]
        state.sl_target         = saved["sl_target"]
        state.entry_time        = datetime.fromisoformat(saved["entry_time"])
        state.entry_contracts   = saved.get("contracts", 0)
        state.entry_product_id  = saved.get("product_id", 0)
        paper_tag = " \\[PAPER\\]" if saved.get("paper_trade") else ""
        await send_message(
            bot,
            f"♻️ *Recovered open position from state file*{paper_tag}\n"
            f"`{saved['symbol']}` @ \\${saved['entry_price']:,.0f}\n"
            f"TP: \\${saved['tp_target']:,.0f}  \\|  SL: \\${saved['sl_target']:,.0f}"
        )
        logger.info(f"Recovered position: {saved['symbol']} @ {saved['entry_price']}")

    # Post startup message to channel
    await send_message(bot, format_startup_message())
    logger.info("Startup message posted")


def main():
    """Entry point. Run this to start the bot."""
    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("check", cmd_check))
    app.add_handler(CommandHandler("entry", cmd_entry))
    app.add_handler(CommandHandler("exit", cmd_exit))
    app.add_handler(CommandHandler("tp", cmd_tp))
    app.add_handler(CommandHandler("skip", cmd_skip))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("dryrun", cmd_dryrun))
    app.add_handler(CommandHandler("skip_today", cmd_skip_today))
    app.add_handler(CommandHandler("position", cmd_position))

    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())
    main()