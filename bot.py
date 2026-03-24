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
from analysis_engine import (
    MarketSnapshot,
    run_pretrade_checklist,
    monitor_position,
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
ENTRY_WINDOW_START = int(os.getenv("ENTRY_WINDOW_START", "11"))
ENTRY_WINDOW_END = int(os.getenv("ENTRY_WINDOW_END", "13"))
MONITOR_INTERVAL_MINUTES = int(os.getenv("MONITOR_INTERVAL_MINUTES", "10"))
HARD_EXIT_HOUR = int(os.getenv("HARD_EXIT_HOUR", "16"))
HARD_EXIT_MINUTE = int(os.getenv("HARD_EXIT_MINUTE", "30"))


# ── Bot State ─────────────────────────────────────────────────────────────────

class BotState:
    """
    Simple in-memory state. 
    Note: resets on restart — this is intentional for daily trading use.
    """
    def __init__(self):
        self.skip_today: bool = False
        self.skip_reason: str = ""

        # Active position (set by /entry command)
        self.position_active: bool = False
        self.entry_price: float = 0.0
        self.entry_symbol: str = ""
        self.entry_strike: float = 0.0
        self.tp_target: float = 0.0
        self.sl_target: float = 0.0
        self.entry_time: Optional[datetime] = None

        # TP override — set by /tp command (e.g. /tp 30 sets 30% decay target)
        self.tp_pct_override: Optional[float] = None

        # Last known market data (for commands like /status)
        self.last_snapshot: Optional[MarketSnapshot] = None


state = BotState()


# ── Data Fetcher ──────────────────────────────────────────────────────────────

async def fetch_full_snapshot() -> Optional[MarketSnapshot]:
    """
    Fetches all required market data and assembles a MarketSnapshot.
    Returns None if critical data is unavailable.
    """
    try:
        async with DeltaClient() as client:
            # Concurrent fetches for speed
            btc_task = asyncio.create_task(client.get_btc_spot())
            straddles_task = asyncio.create_task(client.get_today_straddles())
            options_task = asyncio.create_task(client.get_options_chain())
            candles_task = asyncio.create_task(client.get_btc_candles(resolution="1h", count=48))
            candles_5m_task = asyncio.create_task(client.get_btc_candles(resolution="5m", count=100))

            btc_spot, straddles, options_chain, candles_1h, candles_5m = await asyncio.gather(
                btc_task, straddles_task, options_task, candles_task, candles_5m_task,
                return_exceptions=True,
            )

            hours_to_expiry = client.hours_to_expiry()

        # Handle fetch errors gracefully
        if isinstance(btc_spot, Exception):
            logger.error(f"Failed to fetch BTC spot: {btc_spot}")
            return None

        if isinstance(straddles, Exception):
            logger.warning(f"Failed to fetch straddles: {straddles}")
            straddles = []

        if isinstance(options_chain, Exception):
            logger.warning(f"Failed to fetch options chain: {options_chain}")
            options_chain = []

        if isinstance(candles_1h, Exception):
            logger.warning(f"Failed to fetch 1h candles: {candles_1h}")
            candles_1h = []

        if isinstance(candles_5m, Exception):
            candles_5m = []

        # ── Calculate derived metrics ────────────────────────────────────────

        # PCR + Max Pain
        pcr, max_pain, calls_oi, puts_oi = calculate_pcr_and_max_pain(options_chain)

        # Realised Volatility (from 24 hours of 1hr candles)
        rv = calculate_realised_vol(candles_1h, window=24) if candles_1h else 0.0

        # Implied Vol — from the nearest ATM straddle
        atm_iv = 0.0
        atm_price = 0.0
        if straddles:
            nearest_atm = min(straddles, key=lambda s: abs(s["strike"] - btc_spot))
            atm_price = nearest_atm["mark_price"]
            atm_iv = calculate_implied_vol_from_straddle(
                atm_price, btc_spot, nearest_atm["strike"], hours_to_expiry
            )
            # Also check if IV is directly available from greeks
            direct_iv = nearest_atm.get("iv", 0)
            if direct_iv > 0:
                atm_iv = direct_iv * 100 if direct_iv < 5 else direct_iv

        iv_rv_spread = atm_iv - rv

        # BTC 24h metrics (from 24 hourly candles)
        btc_24h_high = max((c["high"] for c in candles_1h[-24:]), default=btc_spot)
        btc_24h_low = min((c["low"] for c in candles_1h[-24:]), default=btc_spot)
        btc_24h_range = btc_24h_high - btc_24h_low

        # BTC 4hr move (from last 4 hourly candles)
        if len(candles_1h) >= 4:
            price_4h_ago = candles_1h[-4]["open"]
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
        logger.error(f"fetch_full_snapshot failed: {e}", exc_info=True)
        return None


# ── Telegram Sender ───────────────────────────────────────────────────────────

async def send_message(bot: Bot, text: str):
    """Posts to the configured channel with MarkdownV2 formatting."""
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHANNEL_ID,
            text=text,
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")
        # Try plain text fallback
        try:
            plain = text.replace("\\", "").replace("*", "").replace("`", "")
            await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=plain)
        except Exception as e2:
            logger.error(f"Plain text fallback also failed: {e2}")


# ── Scheduled Jobs ────────────────────────────────────────────────────────────

async def job_hourly_scan(bot: Bot):
    """
    Runs every hour outside the entry window.
    Posts a brief market context update.
    """
    now_ist = datetime.now(IST)
    if ENTRY_WINDOW_START <= now_ist.hour < ENTRY_WINDOW_END:
        return  # Entry window jobs handle this period

    logger.info(f"Hourly scan at {now_ist.strftime('%H:%M IST')}")

    if state.skip_today:
        return  # Silent on skip days

    snapshot = await fetch_full_snapshot()
    if snapshot is None:
        await send_message(bot, format_error("hourly scan", "Failed to fetch market data"))
        return

    msg = format_hourly_snapshot(snapshot)
    await send_message(bot, msg)


async def job_entry_window_scan(bot: Bot):
    """
    Runs every 15 minutes during 11 AM – 1 PM IST.
    Runs full pre-trade checklist and posts TRADE/WAIT/PASS verdict.
    """
    now_ist = datetime.now(IST)
    if not (ENTRY_WINDOW_START <= now_ist.hour < ENTRY_WINDOW_END):
        return

    if state.skip_today:
        return

    if state.position_active:
        return  # Don't scan for new entries while in a position

    logger.info(f"Entry window scan at {now_ist.strftime('%H:%M IST')}")

    snapshot = await fetch_full_snapshot()
    if snapshot is None:
        await send_message(bot, format_error("entry window scan", "Failed to fetch data"))
        return

    result = run_pretrade_checklist(snapshot)
    msg = format_pretrade_report(result, snapshot)
    await send_message(bot, msg)

    # Auto-notify if it's a TRADE verdict
    if result.verdict == "TRADE" and result.best_candidate:
        extra = (
            f"🔔 *TRADE SIGNAL FIRED*\n"
            f"Enter `{result.best_candidate.symbol}` at market/limit ~${result.best_candidate.price:,.0f}\n"
            f"Then confirm with: `/entry {result.best_candidate.price:.0f} {result.best_candidate.symbol}`"
        )
        # Escape for markdown
        extra = extra.replace("-", "\\-").replace(".", "\\.").replace("(", "\\(").replace(")", "\\)")
        await send_message(bot, extra)


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

    msg = format_monitor_alert(alert, state.entry_symbol, state.entry_strike)
    await send_message(bot, msg)

    # Auto-clear position on TP/SL/HARD_EXIT
    if alert.action in ("EXIT", "HARD_EXIT") and alert.urgency in ("HIGH", "CRITICAL"):
        state.position_active = False
        logger.info(f"Position auto-cleared: {alert.action} — {alert.reason}")


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
    await update.message.reply_text(
        format_hourly_snapshot(snapshot),
        parse_mode=ParseMode.MARKDOWN_V2,
    )


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

        # Set TP/SL — use override if set, else weekend/weekday defaults
        now_ist = datetime.now(IST)
        is_weekend = now_ist.weekday() in (5, 6)
        if state.tp_pct_override is not None:
            tp_pct = state.tp_pct_override / 100
        else:
            tp_pct = 0.40 if is_weekend else 0.50

        state.position_active = True
        state.entry_price = price
        state.entry_symbol = symbol
        state.entry_strike = strike
        state.tp_target = round(price * (1 - tp_pct))
        state.sl_target = round(price * 1.70)
        state.entry_time = now_ist

        msg = (
            f"✅ *Position Logged*\n\n"
            f"Symbol: `{symbol}`\n"
            f"Entry: ${price:,.0f}\n"
            f"Strike: ${strike:,.0f}\n"
            f"TP: ${state.tp_target:,.0f}\n"
            f"SL: ${state.sl_target:,.0f}\n"
            f"Hard exit: 4:30 PM IST\n\n"
            f"Monitoring every {MONITOR_INTERVAL_MINUTES} minutes\\."
        )
        msg = msg.replace("-", "\\-").replace(".", "\\.")
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)

    except (ValueError, IndexError) as e:
        await update.message.reply_text(f"❌ Error parsing entry: {e}")


async def cmd_exit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear the active position."""
    if not state.position_active:
        await update.message.reply_text("No active position to clear.")
        return

    state.position_active = False
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
        await update.message.reply_text("❌ Failed to fetch market data.")
        return
    result = run_pretrade_checklist(snapshot)
    msg = format_pretrade_report(result, snapshot)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)


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

    # Hourly scan — every hour at :00 (except during entry window)
    scheduler.add_job(
        job_hourly_scan,
        CronTrigger(minute=0, timezone=IST),
        args=[bot],
        id="hourly_scan",
        name="Hourly market scan",
        misfire_grace_time=120,
    )

    # Entry window scans — every 15 minutes, 11 AM – 1 PM IST
    scheduler.add_job(
        job_entry_window_scan,
        CronTrigger(
            hour=f"{ENTRY_WINDOW_START}-{ENTRY_WINDOW_END - 1}",
            minute="0,15,30,45",
            timezone=IST,
        ),
        args=[bot],
        id="entry_window_scan",
        name="Entry window scan",
        misfire_grace_time=60,
    )

    # Position monitor — every N minutes all day
    scheduler.add_job(
        job_monitor_position,
        IntervalTrigger(minutes=MONITOR_INTERVAL_MINUTES, timezone=IST),
        args=[bot],
        id="position_monitor",
        name="Position monitor",
        misfire_grace_time=30,
    )

    scheduler.start()
    logger.info("Scheduler started")

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

    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())
    main()