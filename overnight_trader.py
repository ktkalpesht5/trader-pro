"""
overnight_trader.py
-------------------
Automated overnight short straddle trader for BTC on Delta Exchange India.

Schedule  : Triggered at 23:50 IST on Mon/Wed/Thu/Fri (night before the trade)
Entry     : 00:00 or 00:30 IST next morning (per-day rule)
Exit      : 05:30 IST hard exit, or TP/SL hit during hold
Lots      : 100 (TRADE_LOTS env var)
Monitoring: REST poll every 15s — mark_price + close (last traded) dual confirmation
Post to   : AMTRADINGLOGS_CHANNEL_ID (same channel as daytime trader)

Per-day rules (night-of → trade-day, entry, TP, SL):
  Mon 23:50 → Tuesday  00:30 entry, TP=$450, SL=$500
  Wed 23:50 → Thursday 00:00 entry, TP=$650, SL=$1,000
  Thu 23:50 → Friday   00:30 entry, TP=$350, SL=$600
  Fri 23:50 → Saturday 00:30 entry, TP=$700, SL=$900

Hard skip days (no entry): Sunday, Monday, Wednesday mornings
Macro skips: checked via macro_calendar.should_skip()

PnL scaling: price_change × (lots / 1000)   [100 lots → ×0.1, same as daytime trader]

State file: overnight_position.json   (separate from daytime position.json)
Trade log:  overnight_trades.json     (separate from daytime trades.json)
"""

import asyncio
import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Optional

import pytz
from telegram import Bot

from delta_client import DeltaClient
from macro_calendar import should_skip
from trade_logger import TradeLogger

IST = pytz.timezone("Asia/Kolkata")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("AMTRADINGLOGS_CHANNEL_ID", "")
PAPER_TRADE     = os.getenv("EE_PAPER_TRADE", "false").lower() == "true"
ENTRY_LOTS      = int(os.getenv("TRADE_LOTS", "100"))
FILL_TIMEOUT_S  = 60
CLOSE_RETRY_S   = 300
POLL_INTERVAL_S = 15     # REST price check every 15s — matches daytime trader REST_POLL_S

STATE_FILE      = "overnight_position.json"
_TMP_STATE_FILE = "overnight_position.json.tmp"

_trade_logger   = TradeLogger("overnight_trades.json")

# ── Per-day rules ─────────────────────────────────────────────────────────────
# trade_day_name → (entry_h, entry_m, exit_h, exit_m, tp_usd, sl_usd)

DAY_RULES: dict[str, tuple[int, int, int, int, int, int]] = {
    "Tuesday":  (0, 30, 5, 30, 450,  500),
    "Thursday": (0,  0, 5, 30, 650, 1000),
    "Friday":   (0, 30, 5, 30, 350,  600),
    "Saturday": (0, 30, 5, 30, 700,  900),
}

IV_FLOOR_PCT = 0.5   # skip if straddle < 0.5% of BTC spot


# ── Telegram ──────────────────────────────────────────────────────────────────

async def _telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        bot = Bot(token=TELEGRAM_TOKEN)
        async with bot:
            await bot.send_message(chat_id=TELEGRAM_CHAT, text=text)
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")


# ── Time helpers ──────────────────────────────────────────────────────────────

def _now_ist() -> datetime:
    return datetime.now(IST)


def _ist_datetime(d: date, h: int, m: int) -> datetime:
    """Returns an IST-aware datetime for date d at HH:MM:00."""
    return IST.localize(datetime(d.year, d.month, d.day, h, m, 0))


# ── State store ───────────────────────────────────────────────────────────────

def _save_state(data: dict) -> None:
    try:
        with open(_TMP_STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(_TMP_STATE_FILE, STATE_FILE)
        logger.debug(f"Overnight state saved: {data.get('symbol')} @ {data.get('entry_price')}")
    except Exception as e:
        logger.error(f"overnight state save error: {e}")


def _load_state() -> dict | None:
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"overnight state load error: {e}")
        return None


def _clear_state() -> None:
    for f in (STATE_FILE, _TMP_STATE_FILE):
        if os.path.exists(f):
            try:
                os.remove(f)
            except Exception:
                pass


# ── Order helpers ─────────────────────────────────────────────────────────────

async def _await_fill(order_id: str) -> Optional[dict]:
    """Poll order until filled or FILL_TIMEOUT_S elapses. Returns order dict or None."""
    deadline = time.time() + FILL_TIMEOUT_S
    async with DeltaClient() as client:
        while time.time() < deadline:
            await asyncio.sleep(5)
            try:
                o = await client.get_order(order_id)
            except Exception as e:
                logger.warning(f"Order poll error: {e}")
                continue
            s = o.get("state", "")
            if s in ("filled", "closed"):
                return o
            if s in ("cancelled", "rejected"):
                logger.error(f"Order {order_id} {s}")
                return None
    return None  # timeout


async def _close_all_positions() -> float:
    """
    Dynamic close: fetch ALL open positions from the exchange (no product_id filter),
    then market-buy each to close. Returns the fill price of the last position closed,
    or 0.0 if nothing was open or all retries failed.
    """
    if PAPER_TRADE:
        return 0.0

    deadline = time.time() + CLOSE_RETRY_S
    attempt  = 0

    while time.time() < deadline:
        attempt += 1
        try:
            async with DeltaClient() as client:
                positions = await client.get_all_open_positions()

            if not positions:
                logger.info("_close_all_positions: no open positions on exchange")
                return 0.0

            await _telegram(
                f"🔄 OVERNIGHT CLOSING — sending market buy for "
                f"{len(positions)} position(s)"
            )

            last_fill_px = 0.0
            for pos in positions:
                product_id = int(pos.get("product_id", 0))
                size       = abs(int(pos.get("size", 0)))
                if not product_id or not size:
                    continue

                logger.info(f"Closing product_id={product_id} size={size}")
                async with DeltaClient() as client:
                    order = await client.place_order(
                        product_id=product_id,
                        side="buy",
                        size=size,
                        order_type="market_order",
                    )
                order_id = str(order.get("id", ""))
                if not order_id:
                    logger.error(f"No order ID returned for close of product_id={product_id}")
                    await _telegram(
                        f"⚠️ OVERNIGHT CLOSE: no order ID for product_id={product_id} "
                        f"(size={size}). Retrying..."
                    )
                    continue

                filled = await _await_fill(order_id)
                if filled:
                    last_fill_px = float(filled.get("average_fill_price", 0) or 0)
                    logger.info(f"Closed product_id={product_id} @ ${last_fill_px:.2f}")
                else:
                    logger.warning(f"Close order timed out for product_id={product_id}")
                    await _telegram(
                        f"⚠️ OVERNIGHT CLOSE TIMEOUT: order {order_id} "
                        f"(product_id={product_id}) did not fill in {FILL_TIMEOUT_S}s. Retrying..."
                    )

            return last_fill_px

        except Exception as e:
            logger.error(f"_close_all_positions attempt {attempt}: {e} — retrying in 5s")
            await asyncio.sleep(5)

    await _telegram(
        "🚨 OVERNIGHT EMERGENCY: Failed to close all positions after 5 min.\n"
        "Manual close required on Delta Exchange immediately!"
    )
    return 0.0


# ── ATM discovery ─────────────────────────────────────────────────────────────

async def _find_atm_straddle() -> Optional[dict]:
    """
    Fetch BTC spot + today's straddle chain, return the ATM straddle dict.
    Retries up to 5 times with 30s gaps if the chain is empty.
    """
    for attempt in range(1, 6):
        try:
            async with DeltaClient() as client:
                spot      = await client.get_btc_spot()
                straddles = await client.get_today_straddles()

            if not straddles:
                logger.warning(f"ATM attempt {attempt}/5: no straddles returned from API")
            else:
                best = min(straddles, key=lambda s: (abs(s["strike"] - spot), s["strike"]))
                if best.get("mark_price", 0) > 0:
                    async with DeltaClient() as client:
                        pid = await client.get_product_id(best["symbol"])
                    logger.info(
                        f"ATM found: {best['symbol']} mark=${best['mark_price']:.2f} "
                        f"(BTC spot=${spot:,.0f})"
                    )
                    return {
                        "symbol":     best["symbol"],
                        "strike":     best["strike"],
                        "product_id": pid,
                        "mark_price": float(best["mark_price"]),
                        "spot":       float(spot),
                    }
        except Exception as e:
            logger.error(f"ATM attempt {attempt}/5 error: {e}")

        if attempt < 5:
            logger.info(f"Retrying ATM discovery in 30s")
            await asyncio.sleep(30)

    return None


# ── Entry ─────────────────────────────────────────────────────────────────────

async def _enter_trade(atm: dict) -> Optional[dict]:
    """
    Market sell ENTRY_LOTS contracts. Two attempts max.
    Returns {"order_id", "fill_price", "contracts"} or None on hard failure.
    """
    if PAPER_TRADE:
        logger.info(
            f"[PAPER] Would sell {ENTRY_LOTS} lots of {atm['symbol']} "
            f"@ ${atm['mark_price']:.2f}"
        )
        return {
            "order_id":   "PAPER",
            "fill_price": atm["mark_price"],
            "contracts":  ENTRY_LOTS,
        }

    for attempt in range(1, 3):
        async with DeltaClient() as client:
            order = await client.place_order(
                product_id=atm["product_id"],
                side="sell",
                size=ENTRY_LOTS,
                order_type="market_order",
            )
        order_id = str(order.get("id", ""))
        if not order_id:
            logger.error(f"Entry attempt {attempt}: place_order returned no ID")
            return None

        logger.info(f"Entry attempt {attempt}: placed sell order {order_id}")
        filled = await _await_fill(order_id)

        if filled:
            fill_px = float(filled.get("average_fill_price", 0) or atm["mark_price"])
            logger.info(f"Entry order {order_id} filled @ ${fill_px:.2f}")
            return {"order_id": order_id, "fill_price": fill_px, "contracts": ENTRY_LOTS}

        if attempt == 1:
            logger.warning(
                f"Entry order {order_id} not filled in {FILL_TIMEOUT_S}s — cancelling"
            )
            try:
                async with DeltaClient() as client:
                    await client.cancel_order(order_id, atm["product_id"])
            except Exception as e:
                logger.error(f"Cancel failed: {e}")
                return None   # unknown state — abort to avoid double position

    logger.error("Both entry attempts failed — aborting overnight trade")
    return None


# ── Monitor loop ──────────────────────────────────────────────────────────────

async def _monitor_until_exit(
    symbol:      str,
    entry_price: float,
    tp_usd:      int,
    sl_usd:      int,
    hard_exit:   datetime,
) -> tuple[str, float]:
    """
    Poll the straddle price every POLL_INTERVAL_S seconds (15s).
    Dual-price confirmation — matches daytime trader.py _process_price() pattern:
      - Fetches mark_price AND close (last traded price) from ticker
      - Both confirm breach  → exit immediately
      - mark_price alone     → increment breach counter; exit after 2 consecutive readings
      - Price retreats       → reset breach counters
    Exit triggers (first to fire wins):
      TP        — price decayed by tp_usd below entry  (price ≤ entry - tp_usd)
      SL        — price expanded by sl_usd above entry (price ≥ entry + sl_usd)
      HARD_EXIT — IST clock reaches hard_exit time
    Telegram alert posted on each trigger before returning.
    Returns (reason, last_known_price).
    """
    tp_target = entry_price - tp_usd
    sl_target = entry_price + sl_usd

    logger.info(
        f"Monitor: {symbol} entry=${entry_price:.2f} "
        f"TP≤${tp_target:.2f}  SL≥${sl_target:.2f}  "
        f"hard_exit={hard_exit.strftime('%H:%M IST')}"
    )

    last_price      = entry_price
    tp_breach_count = 0
    sl_breach_count = 0

    while True:
        now = _now_ist()

        # Hard exit check first — unconditional
        if now >= hard_exit:
            logger.info(f"Hard exit triggered at {now.strftime('%H:%M:%S IST')}")
            await _telegram(
                f"⏰ HARD EXIT {hard_exit.strftime('%H:%M IST')} — {symbol}\n"
                f"Last price: ${last_price:.2f}\n"
                f"Closing position..."
            )
            return "HARD_EXIT", last_price

        # Fetch mark_price + close (last traded) from ticker
        mark_price  = 0.0
        trade_price = 0.0
        try:
            async with DeltaClient() as client:
                data = await client._get(f"/v2/tickers/{symbol}")
            ticker      = data.get("result", {})
            mark_price  = float(ticker.get("mark_price", 0) or 0)
            trade_price = float(ticker.get("close", 0) or 0)   # last traded price
            if mark_price > 0:
                last_price = mark_price
        except Exception as e:
            logger.warning(f"Price poll error: {e}")

        if mark_price > 0:
            logger.debug(
                f"Poll: {symbol} mark=${mark_price:.2f}  trade=${trade_price:.2f}  "
                f"(TP≤${tp_target:.2f}  SL≥${sl_target:.2f})"
            )

            # ── TP check ─────────────────────────────────────────────────────
            tp_mark  = mark_price  <= tp_target
            tp_trade = trade_price <= tp_target and trade_price > 0

            if tp_mark and tp_trade:
                # Both prices confirm — exit immediately
                logger.info(f"TP hit (dual): mark=${mark_price:.2f}  trade=${trade_price:.2f} ≤ ${tp_target:.2f}")
                await _telegram(
                    f"🎯 TP HIT — {symbol}\n"
                    f"Mark:  ${mark_price:.2f}  Trade: ${trade_price:.2f}\n"
                    f"TP target: ≤${tp_target:.2f}\n"
                    f"Closing position..."
                )
                return "TP", mark_price
            elif tp_mark:
                # Only mark confirms — require 2 consecutive readings
                tp_breach_count += 1
                logger.info(f"TP mark-only breach #{tp_breach_count}: mark=${mark_price:.2f} ≤ ${tp_target:.2f}")
                if tp_breach_count >= 2:
                    logger.info(f"TP confirmed after {tp_breach_count} consecutive mark breaches")
                    await _telegram(
                        f"🎯 TP HIT (confirmed) — {symbol}\n"
                        f"Mark:  ${mark_price:.2f} ≤ TP ${tp_target:.2f}\n"
                        f"({tp_breach_count} consecutive readings)\n"
                        f"Closing position..."
                    )
                    return "TP", mark_price
            else:
                tp_breach_count = 0   # price retreated — reset

            # ── SL check ─────────────────────────────────────────────────────
            sl_mark  = mark_price  >= sl_target
            sl_trade = trade_price >= sl_target and trade_price > 0

            if sl_mark and sl_trade:
                # Both prices confirm — exit immediately
                logger.info(f"SL hit (dual): mark=${mark_price:.2f}  trade=${trade_price:.2f} ≥ ${sl_target:.2f}")
                await _telegram(
                    f"🛑 SL HIT — {symbol}\n"
                    f"Mark:  ${mark_price:.2f}  Trade: ${trade_price:.2f}\n"
                    f"SL target: ≥${sl_target:.2f}\n"
                    f"Closing position..."
                )
                return "SL", mark_price
            elif sl_mark:
                # Only mark confirms — require 2 consecutive readings
                sl_breach_count += 1
                logger.info(f"SL mark-only breach #{sl_breach_count}: mark=${mark_price:.2f} ≥ ${sl_target:.2f}")
                if sl_breach_count >= 2:
                    logger.info(f"SL confirmed after {sl_breach_count} consecutive mark breaches")
                    await _telegram(
                        f"🛑 SL HIT (confirmed) — {symbol}\n"
                        f"Mark:  ${mark_price:.2f} ≥ SL ${sl_target:.2f}\n"
                        f"({sl_breach_count} consecutive readings)\n"
                        f"Closing position..."
                    )
                    return "SL", mark_price
            else:
                sl_breach_count = 0   # price retreated — reset

        # Sleep until next poll, but cap at time remaining to hard exit
        secs_to_exit = max(0.0, (hard_exit - _now_ist()).total_seconds())
        await asyncio.sleep(min(POLL_INTERVAL_S, max(1.0, secs_to_exit)))


# ── Trade finalization ─────────────────────────────────────────────────────────

async def _finalize(
    symbol:      str,
    entry_price: float,
    contracts:   int,
    reason:      str,
    trigger_px:  float,
) -> None:
    """Close all positions, log the trade, send Telegram summary."""
    actual_fill = await _close_all_positions()
    exit_price  = actual_fill if actual_fill > 0 else trigger_px

    price_change = entry_price - exit_price
    pnl_usd      = round(price_change * (contracts / 1000), 4)
    exit_time    = _now_ist().isoformat()

    _trade_logger.log_exit(
        symbol      = symbol,
        exit_price  = exit_price,
        exit_reason = reason,
        pnl_usd     = pnl_usd,
        exit_time   = exit_time,
        running_min = exit_price,
    )
    _clear_state()

    icon    = "✅" if pnl_usd >= 0 else "❌"
    summary = _trade_logger.get_monthly_summary()
    paper   = "  [PAPER]" if PAPER_TRADE else ""

    msg = (
        f"{icon} OVERNIGHT CLOSED{paper}\n"
        f"Symbol: {symbol}\n"
        f"Reason: {reason}\n"
        f"Entry:  ${entry_price:.2f}\n"
        f"Exit:   ${exit_price:.2f}\n"
        f"PnL:    ${pnl_usd:+.2f}\n"
        f"\n"
        f"Month: {summary['trades']} trades  "
        f"${summary['total_pnl']:+.2f} total  "
        f"{summary['win_rate']:.0f}% WR"
    )
    await _telegram(msg)
    logger.info(f"Overnight finalized: {reason} exit=${exit_price:.2f} PnL=${pnl_usd:+.2f}")


# ── Main job (called by APScheduler) ─────────────────────────────────────────

async def run_overnight_job() -> None:
    """
    Entry point called by APScheduler at 23:50 IST on Mon/Wed/Thu/Fri.
    Runs the full overnight trade lifecycle for the following morning.
    """
    now        = _now_ist()
    trade_date = now.date() + timedelta(days=1)   # next calendar day = the IST trade day
    day_name   = trade_date.strftime("%A")         # "Tuesday", "Thursday", etc.

    if day_name not in DAY_RULES:
        logger.info(f"Overnight: no rule for {day_name} — nothing to do")
        return

    entry_h, entry_m, exit_h, exit_m, tp_usd, sl_usd = DAY_RULES[day_name]

    # ── Macro calendar check ──────────────────────────────────────────────────
    skip, skip_reason = should_skip(trade_date)
    if skip:
        logger.info(f"Overnight: macro skip {trade_date} — {skip_reason}")
        await _telegram(
            f"⏭ OVERNIGHT SKIP — {day_name} {trade_date.strftime('%d %b %Y')}\n"
            f"Reason: {skip_reason}"
        )
        return

    # ── Restart recovery ──────────────────────────────────────────────────────
    # If a previous run crashed after entry, recover the monitor from state.
    saved = _load_state()
    if saved and saved.get("active"):
        logger.info(f"Overnight: recovering position {saved['symbol']}")
        await _telegram(
            f"♻️ Overnight restarted — resuming monitor\n"
            f"Symbol: {saved['symbol']}\n"
            f"Entry: ${saved['entry_price']:.2f}"
        )
        # Derive hard_exit from the saved entry_time's calendar date
        entry_dt_ist = datetime.fromisoformat(saved["entry_time"]).astimezone(IST)
        saved_exit_h = saved.get("exit_h", 5)
        saved_exit_m = saved.get("exit_m", 30)
        hard_exit    = _ist_datetime(entry_dt_ist.date(), saved_exit_h, saved_exit_m)

        reason, trigger_px = await _monitor_until_exit(
            symbol      = saved["symbol"],
            entry_price = float(saved["entry_price"]),
            tp_usd      = int(saved["tp_usd"]),
            sl_usd      = int(saved["sl_usd"]),
            hard_exit   = hard_exit,
        )
        await _finalize(
            symbol      = saved["symbol"],
            entry_price = float(saved["entry_price"]),
            contracts   = int(saved.get("contracts", ENTRY_LOTS)),
            reason      = reason,
            trigger_px  = trigger_px,
        )
        return

    # ── Sleep until entry time ────────────────────────────────────────────────
    entry_ist = _ist_datetime(trade_date, entry_h, entry_m)
    hard_exit = _ist_datetime(trade_date, exit_h,  exit_m)

    wait_secs = (entry_ist - _now_ist()).total_seconds()
    if wait_secs > 0:
        logger.info(
            f"Overnight ({day_name}): sleeping {wait_secs:.0f}s "
            f"until {entry_ist.strftime('%H:%M IST')}"
        )
        await asyncio.sleep(wait_secs)

    # ── IV proxy filter ───────────────────────────────────────────────────────
    # Find ATM first so we can check IV before committing
    atm = await _find_atm_straddle()
    if not atm:
        await _telegram(
            f"⚠️ Overnight: No ATM straddle found at "
            f"{_now_ist().strftime('%H:%M IST')} ({day_name}). Skipping."
        )
        return

    spot      = atm["spot"]
    entry_pct = atm["mark_price"] / spot * 100 if spot > 0 else 0
    if entry_pct < IV_FLOOR_PCT:
        msg = (
            f"⏭ Overnight: IV too low — {entry_pct:.2f}% of spot "
            f"(floor={IV_FLOOR_PCT}%). Skipping {day_name}."
        )
        await _telegram(msg)
        logger.info(f"IV filter skip: {atm['symbol']} {entry_pct:.2f}%")
        return

    # ── Enter trade ───────────────────────────────────────────────────────────
    fill = await _enter_trade(atm)
    if not fill:
        await _telegram(
            f"⚠️ Overnight: Entry failed for {atm['symbol']} on {day_name}. Skipping."
        )
        return

    entry_price = fill["fill_price"]
    entry_time  = _now_ist().isoformat()

    # Persist state so a restart can resume monitoring
    _save_state({
        "active":      True,
        "symbol":      atm["symbol"],
        "strike":      atm["strike"],
        "product_id":  atm["product_id"],
        "entry_price": entry_price,
        "entry_time":  entry_time,
        "contracts":   fill["contracts"],
        "tp_usd":      tp_usd,
        "sl_usd":      sl_usd,
        "exit_h":      exit_h,
        "exit_m":      exit_m,
        "paper_trade": PAPER_TRADE,
        "order_id":    fill["order_id"],
    })

    _trade_logger.log_entry(
        symbol      = atm["symbol"],
        strike      = atm["strike"],
        entry_price = entry_price,
        contracts   = fill["contracts"],
        entry_time  = entry_time,
        paper_trade = PAPER_TRADE,
    )

    paper = "  [PAPER]" if PAPER_TRADE else ""
    await _telegram(
        f"🌙 OVERNIGHT ENTERED{paper} — {day_name}\n"
        f"Symbol:  {atm['symbol']}\n"
        f"Strike:  ${atm['strike']:,}\n"
        f"Entry:   ${entry_price:.2f}  ({entry_pct:.2f}% of spot)\n"
        f"Lots:    {fill['contracts']}\n"
        f"TP:      -${tp_usd}  (≤${entry_price - tp_usd:.2f})\n"
        f"SL:      +${sl_usd}  (≥${entry_price + sl_usd:.2f})\n"
        f"Exit:    {hard_exit.strftime('%H:%M IST')}"
    )

    # ── Monitor ───────────────────────────────────────────────────────────────
    reason, trigger_px = await _monitor_until_exit(
        symbol      = atm["symbol"],
        entry_price = entry_price,
        tp_usd      = tp_usd,
        sl_usd      = sl_usd,
        hard_exit   = hard_exit,
    )

    # ── Exit + log ────────────────────────────────────────────────────────────
    await _finalize(
        symbol      = atm["symbol"],
        entry_price = entry_price,
        contracts   = fill["contracts"],
        reason      = reason,
        trigger_px  = trigger_px,
    )
