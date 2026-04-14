"""
trader.py
---------
Automated trailing-SL trader for BTC daily straddles on Delta Exchange India.

Schedule  : Mon / Wed / Thu / Sun only
Entry     : 06:00:00 IST  (discovery window 05:45–06:10, retry every 30 s)
Lots      : 100 (TRADE_LOTS env var)
Trail SL  : $150 on straddle mark price (TRAIL_DISTANCE env var)
            fires when current_price > running_min + trail_dist
            only when running_min < entry_price (straddle dipped below entry)
Hard exit : 16:30 IST — unconditional

Monitoring: WebSocket mark_price channel (primary)
            REST fallback every 60 s
            Safety close if both silent for 5 min

PnL scaling: pnl_usd = price_change × (lots / 1000)
             100 lots → actual P&L = price_change × 0.1
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytz
import websockets
from telegram import Bot

from delta_client import DeltaClient
from state_store import load_position, save_position, clear_position
from trade_logger import TradeLogger

IST = pytz.timezone("Asia/Kolkata")
logger = logging.getLogger(__name__)

# ── Config (overrideable via env vars) ────────────────────────────────────────
TRADE_DAYS      = {0, 2, 3, 6}    # Mon=0, Wed=2, Thu=3, Sun=6
DISC_START_H    = 5
DISC_START_M    = 45               # 05:45 IST — discovery window opens
DISC_END_H      = 6
DISC_END_M      = 10               # 06:10 IST — abort if still no ATM
ENTRY_H         = 6
ENTRY_M         = 0                # 06:00 IST — lock the ATM strike
DISC_RETRY_S    = 30               # retry interval inside discovery loop
FILL_TIMEOUT_S  = 60               # wait for market order fill
HARD_EXIT_H     = 16
HARD_EXIT_M     = 30               # 16:30 IST unconditional exit
REST_POLL_S     = 15               # REST fallback polling interval (straddle WS doesn't stream)
WS_FAIL_LIMIT_S = 300              # safety close after 5 min with no price update
CLOSE_RETRY_LIMIT_S = 300          # keep retrying close for up to 5 min before emergency alert
WS_URL          = "wss://socket.india.delta.exchange"
WS_HOST         = "socket.india.delta.exchange"
WS_PORT         = 443

ENTRY_LOTS     = int(os.getenv("TRADE_LOTS", "100"))
TRAIL_DISTANCE = float(os.getenv("TRAIL_DISTANCE", "150"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHANNEL_ID", "")
PAPER_TRADE    = os.getenv("PAPER_TRADE", "false").lower() == "true"

# ── Module-level state (queried by bot.py commands) ───────────────────────────
_skip_today   = False
_skip_reason  = ""
_trade_logger = TradeLogger()
# active monitor — kept alive so bot.py /position can read it
_active_monitor: Optional["TrailMonitor"] = None


# ── Telegram helper ───────────────────────────────────────────────────────────

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


def is_trade_day() -> bool:
    return _now_ist().weekday() in TRADE_DAYS


def _seconds_until_ist(h: int, m: int) -> float:
    now    = _now_ist()
    target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if target <= now:
        return 0.0
    return (target - now).total_seconds()


# ── Public skip / status API (used by bot.py commands) ───────────────────────

def skip_today(reason: str = "manual skip") -> None:
    global _skip_today, _skip_reason
    _skip_today  = True
    _skip_reason = reason
    logger.info(f"Trader: skip_today set — {reason}")


def resume_trader() -> None:
    global _skip_today, _skip_reason
    _skip_today  = False
    _skip_reason = ""
    logger.info("Trader: skip_today cleared")


def get_position_status() -> str:
    """Human-readable status for /position command."""
    mon = _active_monitor
    if mon is None or mon._stop.is_set():
        saved = load_position()
        if saved and saved.get("active"):
            return (
                f"Position: {saved['symbol']}\n"
                f"Entry: ${saved['entry_price']:.2f}\n"
                f"Running min: ${saved.get('running_min', saved['entry_price']):.2f}\n"
                f"Trail SL fires at: ${saved.get('running_min', saved['entry_price']) + TRAIL_DISTANCE:.2f}\n"
                f"Status: monitor not running (bot may have restarted)"
            )
        return "No active trader position."

    trail_level = mon.running_min + mon.trail_dist
    return (
        f"Position: {mon.symbol}\n"
        f"Entry: ${mon.entry_price:.2f}\n"
        f"Running min: ${mon.running_min:.2f}\n"
        f"Trail SL fires at: ${trail_level:.2f}\n"
        f"Lots: {mon.contracts}\n"
        f"Last update: {int(time.time() - mon._last_update)}s ago"
    )


# ── ATM discovery ─────────────────────────────────────────────────────────────

async def _try_find_atm() -> Optional[dict]:
    """Single attempt: fetch BTC spot + straddle chain, return ATM dict or None."""
    async with DeltaClient() as client:
        spot      = await client.get_btc_spot()
        straddles = await client.get_today_straddles()

    if not straddles or not spot:
        return None

    # Closest strike; on equidistant prefer the lower strike
    best = min(straddles, key=lambda s: (abs(s["strike"] - spot), s["strike"]))
    if not best.get("mark_price", 0):
        return None

    async with DeltaClient() as client:
        pid = await client.get_product_id(best["symbol"])

    return {
        "symbol":     best["symbol"],
        "strike":     best["strike"],
        "product_id": pid,
        "mark_price": float(best["mark_price"]),
    }


async def find_atm_straddle() -> Optional[dict]:
    """
    Discovery loop: sleeps until 05:45, then waits for 06:00 IST to lock the ATM.
    If 06:00 fetch fails, retries every 30 s until 06:10.
    Returns ATM dict or None if nothing found by 06:10.
    """
    # Sleep until discovery window opens (05:45 IST)
    wait = _seconds_until_ist(DISC_START_H, DISC_START_M)
    if wait > 0:
        logger.info(f"Waiting {wait:.0f}s until discovery window (05:45 IST)")
        await asyncio.sleep(wait)

    # Sleep until exactly 06:00:00 IST
    wait_to_entry = _seconds_until_ist(ENTRY_H, ENTRY_M)
    if wait_to_entry > 0:
        logger.info(f"Discovery window open. Sleeping {wait_to_entry:.0f}s until 06:00 IST")
        await asyncio.sleep(wait_to_entry)

    # Retry loop: 06:00 → 06:10
    disc_end = _now_ist().replace(
        hour=DISC_END_H, minute=DISC_END_M, second=0, microsecond=0
    )

    while _now_ist() < disc_end:
        try:
            atm = await _try_find_atm()
            if atm:
                logger.info(f"ATM locked at {_now_ist().strftime('%H:%M:%S')} IST: "
                            f"{atm['symbol']} mark=${atm['mark_price']:.2f}")
                return atm
        except Exception as e:
            logger.warning(f"ATM discovery error: {e}")

        remaining = (disc_end - _now_ist()).total_seconds()
        if remaining <= 0:
            break
        logger.warning(f"ATM not found, retrying in {DISC_RETRY_S}s "
                       f"({remaining:.0f}s remaining in window)")
        await asyncio.sleep(min(DISC_RETRY_S, remaining))

    logger.error("ATM discovery failed by 06:10 IST — aborting today")
    return None


# ── Order execution ───────────────────────────────────────────────────────────

async def _await_fill(order_id: str, timeout_s: float) -> Optional[dict]:
    """Poll order state until filled, cancelled, or timeout. Returns order dict or None."""
    deadline = time.time() + timeout_s
    async with DeltaClient() as client:
        while time.time() < deadline:
            await asyncio.sleep(5)
            try:
                o = await client.get_order(order_id)
            except Exception as e:
                logger.warning(f"Order poll error: {e}")
                continue
            state = o.get("state", "")
            if state in ("filled", "closed"):   # Delta India uses "closed" for filled
                return o
            if state in ("cancelled", "rejected"):
                logger.error(f"Order {order_id} {state}")
                return None
    return None  # timeout


async def enter_trade(atm: dict) -> Optional[dict]:
    """
    Market sell ENTRY_LOTS contracts.
    On partial fill / timeout: cancel + retry once as a fresh market order.
    Returns {"order_id", "fill_price", "contracts"} or None on hard failure.
    """
    if PAPER_TRADE:
        logger.info(f"[PAPER] Would sell {ENTRY_LOTS} lots of {atm['symbol']} "
                    f"@ ${atm['mark_price']:.2f}")
        return {
            "order_id":   "PAPER",
            "fill_price": atm["mark_price"],
            "contracts":  ENTRY_LOTS,
        }

    for attempt in range(1, 3):  # max 2 attempts
        async with DeltaClient() as client:
            order = await client.place_order(
                product_id=atm["product_id"],
                side="sell",
                size=ENTRY_LOTS,
                order_type="market_order",
            )
        order_id = str(order.get("id", ""))
        if not order_id:
            logger.error(f"Attempt {attempt}: place_order returned no ID: {order}")
            return None

        logger.info(f"Attempt {attempt}: placed sell order {order_id} "
                    f"({ENTRY_LOTS} lots of {atm['symbol']})")

        filled = await _await_fill(order_id, FILL_TIMEOUT_S)

        if filled:
            fill_px = float(filled.get("average_fill_price", 0) or atm["mark_price"])
            logger.info(f"Order {order_id} filled @ ${fill_px:.2f}")
            return {"order_id": order_id, "fill_price": fill_px, "contracts": ENTRY_LOTS}

        # Timeout on first attempt — cancel then verify no position before retry
        if attempt == 1:
            logger.warning(f"Order {order_id} not filled in {FILL_TIMEOUT_S}s — cancelling")
            try:
                async with DeltaClient() as client:
                    await client.cancel_order(order_id, atm["product_id"])
            except Exception as e:
                logger.error(f"Cancel failed: {e}")

            # Guard: confirm sub-account has no open position before placing attempt 2.
            # If the cancel failed and the order eventually filled, we'd double up.
            try:
                async with DeltaClient() as client:
                    pos = await client.get_position(atm["product_id"])
                if pos and int(pos.get("size", 0)) != 0:
                    logger.error(
                        f"Position already open ({pos.get('size')} lots) after cancel attempt — "
                        "aborting retry to avoid doubling. Manual review required."
                    )
                    await _telegram(
                        f"⚠️ ENTRY ABORT: cancel timed out and position is already open "
                        f"({pos.get('size')} lots of {atm['symbol']}). "
                        f"Manual review required."
                    )
                    return None
            except Exception as e:
                logger.warning(f"Position check before retry failed: {e} — aborting to be safe")
                return None

    logger.error("Both entry attempts failed — aborting trade")
    return None


async def close_position(product_id: int, symbol: str, contracts: int) -> float:
    """
    Market buy to close short. Retries every FILL_TIMEOUT_S until confirmed filled
    or CLOSE_RETRY_LIMIT_S has elapsed, then sends an emergency Telegram alert.
    Returns confirmed fill price, or 0.0 if all retries exhausted.
    """
    if PAPER_TRADE:
        async with DeltaClient() as client:
            data = await client._get(f"/v2/tickers/{symbol}")
        return float(data.get("result", {}).get("mark_price", 0) or 0)

    deadline = time.time() + CLOSE_RETRY_LIMIT_S
    attempt  = 0

    while time.time() < deadline:
        attempt += 1
        try:
            async with DeltaClient() as client:
                order = await client.place_order(
                    product_id=product_id,
                    side="buy",
                    size=contracts,
                    order_type="market_order",
                )
            order_id = str(order.get("id", ""))
            if not order_id:
                logger.error(f"close_position attempt {attempt}: no order ID — retrying in 5s")
                await asyncio.sleep(5)
                continue

            logger.info(f"Close attempt {attempt}: order {order_id} (buy {contracts} lots)")
            filled = await _await_fill(order_id, FILL_TIMEOUT_S)
            if filled:
                fill_px = float(filled.get("average_fill_price", 0) or 0)
                logger.info(f"Close confirmed @ ${fill_px:.2f} (attempt {attempt})")
                return fill_px

            logger.warning(f"Close attempt {attempt} timed out — retrying immediately")

        except Exception as e:
            logger.error(f"Close attempt {attempt} error: {e} — retrying in 5s")
            await asyncio.sleep(5)

    # All retries exhausted — position may still be open
    logger.critical(
        f"FAILED to close {contracts} lots of {symbol} after {CLOSE_RETRY_LIMIT_S}s — MANUAL ACTION REQUIRED"
    )
    await _telegram(
        f"🚨 EMERGENCY: Failed to close {contracts} lots of {symbol} "
        f"after {CLOSE_RETRY_LIMIT_S}s of retries.\n"
        f"Position may still be OPEN on Delta Exchange.\n"
        f"MANUAL CLOSE REQUIRED IMMEDIATELY."
    )
    return 0.0


# ── Trail monitor ─────────────────────────────────────────────────────────────

class TrailMonitor:
    """
    Monitors mark price via WebSocket (primary) + REST poll (fallback).
    Fires trail SL when: current_price > running_min + trail_dist
                         AND running_min < entry_price (price dipped below entry)

    After .run() returns, read ._exit_reason and ._exit_price.
    """

    def __init__(
        self,
        symbol:      str,
        product_id:  int,
        entry_price: float,
        contracts:   int,
        trail_dist:  float,
        running_min: Optional[float] = None,
    ):
        self.symbol      = symbol
        self.product_id  = product_id
        self.entry_price = entry_price
        self.contracts   = contracts
        self.trail_dist  = trail_dist
        self.running_min = running_min if running_min is not None else entry_price
        self._stop       = asyncio.Event()
        self._last_update = time.time()
        self._exit_reason: Optional[str] = None
        self._exit_price:  Optional[float] = None

    async def run(self) -> tuple[str, Optional[float]]:
        """Blocks until trail fires, hard exit, or safety close."""
        global _active_monitor
        _active_monitor = self

        hard_exit_ist = _now_ist().replace(
            hour=HARD_EXIT_H, minute=HARD_EXIT_M, second=0, microsecond=0
        )

        tasks = [
            asyncio.create_task(self._ws_loop(),          name="ws_loop"),
            asyncio.create_task(self._rest_loop(),         name="rest_loop"),
            asyncio.create_task(self._watchdog(),          name="watchdog"),
            asyncio.create_task(self._hard_exit_timer(hard_exit_ist), name="hard_exit"),
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        # Cancel siblings
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        _active_monitor = None
        return self._exit_reason or "UNKNOWN", self._exit_price

    # ── Price processing ──────────────────────────────────────────────────────

    async def _process_price(self, price: float) -> None:
        self._last_update = time.time()
        if price <= 0:
            return

        if price < self.running_min:
            self.running_min = price
            self._persist()

        # Trail fires only if price has dipped below entry
        if self.running_min < self.entry_price:
            trail_level = self.running_min + self.trail_dist
            if price >= trail_level:
                logger.info(
                    f"TRAIL FIRED: px={price:.2f} >= trail={trail_level:.2f} "
                    f"(running_min={self.running_min:.2f}, entry={self.entry_price:.2f})"
                )
                self._exit_reason = "TRAIL"
                self._exit_price  = price
                self._stop.set()

    def _persist(self) -> None:
        try:
            pos = load_position()
            if pos:
                pos["running_min"] = self.running_min
                save_position(pos)
        except Exception as e:
            logger.debug(f"State persist error: {e}")

    # ── WebSocket loop ────────────────────────────────────────────────────────

    async def _ws_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._ws_session()
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._stop.is_set():
                    break
                logger.warning(f"WebSocket error: {e} — reconnecting in 5s")
                await asyncio.sleep(5)

    async def _ws_session(self) -> None:
        import ssl as _ssl
        ssl_ctx   = _ssl.create_default_context()
        proxy_url = os.getenv("QUOTAGUARDSTATIC_URL", "")

        if proxy_url:
            # Route WebSocket through QuotaGuard static-IP proxy so Delta's
            # IP whitelist is satisfied on Render (which has no fixed IP).
            from python_socks.async_.asyncio import Proxy
            proxy = Proxy.from_url(proxy_url, rdns=True)
            sock  = await proxy.connect(
                dest_host=WS_HOST,
                dest_port=WS_PORT,
                timeout=15,
            )
            extra = {"sock": sock}
        else:
            extra = {}

        async with websockets.connect(
            WS_URL,
            ssl=ssl_ctx,
            ping_interval=20,
            ping_timeout=10,
            open_timeout=15,
            **extra,
        ) as ws:
            sub = json.dumps({
                "type": "subscribe",
                "payload": {
                    "channels": [{"name": "mark_price", "symbols": [self.symbol]}]
                }
            })
            await ws.send(sub)
            logger.info(f"WebSocket subscribed: mark_price/{self.symbol} (pid={self.product_id})")

            async for raw in ws:
                if self._stop.is_set():
                    return
                try:
                    msg = json.loads(raw)
                    msg_type = msg.get("type", "")

                    # Skip control messages
                    if msg_type in ("subscriptions", "heartbeat", "info", "ping"):
                        continue

                    # Delta Exchange sends type="mark_price" with product_id + price
                    if msg_type == "mark_price":
                        if int(msg.get("product_id", -1)) != self.product_id:
                            continue   # different product on same channel
                        price = float(msg.get("price", 0) or 0)
                        if price > 0:
                            await self._process_price(price)
                except Exception as e:
                    logger.debug(f"WS parse error: {e}")

    # ── REST fallback ─────────────────────────────────────────────────────────

    async def _rest_loop(self) -> None:
        """Poll REST every REST_POLL_S seconds as a safety net."""
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    asyncio.shield(asyncio.sleep(REST_POLL_S)),
                    timeout=REST_POLL_S + 5,
                )
            except (asyncio.CancelledError, asyncio.TimeoutError):
                break
            if self._stop.is_set():
                break
            try:
                async with DeltaClient() as client:
                    data = await client._get(f"/v2/tickers/{self.symbol}")
                price = float(data.get("result", {}).get("mark_price", 0) or 0)
                if price > 0:
                    logger.debug(f"REST fallback price: ${price:.2f}")
                    await self._process_price(price)
            except Exception as e:
                logger.warning(f"REST fallback error: {e}")

    # ── Failure watchdog ──────────────────────────────────────────────────────

    async def _watchdog(self) -> None:
        """Safety close if no price update received for WS_FAIL_LIMIT_S seconds."""
        while not self._stop.is_set():
            await asyncio.sleep(30)
            if self._stop.is_set():
                break
            silent_for = time.time() - self._last_update
            if silent_for > WS_FAIL_LIMIT_S:
                logger.error(
                    f"No price update for {silent_for:.0f}s — triggering safety close"
                )
                self._exit_reason = "SAFETY_CLOSE"
                self._exit_price  = None
                self._stop.set()
                return

    # ── Hard exit timer ───────────────────────────────────────────────────────

    async def _hard_exit_timer(self, hard_exit_ist: datetime) -> None:
        wait = (hard_exit_ist - _now_ist()).total_seconds()
        if wait > 0:
            try:
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                return
        if not self._stop.is_set():
            logger.info("Hard exit triggered: 16:30 IST")
            self._exit_reason = "HARD_EXIT"
            self._exit_price  = None
            self._stop.set()


# ── Restart recovery ──────────────────────────────────────────────────────────

async def _resume_from_state(saved: dict) -> None:
    """
    Called on bot restart when a position is found in state_store.
    Re-attaches the trail monitor to an existing position.
    """
    logger.info(f"Resuming monitor for {saved['symbol']} "
                f"(running_min=${saved.get('running_min', saved['entry_price']):.2f})")
    await _telegram(
        f"♻️ Trader restarted — resuming trail monitor\n"
        f"Symbol: {saved['symbol']}\n"
        f"Entry: ${saved['entry_price']:.2f}\n"
        f"Running min: ${saved.get('running_min', saved['entry_price']):.2f}"
    )

    monitor = TrailMonitor(
        symbol      = saved["symbol"],
        product_id  = int(saved["product_id"]),
        entry_price = float(saved["entry_price"]),
        contracts   = int(saved.get("contracts", ENTRY_LOTS)),
        trail_dist  = float(saved.get("trail_distance", TRAIL_DISTANCE)),
        running_min = float(saved.get("running_min", saved["entry_price"])),
    )

    reason, exit_px = await monitor.run()
    await _finalize_trade(
        symbol     = saved["symbol"],
        product_id = int(saved["product_id"]),
        contracts  = int(saved.get("contracts", ENTRY_LOTS)),
        entry_price = float(saved["entry_price"]),
        monitor    = monitor,
        reason     = reason,
        exit_px    = exit_px,
    )


# ── Trade finalization (shared by normal flow and resume) ─────────────────────

async def _finalize_trade(
    symbol:      str,
    product_id:  int,
    contracts:   int,
    entry_price: float,
    monitor:     TrailMonitor,
    reason:      str,
    exit_px:     Optional[float],
) -> None:
    # If price unknown, fetch current mark price
    if exit_px is None or exit_px <= 0:
        try:
            async with DeltaClient() as client:
                data = await client._get(f"/v2/tickers/{symbol}")
            exit_px = float(data.get("result", {}).get("mark_price", 0) or entry_price)
        except Exception:
            exit_px = entry_price

    # Close the position
    actual_exit_px = await close_position(product_id, symbol, contracts)
    if actual_exit_px > 0:
        exit_px = actual_exit_px  # use confirmed fill price

    # pnl: straddle sold at entry_price, bought back at exit_px
    # scale: 1000 lots = 1 BTC notional → 100 lots = 0.1 scaling factor
    price_change = entry_price - exit_px
    pnl_usd      = round(price_change * (contracts / 1000), 4)
    exit_time    = _now_ist().isoformat()

    # Log trade
    _trade_logger.log_exit(
        symbol      = symbol,
        exit_price  = exit_px,
        exit_reason = reason,
        pnl_usd     = pnl_usd,
        exit_time   = exit_time,
        running_min = monitor.running_min,
    )

    # Clear persisted state
    clear_position()

    # Telegram alert
    icon = "✅" if pnl_usd >= 0 else "❌"
    summary = _trade_logger.get_monthly_summary()
    paper_tag = "  [PAPER TRADE]" if PAPER_TRADE else ""
    msg = (
        f"{icon} POSITION CLOSED{paper_tag}\n"
        f"Symbol: {symbol}\n"
        f"Reason: {reason}\n"
        f"Entry:  ${entry_price:.2f}\n"
        f"Exit:   ${exit_px:.2f}\n"
        f"Min:    ${monitor.running_min:.2f}\n"
        f"PnL:    ${pnl_usd:+.2f}\n"
        f"\n"
        f"Month so far: {summary['trades']} trades, "
        f"${summary['total_pnl']:+.2f} total, "
        f"{summary['win_rate']:.0f}% WR"
    )
    await _telegram(msg)
    logger.info(f"Trade finalized: {reason} @ ${exit_px:.2f}, PnL=${pnl_usd:+.2f}")


# ── Main trade job (called by scheduler) ─────────────────────────────────────

async def run_trade_job() -> None:
    """
    Entry point invoked by the APScheduler job at 05:45 IST.
    Handles the full lifecycle: discover → enter → monitor → exit.
    """
    global _skip_today

    now = _now_ist()

    # Day-of-week guard (belt + suspenders — cron also restricts days)
    if not is_trade_day():
        logger.debug(f"Trader: not a trade day ({now.strftime('%A')})")
        return

    if _skip_today:
        logger.info(f"Trader: skip_today is set ({_skip_reason}), skipping")
        _skip_today = False   # auto-reset for next day
        return

    # Restart recovery: if there's an active persisted position, resume monitoring
    saved = load_position()
    if saved and saved.get("active"):
        logger.info("Found active position from previous session — resuming monitor")
        await _resume_from_state(saved)
        return

    # ── 1. Find ATM straddle ──────────────────────────────────────────────────
    atm = await find_atm_straddle()
    if not atm:
        await _telegram(
            f"⚠️ Trader: No ATM straddle found by 06:10 IST "
            f"on {now.strftime('%a %d %b %Y')}. Skipping today."
        )
        return

    # ── 2. Enter trade ────────────────────────────────────────────────────────
    fill = await enter_trade(atm)
    if not fill:
        await _telegram(
            f"⚠️ Trader: Order entry failed for {atm['symbol']}. Skipping today."
        )
        return

    entry_price = fill["fill_price"]
    entry_time  = _now_ist().isoformat()

    # Persist position state (survives restarts)
    save_position({
        "active":          True,
        "symbol":          atm["symbol"],
        "strike":          atm["strike"],
        "product_id":      atm["product_id"],
        "entry_price":     entry_price,
        "entry_time":      entry_time,
        "contracts":       fill["contracts"],
        "running_min":     entry_price,
        "trail_distance":  TRAIL_DISTANCE,
        "paper_trade":     PAPER_TRADE,
        "order_id":        fill["order_id"],
        # Pad unused fields that state_store.py documents (harmless extras)
        "tp_target":       0,
        "sl_target":       0,
    })

    # Log entry
    _trade_logger.log_entry(
        symbol      = atm["symbol"],
        strike      = atm["strike"],
        entry_price = entry_price,
        contracts   = fill["contracts"],
        entry_time  = entry_time,
        paper_trade = PAPER_TRADE,
    )

    # Entry Telegram alert
    paper_tag = "  [PAPER TRADE]" if PAPER_TRADE else ""
    await _telegram(
        f"📊 SHORT STRADDLE ENTERED{paper_tag}\n"
        f"Symbol:  {atm['symbol']}\n"
        f"Strike:  ${atm['strike']:,}\n"
        f"Entry:   ${entry_price:.2f}\n"
        f"Lots:    {fill['contracts']}\n"
        f"Trail:   $+{TRAIL_DISTANCE:.0f} above running min\n"
        f"Hard exit: 16:30 IST"
    )

    # ── 3. Monitor ────────────────────────────────────────────────────────────
    monitor = TrailMonitor(
        symbol      = atm["symbol"],
        product_id  = atm["product_id"],
        entry_price = entry_price,
        contracts   = fill["contracts"],
        trail_dist  = TRAIL_DISTANCE,
        running_min = entry_price,
    )

    reason, exit_px = await monitor.run()

    # ── 4. Exit + log ─────────────────────────────────────────────────────────
    await _finalize_trade(
        symbol      = atm["symbol"],
        product_id  = atm["product_id"],
        contracts   = fill["contracts"],
        entry_price = entry_price,
        monitor     = monitor,
        reason      = reason,
        exit_px     = exit_px,
    )
