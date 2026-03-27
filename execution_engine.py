"""
execution_engine.py
-------------------
Autonomous order execution: enter and exit straddle positions.
Handles limit→market fallback and state persistence.

No Telegram, no scheduler logic — pure order lifecycle functions.
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime

import pytz

from state_store import save_position, clear_position

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# ── Constants ─────────────────────────────────────────────────────────────────

TP_PCT  = 0.25    # 25% decay from entry (grid search optimal)
SL_MULT = 1.20    # 120% of entry       (grid search optimal)

POSITION_CONTRACTS  = int(os.getenv("POSITION_CONTRACTS",  "10"))
HALF_SIZE_CONTRACTS = int(os.getenv("HALF_SIZE_CONTRACTS", "5"))
PAPER_TRADE         = os.getenv("PAPER_TRADE", "false").lower() == "true"
ORDER_FILL_TIMEOUT  = 30     # seconds to wait for limit fill before switching to market
CONTRACT_BTC        = 0.001  # each contract = 0.001 BTC

# Cost model — matches backtest assumptions
# Delta Exchange India: ~0.05% taker fee each way + ~0.1% slippage per fill
FEE_PCT_PER_FILL    = float(os.getenv("FEE_PCT_PER_FILL",   "0.0005"))   # 0.05%
SLIPPAGE_PCT        = float(os.getenv("SLIPPAGE_PCT",        "0.001"))    # 0.1%
ROUND_TRIP_COST_PCT = (FEE_PCT_PER_FILL + SLIPPAGE_PCT) * 2              # both legs


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ExecutionResult:
    success:    bool
    fill_price: float
    contracts:  int
    order_id:   str
    error:      str  = ""
    pnl_pct:    float = 0.0   # exit only
    pnl_usd:    float = 0.0   # exit only


class ExecutionError(Exception):
    pass


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _wait_for_fill(client, order_id: str, timeout: int) -> dict | None:
    """Poll order status every 2s. Returns filled order dict or None on timeout/cancel."""
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(2)
        elapsed += 2
        try:
            order = await client.get_order(order_id)
            state = order.get("state", "")
            if state == "filled":
                return order
            if state in ("cancelled", "rejected"):
                return None
        except Exception as e:
            logger.warning(f"get_order poll failed (order {order_id}): {e}")
    return None


# ── Entry ─────────────────────────────────────────────────────────────────────

async def enter_trade(
    client,
    symbol:      str,
    strike:      int,
    product_id:  int,
    mark_price:  float,
    is_half_size: bool = False,
) -> ExecutionResult:
    """
    Short-sell a straddle. Tries limit at mark_price first; falls back to market.
    Saves position to state_store on success.
    """
    contracts = HALF_SIZE_CONTRACTS if is_half_size else POSITION_CONTRACTS
    tp_target = round(mark_price * (1 - TP_PCT), 2)
    sl_target = round(mark_price * SL_MULT, 2)

    # ── Paper trade: skip API, synthetic fill ─────────────────────────────────
    if PAPER_TRADE:
        logger.info(f"[PAPER] Enter {symbol} × {contracts} @ {mark_price}")
        save_position({
            "symbol":       symbol,
            "strike":       strike,
            "product_id":   product_id,
            "entry_price":  mark_price,
            "entry_time":   datetime.now(IST).isoformat(),
            "contracts":    contracts,
            "tp_target":    tp_target,
            "sl_target":    sl_target,
            "order_id":     "PAPER",
            "paper_trade":  True,
        })
        return ExecutionResult(
            success=True, fill_price=mark_price,
            contracts=contracts, order_id="PAPER",
        )

    # ── Live: limit → market ──────────────────────────────────────────────────
    try:
        order = await client.place_order(
            product_id=product_id,
            side="sell",
            size=contracts,
            order_type="limit_order",
            limit_price=mark_price,
        )
        order_id = str(order.get("id", ""))
        filled   = await _wait_for_fill(client, order_id, ORDER_FILL_TIMEOUT)

        if not filled:
            try:
                await client.cancel_order(order_id)
            except Exception:
                pass
            logger.warning(f"Limit not filled — switching to market for {symbol}")
            order    = await client.place_order(
                product_id=product_id, side="sell",
                size=contracts, order_type="market_order",
            )
            order_id = str(order.get("id", ""))
            filled   = await _wait_for_fill(client, order_id, 10)

        if not filled:
            raise ExecutionError(f"Could not fill entry order for {symbol}")

        fill_price = float(filled.get("average_fill_price", mark_price) or mark_price)
        tp_target  = round(fill_price * (1 - TP_PCT), 2)
        sl_target  = round(fill_price * SL_MULT, 2)

        save_position({
            "symbol":       symbol,
            "strike":       strike,
            "product_id":   product_id,
            "entry_price":  fill_price,
            "entry_time":   datetime.now(IST).isoformat(),
            "contracts":    contracts,
            "tp_target":    tp_target,
            "sl_target":    sl_target,
            "order_id":     order_id,
            "paper_trade":  False,
        })
        return ExecutionResult(
            success=True, fill_price=fill_price,
            contracts=contracts, order_id=order_id,
        )

    except ExecutionError:
        raise
    except Exception as e:
        logger.error(f"enter_trade failed: {e}", exc_info=True)
        return ExecutionResult(
            success=False, fill_price=0.0,
            contracts=contracts, order_id="", error=str(e),
        )


# ── Exit ──────────────────────────────────────────────────────────────────────

async def exit_trade(
    client,
    symbol:       str,
    product_id:   int,
    contracts:    int,
    entry_price:  float,
    reason:       str,
) -> ExecutionResult:
    """
    Buy back (close) an open short straddle.
    Limit order with 0.5% buffer above mark for fast fill; falls back to market.
    Always clears state_store, even on failure (prevent stuck position).
    """
    # ── Fetch current price ───────────────────────────────────────────────────
    try:
        ticker_data    = await client._get(f"/v2/tickers/{symbol}")
        current_price  = float(ticker_data.get("result", {}).get("mark_price", 0) or 0)
        if current_price <= 0:
            raise ExecutionError(f"Got zero price for {symbol}")
    except Exception as e:
        logger.error(f"Failed to fetch price for exit: {e}")
        clear_position()
        return ExecutionResult(
            success=False, fill_price=0.0,
            contracts=contracts, order_id="", error=str(e),
        )

    # ── Paper trade ───────────────────────────────────────────────────────────
    if PAPER_TRADE:
        logger.info(f"[PAPER] Exit {symbol} × {contracts} @ {current_price}")
        gross_pnl_pct = (entry_price - current_price) / entry_price * 100
        pnl_pct = gross_pnl_pct - ROUND_TRIP_COST_PCT * 100
        pnl_usd = (entry_price - current_price) * contracts * CONTRACT_BTC
        pnl_usd -= (entry_price + current_price) / 2 * contracts * CONTRACT_BTC * ROUND_TRIP_COST_PCT
        clear_position()
        return ExecutionResult(
            success=True, fill_price=current_price,
            contracts=contracts, order_id="PAPER",
            pnl_pct=pnl_pct, pnl_usd=pnl_usd,
        )

    # ── Live: limit with buffer → market ─────────────────────────────────────
    limit_price = round(current_price * 1.005, 2)

    try:
        order    = await client.place_order(
            product_id=product_id, side="buy",
            size=contracts, order_type="limit_order", limit_price=limit_price,
        )
        order_id = str(order.get("id", ""))
        filled   = await _wait_for_fill(client, order_id, ORDER_FILL_TIMEOUT)

        if not filled:
            try:
                await client.cancel_order(order_id)
            except Exception:
                pass
            logger.warning(f"Exit limit not filled — switching to market for {symbol}")
            order    = await client.place_order(
                product_id=product_id, side="buy",
                size=contracts, order_type="market_order",
            )
            order_id = str(order.get("id", ""))
            filled   = await _wait_for_fill(client, order_id, 10)

        if not filled:
            clear_position()
            raise ExecutionError(f"Could not fill exit order for {symbol}")

        fill_price  = float(filled.get("average_fill_price", current_price) or current_price)
        gross_pnl_pct = (entry_price - fill_price) / entry_price * 100
        pnl_pct       = gross_pnl_pct - ROUND_TRIP_COST_PCT * 100  # net of fees+slippage
        pnl_usd       = (entry_price - fill_price) * contracts * CONTRACT_BTC
        pnl_usd      -= (entry_price + fill_price) / 2 * contracts * CONTRACT_BTC * ROUND_TRIP_COST_PCT

        clear_position()
        return ExecutionResult(
            success=True, fill_price=fill_price,
            contracts=contracts, order_id=order_id,
            pnl_pct=pnl_pct, pnl_usd=pnl_usd,
        )

    except ExecutionError:
        raise
    except Exception as e:
        logger.error(f"exit_trade failed: {e}", exc_info=True)
        clear_position()  # always clear — cannot be stuck
        return ExecutionResult(
            success=False, fill_price=0.0,
            contracts=contracts, order_id="", error=str(e),
            pnl_pct=(entry_price - current_price) / entry_price * 100,
        )
