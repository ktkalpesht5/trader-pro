"""
state_store.py
--------------
Persistent position state using atomic JSON writes.
Survives bot restarts — used to recover open positions after a crash or redeploy.
"""

import json
import os
import logging

logger = logging.getLogger(__name__)

STATE_FILE = "position.json"
_TMP_FILE  = "position.json.tmp"


def load_position() -> dict | None:
    """Returns persisted position dict if one exists, else None."""
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load position state: {e}")
        return None


def save_position(data: dict) -> None:
    """
    Atomically write position state to disk.
    Uses tmp → os.replace() so a crash mid-write never corrupts the state file.

    Expected keys:
        symbol, strike, product_id, entry_price, entry_time,
        contracts, tp_target, sl_target, order_id, paper_trade
    """
    try:
        with open(_TMP_FILE, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(_TMP_FILE, STATE_FILE)
        logger.debug(f"Position state saved: {data['symbol']} @ {data['entry_price']}")
    except Exception as e:
        logger.error(f"Failed to save position state: {e}")
        raise


def clear_position() -> None:
    """Delete the state file. Called after exit is confirmed."""
    try:
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
        if os.path.exists(_TMP_FILE):
            os.remove(_TMP_FILE)
        logger.debug("Position state cleared")
    except Exception as e:
        logger.error(f"Failed to clear position state: {e}")
