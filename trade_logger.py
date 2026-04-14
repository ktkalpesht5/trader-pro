"""
trade_logger.py
---------------
Append-only JSON trade journal for the automated trailing-SL trader.
Writes one record per trade to trades.json for monthly analysis.
"""

import json
import logging
import os
from datetime import datetime

import pytz

logger = logging.getLogger(__name__)

IST       = pytz.timezone("Asia/Kolkata")
LOG_FILE  = "trades.json"
_TMP_FILE = "trades.json.tmp"


def _load_all() -> list:
    if not os.path.exists(LOG_FILE):
        return []
    try:
        with open(LOG_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []


def _save_all(records: list) -> None:
    with open(_TMP_FILE, "w") as f:
        json.dump(records, f, indent=2)
    os.replace(_TMP_FILE, LOG_FILE)


class TradeLogger:
    """Append-only trade journal stored as a flat JSON array."""

    def __init__(self, log_file: str = LOG_FILE):
        global LOG_FILE, _TMP_FILE
        LOG_FILE  = log_file
        _TMP_FILE = log_file + ".tmp"
        self._current_id: str | None = None

    def log_entry(
        self,
        symbol:     str,
        strike:     int,
        entry_price: float,
        contracts:  int,
        entry_time: str,
        paper_trade: bool = False,
    ) -> str:
        """
        Record a new entry. Returns a trade_id (date string).
        Call log_exit() later to complete the record.
        """
        trade_id = datetime.now(IST).strftime("%Y-%m-%d")
        record = {
            "trade_id":    trade_id,
            "symbol":      symbol,
            "strike":      strike,
            "contracts":   contracts,
            "entry_price": entry_price,
            "entry_time":  entry_time,
            "exit_price":  None,
            "exit_time":   None,
            "exit_reason": None,
            "running_min": None,
            "pnl_usd":     None,
            "paper_trade": paper_trade,
            "status":      "open",
        }
        records = _load_all()
        records.append(record)
        _save_all(records)
        self._current_id = trade_id
        logger.info(f"TradeLogger: entry logged — {symbol} @ ${entry_price:.2f}")
        return trade_id

    def log_exit(
        self,
        symbol:      str,
        exit_price:  float,
        exit_reason: str,
        pnl_usd:     float,
        exit_time:   str,
        running_min: float,
    ) -> None:
        """Update the most recent open trade record with exit details."""
        records = _load_all()
        for rec in reversed(records):
            if rec.get("symbol") == symbol and rec.get("status") == "open":
                rec["exit_price"]  = exit_price
                rec["exit_time"]   = exit_time
                rec["exit_reason"] = exit_reason
                rec["running_min"] = running_min
                rec["pnl_usd"]     = round(pnl_usd, 4)
                rec["status"]      = "closed"
                break
        _save_all(records)
        logger.info(f"TradeLogger: exit logged — {symbol} @ ${exit_price:.2f} "
                    f"({exit_reason}) PnL=${pnl_usd:+.2f}")

    def get_monthly_summary(self) -> dict:
        """Returns a summary dict for the current calendar month."""
        records = _load_all()
        now_ist = datetime.now(IST)
        month_key = now_ist.strftime("%Y-%m")
        closed = [r for r in records
                  if r.get("status") == "closed"
                  and r.get("trade_id", "").startswith(month_key)]

        if not closed:
            return {"month": month_key, "trades": 0, "total_pnl": 0.0, "win_rate": 0.0}

        total_pnl = sum(r["pnl_usd"] for r in closed if r["pnl_usd"] is not None)
        wins      = sum(1 for r in closed if (r["pnl_usd"] or 0) > 0)
        return {
            "month":     month_key,
            "trades":    len(closed),
            "total_pnl": round(total_pnl, 2),
            "win_rate":  round(wins / len(closed) * 100, 1) if closed else 0.0,
            "avg_pnl":   round(total_pnl / len(closed), 2) if closed else 0.0,
        }
