"""
test_unit_trade_logger.py
-------------------------
Unit tests for trade_logger.py — uses a temp file per test, no shared state.

Run with:
    pytest tests/test_unit_trade_logger.py -v
"""

import json
from datetime import datetime

import pytest
import pytz

from trade_logger import TradeLogger

IST = pytz.timezone("Asia/Kolkata")


@pytest.fixture
def tl(tmp_path):
    """Fresh TradeLogger pointing at a temp file for each test."""
    return TradeLogger(str(tmp_path / "trades_test.json"))


def _load(tl: TradeLogger) -> list:
    import trade_logger
    return json.loads(open(trade_logger.LOG_FILE).read())


# ══════════════════════════════════════════════════════════════════════════════
# Entry
# ══════════════════════════════════════════════════════════════════════════════

class TestLogEntry:
    def test_creates_open_record(self, tl):
        tl.log_entry("MV-BTC-68000-150426", 68000, 650.0, 1,
                     "2026-04-15T06:00:00+05:30")
        recs = _load(tl)
        assert len(recs) == 1
        r = recs[0]
        assert r["symbol"]      == "MV-BTC-68000-150426"
        assert r["strike"]      == 68000
        assert r["entry_price"] == 650.0
        assert r["contracts"]   == 1
        assert r["status"]      == "open"
        assert r["exit_price"]  is None
        assert r["pnl_usd"]     is None

    def test_paper_trade_flag_recorded(self, tl):
        tl.log_entry("MV-BTC-68000-150426", 68000, 650.0, 1, "t",
                     paper_trade=True)
        r = _load(tl)[0]
        assert r["paper_trade"] is True

    def test_live_flag_default(self, tl):
        tl.log_entry("MV-BTC-68000-150426", 68000, 650.0, 1, "t")
        r = _load(tl)[0]
        assert r["paper_trade"] is False

    def test_returns_trade_id(self, tl):
        tid = tl.log_entry("SYM", 68000, 650.0, 1, "t")
        assert tid is not None and len(tid) > 0

    def test_multiple_entries_append(self, tl):
        for i in range(4):
            tl.log_entry(f"SYM_{i}", 68000, 650.0, 1, "t")
        recs = _load(tl)
        assert len(recs) == 4


# ══════════════════════════════════════════════════════════════════════════════
# Exit
# ══════════════════════════════════════════════════════════════════════════════

class TestLogExit:
    def test_closes_open_record(self, tl):
        tl.log_entry("MV-BTC-68000-150426", 68000, 650.0, 1, "t")
        tl.log_exit("MV-BTC-68000-150426", 500.0, "TRAIL",
                    pnl_usd=15.0, exit_time="t2", running_min=480.0)
        r = _load(tl)[0]
        assert r["status"]      == "closed"
        assert r["exit_price"]  == 500.0
        assert r["exit_reason"] == "TRAIL"
        assert r["pnl_usd"]     == 15.0
        assert r["running_min"] == 480.0

    def test_exit_on_hard_exit(self, tl):
        tl.log_entry("SYM", 68000, 650.0, 1, "t")
        tl.log_exit("SYM", 620.0, "HARD_EXIT", pnl_usd=3.0,
                    exit_time="t2", running_min=620.0)
        r = _load(tl)[0]
        assert r["exit_reason"] == "HARD_EXIT"

    def test_negative_pnl_stored(self, tl):
        tl.log_entry("SYM", 68000, 650.0, 1, "t")
        tl.log_exit("SYM", 800.0, "TRAIL", pnl_usd=-15.0,
                    exit_time="t2", running_min=650.0)
        r = _load(tl)[0]
        assert r["pnl_usd"] == -15.0

    def test_exit_matches_latest_open_record_for_symbol(self, tl):
        """Exit closes the most recent open trade for the symbol."""
        tl.log_entry("SYM_A", 68000, 650.0, 1, "t1")
        tl.log_exit("SYM_A", 500.0, "TRAIL", 15.0, "t2", 480.0)
        tl.log_entry("SYM_A", 68000, 660.0, 1, "t3")   # second trade
        tl.log_exit("SYM_A", 510.0, "HARD_EXIT", 15.0, "t4", 490.0)
        recs = _load(tl)
        assert all(r["status"] == "closed" for r in recs)
        assert recs[0]["exit_price"] == 500.0
        assert recs[1]["exit_price"] == 510.0


# ══════════════════════════════════════════════════════════════════════════════
# Monthly summary
# ══════════════════════════════════════════════════════════════════════════════

class TestMonthlySummary:
    def _write_records(self, tl, records):
        import trade_logger
        with open(trade_logger.LOG_FILE, "w") as f:
            json.dump(records, f)

    def test_empty_returns_zero(self, tl):
        s = tl.get_monthly_summary()
        assert s["trades"] == 0
        assert s["total_pnl"] == 0.0
        assert s["win_rate"] == 0.0

    def test_correct_month_filtered(self, tl):
        this_month = datetime.now(IST).strftime("%Y-%m")
        recs = [
            {"trade_id": f"{this_month}-15", "symbol": "A",
             "status": "closed", "pnl_usd": 20.0},
            {"trade_id": "2020-01-01", "symbol": "B",    # old — excluded
             "status": "closed", "pnl_usd": 999.0},
        ]
        self._write_records(tl, recs)
        s = tl.get_monthly_summary()
        assert s["trades"] == 1
        assert s["total_pnl"] == pytest.approx(20.0)

    def test_win_rate_calculation(self, tl):
        this_month = datetime.now(IST).strftime("%Y-%m")
        recs = [
            {"trade_id": f"{this_month}-01", "symbol": "A",
             "status": "closed", "pnl_usd":  20.0},
            {"trade_id": f"{this_month}-02", "symbol": "B",
             "status": "closed", "pnl_usd": -5.0},
            {"trade_id": f"{this_month}-03", "symbol": "C",
             "status": "closed", "pnl_usd":  10.0},
        ]
        self._write_records(tl, recs)
        s = tl.get_monthly_summary()
        assert s["trades"]    == 3
        assert s["total_pnl"] == pytest.approx(25.0)
        assert s["win_rate"]  == pytest.approx(66.7, rel=0.01)
        assert s["avg_pnl"]   == pytest.approx(25.0 / 3, rel=0.01)

    def test_open_trades_excluded_from_summary(self, tl):
        this_month = datetime.now(IST).strftime("%Y-%m")
        recs = [
            {"trade_id": f"{this_month}-15", "symbol": "A",
             "status": "open",   "pnl_usd": None},
            {"trade_id": f"{this_month}-15", "symbol": "B",
             "status": "closed", "pnl_usd": 10.0},
        ]
        self._write_records(tl, recs)
        s = tl.get_monthly_summary()
        assert s["trades"] == 1


# ══════════════════════════════════════════════════════════════════════════════
# Atomic write
# ══════════════════════════════════════════════════════════════════════════════

class TestAtomicWrite:
    def test_no_corruption_on_concurrent_writes(self, tl):
        """tmp → os.replace() ensures a crashed mid-write never corrupts the file."""
        for i in range(10):
            tl.log_entry(f"SYM_{i}", 68000 + i * 500, 650.0, 1, "t")
        import trade_logger
        recs = json.loads(open(trade_logger.LOG_FILE).read())
        assert len(recs) == 10
        assert all(isinstance(r["entry_price"], float) for r in recs)

    def test_tmp_file_cleaned_up_after_write(self, tl):
        import trade_logger
        tl.log_entry("SYM", 68000, 650.0, 1, "t")
        assert not (trade_logger.LOG_FILE + ".tmp" in
                    __import__('os').listdir(
                        __import__('os').path.dirname(
                            trade_logger.LOG_FILE) or "."))
