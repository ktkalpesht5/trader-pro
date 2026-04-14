"""
test_unit_trader.py
-------------------
Unit tests for trader.py — no real API calls, no real orders.
All external I/O is mocked.

Run with:
    pytest tests/test_unit_trader.py -v
"""

import asyncio
import json
import os
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, call
import pytest
import pytz

# Project root already on path via conftest.py
import trader
from trader import (
    TrailMonitor,
    is_trade_day,
    skip_today,
    resume_trader,
    get_position_status,
    TRAIL_DISTANCE,
    HARD_EXIT_H,
    HARD_EXIT_M,
)
from tests.conftest import make_mock_client, SAMPLE_STRADDLES

IST = pytz.timezone("Asia/Kolkata")

# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_monitor(
    entry_price: float = 650.0,
    running_min: float = None,
    trail_dist: float = 150.0,
    product_id: int = 12345,
    symbol: str = "MV-BTC-68000-150426",
    contracts: int = 1,
) -> TrailMonitor:
    m = TrailMonitor(
        symbol=symbol,
        product_id=product_id,
        entry_price=entry_price,
        contracts=contracts,
        trail_dist=trail_dist,
        running_min=running_min,
    )
    return m


# ══════════════════════════════════════════════════════════════════════════════
# 1. Day-of-week guard
# ══════════════════════════════════════════════════════════════════════════════

class TestTradeDay:
    """TRADE_DAYS = {0=Mon, 2=Wed, 3=Thu, 6=Sun}"""

    @pytest.mark.parametrize("weekday,expected", [
        (0, True),   # Monday
        (1, False),  # Tuesday
        (2, True),   # Wednesday
        (3, True),   # Thursday
        (4, False),  # Friday
        (5, False),  # Saturday
        (6, True),   # Sunday
    ])
    def test_is_trade_day(self, weekday, expected):
        fake_now = MagicMock()
        fake_now.weekday.return_value = weekday
        with patch('trader._now_ist', return_value=fake_now):
            assert is_trade_day() == expected


# ══════════════════════════════════════════════════════════════════════════════
# 2. Skip / resume flags
# ══════════════════════════════════════════════════════════════════════════════

class TestSkipResume:
    def setup_method(self):
        resume_trader()  # ensure clean state before each test

    def test_skip_sets_flag(self):
        skip_today("FOMC meeting")
        assert trader._skip_today is True
        assert trader._skip_reason == "FOMC meeting"

    def test_resume_clears_flag(self):
        skip_today("test")
        resume_trader()
        assert trader._skip_today is False
        assert trader._skip_reason == ""

    def test_default_skip_reason(self):
        skip_today()
        assert trader._skip_reason == "manual skip"


# ══════════════════════════════════════════════════════════════════════════════
# 3. Trail monitor — _process_price core logic
# ══════════════════════════════════════════════════════════════════════════════

class TestTrailMonitorPriceLogic:

    @pytest.mark.asyncio
    async def test_running_min_updates_downward(self):
        m = _make_monitor(entry_price=650.0)
        with patch.object(m, '_persist'):
            await m._process_price(600.0)
            assert m.running_min == 600.0
            await m._process_price(550.0)
            assert m.running_min == 550.0

    @pytest.mark.asyncio
    async def test_running_min_never_increases(self):
        m = _make_monitor(entry_price=650.0, running_min=500.0)
        with patch.object(m, '_persist'):
            await m._process_price(700.0)  # price bounces up
            assert m.running_min == 500.0  # should NOT change

    @pytest.mark.asyncio
    async def test_trail_does_not_fire_if_price_never_dips(self):
        """
        If price never goes below entry, trail must NOT fire —
        even if price rises far above entry.
        """
        m = _make_monitor(entry_price=650.0)
        with patch.object(m, '_persist'):
            for px in [660.0, 700.0, 800.0, 1000.0]:
                await m._process_price(px)
        assert not m._stop.is_set()
        assert m._exit_reason is None

    @pytest.mark.asyncio
    async def test_trail_fires_after_dip_and_bounce(self):
        """
        Classic trail scenario:
          entry=650 → dip to 480 (running_min=480)
          → bounce to 631 (480+150=630, 631 > 630) → TRAIL fires
        """
        m = _make_monitor(entry_price=650.0, trail_dist=150.0)
        with patch.object(m, '_persist'):
            await m._process_price(480.0)   # dip; running_min=480
            assert not m._stop.is_set()

            await m._process_price(629.0)   # 629 < 630 — not yet
            assert not m._stop.is_set()

            await m._process_price(631.0)   # 631 > 630 — fires!
            assert m._stop.is_set()
            assert m._exit_reason == "TRAIL"
            assert m._exit_price == 631.0

    @pytest.mark.asyncio
    async def test_trail_fires_exactly_at_boundary(self):
        """Trail fires when price == running_min + trail_dist (>= not just >)."""
        m = _make_monitor(entry_price=650.0, trail_dist=150.0)
        with patch.object(m, '_persist'):
            await m._process_price(500.0)   # running_min = 500
            await m._process_price(650.0)   # exactly at 500+150 — fires
        assert m._stop.is_set()
        assert m._exit_reason == "TRAIL"

    @pytest.mark.asyncio
    async def test_trail_does_not_fire_just_below_boundary(self):
        m = _make_monitor(entry_price=650.0, trail_dist=150.0)
        with patch.object(m, '_persist'):
            await m._process_price(500.0)   # running_min = 500
            await m._process_price(649.99)  # 649.99 < 650 — not firing
        assert not m._stop.is_set()

    @pytest.mark.asyncio
    async def test_running_min_tracks_progressive_lows(self):
        """Multiple dips: running_min follows the lowest."""
        m = _make_monitor(entry_price=700.0, trail_dist=150.0)
        with patch.object(m, '_persist'):
            await m._process_price(650.0)
            assert m.running_min == 650.0
            await m._process_price(600.0)
            assert m.running_min == 600.0
            await m._process_price(620.0)
            assert m.running_min == 600.0  # didn't go lower
            await m._process_price(580.0)
            assert m.running_min == 580.0

    @pytest.mark.asyncio
    async def test_zero_or_negative_price_ignored(self):
        m = _make_monitor(entry_price=650.0)
        with patch.object(m, '_persist'):
            await m._process_price(0.0)
            await m._process_price(-1.0)
        assert m.running_min == 650.0
        assert not m._stop.is_set()

    @pytest.mark.asyncio
    async def test_trail_uses_running_min_from_init(self):
        """Restart recovery: running_min can be pre-loaded below entry."""
        m = _make_monitor(entry_price=650.0, running_min=480.0, trail_dist=150.0)
        with patch.object(m, '_persist'):
            # Trail level is 480+150=630; price at 635 should fire
            await m._process_price(635.0)
        assert m._stop.is_set()
        assert m._exit_reason == "TRAIL"

    @pytest.mark.asyncio
    async def test_persist_called_only_on_new_min(self):
        m = _make_monitor(entry_price=650.0)
        with patch.object(m, '_persist') as mock_persist:
            await m._process_price(600.0)   # new min → persist
            await m._process_price(700.0)   # no new min → no persist
            await m._process_price(550.0)   # new min → persist
        assert mock_persist.call_count == 2


# ══════════════════════════════════════════════════════════════════════════════
# 4. ATM strike selection
# ══════════════════════════════════════════════════════════════════════════════

class TestAtmSelection:
    """Tests the ATM selection logic inside _try_find_atm."""

    def _select_atm(self, straddles: list, btc_spot: float) -> dict:
        """Mirror of the selection logic in trader._try_find_atm."""
        return min(straddles, key=lambda s: (abs(s["strike"] - btc_spot), s["strike"]))

    def test_exact_match(self):
        s = self._select_atm(SAMPLE_STRADDLES, 68000.0)
        assert s["strike"] == 68000

    def test_closest_below(self):
        s = self._select_atm(SAMPLE_STRADDLES, 67200.0)
        assert s["strike"] == 67000   # 67200-67000=200 vs 67500-67200=300

    def test_closest_above(self):
        s = self._select_atm(SAMPLE_STRADDLES, 68300.0)
        assert s["strike"] == 68500   # 68500-68300=200 vs 68300-68000=300

    def test_equidistant_picks_lower(self):
        """Decision 3: on equidistant, pick the lower strike."""
        straddles = [
            {"symbol": "MV-BTC-67750-150426", "strike": 67750, "mark_price": 600.0},
            {"symbol": "MV-BTC-68250-150426", "strike": 68250, "mark_price": 610.0},
        ]
        s = self._select_atm(straddles, 68000.0)  # equidistant: 250 each
        assert s["strike"] == 67750

    def test_single_available_strike(self):
        straddles = [
            {"symbol": "MV-BTC-68000-150426", "strike": 68000, "mark_price": 650.0}
        ]
        s = self._select_atm(straddles, 75000.0)
        assert s["strike"] == 68000


# ══════════════════════════════════════════════════════════════════════════════
# 5. enter_trade — paper mode
# ══════════════════════════════════════════════════════════════════════════════

class TestEnterTradePaper:
    @pytest.mark.asyncio
    async def test_paper_trade_returns_immediately(self):
        atm = {"symbol": "MV-BTC-68000-150426", "strike": 68000,
               "product_id": 99, "mark_price": 642.5}
        with patch('trader.PAPER_TRADE', True):
            result = await trader.enter_trade(atm)
        assert result is not None
        assert result["fill_price"] == 642.5
        assert result["contracts"] == trader.ENTRY_LOTS
        assert result["order_id"] == "PAPER"

    @pytest.mark.asyncio
    async def test_paper_trade_no_api_calls(self):
        atm = {"symbol": "MV-BTC-68000-150426", "strike": 68000,
               "product_id": 99, "mark_price": 600.0}
        with patch('trader.PAPER_TRADE', True), \
             patch('trader.DeltaClient') as mock_cls:
            await trader.enter_trade(atm)
        mock_cls.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# 6. enter_trade — live order flow (mocked API)
# ══════════════════════════════════════════════════════════════════════════════

class TestEnterTradeLive:
    ATM = {"symbol": "MV-BTC-68000-150426", "strike": 68000,
           "product_id": 99, "mark_price": 650.0}

    @pytest.mark.asyncio
    async def test_immediate_fill(self):
        filled_order = {"id": "111", "state": "filled", "average_fill_price": "647.5"}
        client = make_mock_client(
            place_order={"id": "111", "state": "open"},
            get_order=filled_order,
        )
        with patch('trader.PAPER_TRADE', False), \
             patch('trader.DeltaClient', return_value=client), \
             patch('trader.FILL_TIMEOUT_S', 30):
            result = await trader.enter_trade(self.ATM)

        assert result is not None
        assert result["fill_price"] == 647.5
        assert result["order_id"] == "111"

    @pytest.mark.asyncio
    async def test_cancelled_order_returns_none(self):
        client = make_mock_client(
            place_order={"id": "222", "state": "open"},
            get_order={"id": "222", "state": "cancelled"},
            cancel_order={},
            get_position=None,   # no existing position → retry fires, also times out
        )
        with patch('trader.PAPER_TRADE', False), \
             patch('trader.DeltaClient', return_value=client), \
             patch('trader.FILL_TIMEOUT_S', 0):
            result = await trader.enter_trade(self.ATM)
        assert result is None

    @pytest.mark.asyncio
    async def test_no_order_id_returns_none(self):
        client = make_mock_client(place_order={})
        with patch('trader.PAPER_TRADE', False), \
             patch('trader.DeltaClient', return_value=client):
            result = await trader.enter_trade(self.ATM)
        assert result is None

    @pytest.mark.asyncio
    async def test_timeout_triggers_cancel_and_retry(self):
        """
        First _await_fill times out (returns None) → order cancelled → retry placed.
        Second _await_fill succeeds → returns fill.
        """
        call_count = {"place": 0, "await": 0}

        async def mock_place_order(*a, **kw):
            call_count["place"] += 1
            return {"id": "AAA"} if call_count["place"] == 1 else {"id": "BBB"}

        async def mock_await_fill(order_id, timeout_s):
            call_count["await"] += 1
            if order_id == "AAA":
                return None   # first order: timeout
            return {"id": "BBB", "state": "filled", "average_fill_price": "655.0"}

        # position check returns empty — no existing position, so retry proceeds
        client = make_mock_client(cancel_order={}, get_position=None)
        client.place_order = AsyncMock(side_effect=mock_place_order)

        with patch('trader.PAPER_TRADE', False), \
             patch('trader.DeltaClient', return_value=client), \
             patch('trader._await_fill', side_effect=mock_await_fill):
            result = await trader.enter_trade(self.ATM)

        assert result is not None, "Expected a fill result on second attempt"
        assert result["fill_price"] == 655.0
        assert result["order_id"] == "BBB"
        assert call_count["place"] == 2   # two placement attempts
        assert call_count["await"] == 2   # two fill polls

    @pytest.mark.asyncio
    async def test_retry_aborted_when_position_already_open(self):
        """If cancel fails and a position already exists, attempt 2 must NOT fire."""
        call_count = {"place": 0}

        async def mock_place_order(*a, **kw):
            call_count["place"] += 1
            return {"id": "AAA"}

        async def mock_await_fill(order_id, timeout_s):
            return None   # always timeout

        alerts = []
        async def mock_telegram(msg):
            alerts.append(msg)

        # Position check returns a non-zero position — cancel silently failed
        client = make_mock_client(
            cancel_order={},
            get_position={"product_id": 131059, "size": 100},
        )
        client.place_order = AsyncMock(side_effect=mock_place_order)

        with patch('trader.PAPER_TRADE', False), \
             patch('trader.DeltaClient', return_value=client), \
             patch('trader._await_fill', side_effect=mock_await_fill), \
             patch('trader._telegram', side_effect=mock_telegram):
            result = await trader.enter_trade(self.ATM)

        assert result is None
        assert call_count["place"] == 1          # only one order placed
        assert any("ENTRY ABORT" in a for a in alerts)


# ══════════════════════════════════════════════════════════════════════════════
# 7. close_position — paper mode
# ══════════════════════════════════════════════════════════════════════════════

class TestClosePosition:
    @pytest.mark.asyncio
    async def test_paper_returns_mark_price(self):
        client = make_mock_client()
        client._get = AsyncMock(return_value={"result": {"mark_price": "499.5"}})
        with patch('trader.PAPER_TRADE', True), \
             patch('trader.DeltaClient', return_value=client):
            px = await trader.close_position(99, "MV-BTC-68000-150426", 1)
        assert px == 499.5

    @pytest.mark.asyncio
    async def test_live_returns_fill_price(self):
        client = make_mock_client(
            place_order={"id": "CLOSE1"},
            get_order={"id": "CLOSE1", "state": "filled", "average_fill_price": "502.0"},
        )
        with patch('trader.PAPER_TRADE', False), \
             patch('trader.DeltaClient', return_value=client), \
             patch('trader.FILL_TIMEOUT_S', 30), \
             patch('trader.CLOSE_RETRY_LIMIT_S', 120):
            px = await trader.close_position(99, "MV-BTC-68000-150426", 1)
        assert px == 502.0

    @pytest.mark.asyncio
    async def test_close_retries_on_timeout_then_succeeds(self):
        """First close attempt times out; second attempt fills."""
        attempt = {"n": 0}

        async def mock_await_fill(order_id, timeout_s):
            attempt["n"] += 1
            if attempt["n"] == 1:
                return None   # first attempt: timeout
            return {"id": order_id, "state": "closed", "average_fill_price": "510.0"}

        client = make_mock_client(place_order={"id": "CL2"})
        with patch('trader.PAPER_TRADE', False), \
             patch('trader.DeltaClient', return_value=client), \
             patch('trader.CLOSE_RETRY_LIMIT_S', 999), \
             patch('trader._await_fill', side_effect=mock_await_fill):
            px = await trader.close_position(99, "MV-BTC-68000-150426", 1)

        assert px == 510.0
        assert attempt["n"] == 2

    @pytest.mark.asyncio
    async def test_close_sends_emergency_alert_after_all_retries(self):
        """All close attempts time out → returns 0.0 and sends Telegram alert."""
        alerts = []

        async def mock_telegram(msg):
            alerts.append(msg)

        async def mock_await_fill(order_id, timeout_s):
            return None   # always times out

        client = make_mock_client(place_order={"id": "CL3"})
        with patch('trader.PAPER_TRADE', False), \
             patch('trader.DeltaClient', return_value=client), \
             patch('trader.CLOSE_RETRY_LIMIT_S', 0), \
             patch('trader._telegram', side_effect=mock_telegram), \
             patch('trader._await_fill', side_effect=mock_await_fill):
            px = await trader.close_position(99, "MV-BTC-68000-150426", 1)

        assert px == 0.0
        assert any("EMERGENCY" in a for a in alerts)


# ══════════════════════════════════════════════════════════════════════════════
# 8. Hard exit timer
# ══════════════════════════════════════════════════════════════════════════════

class TestHardExit:
    @pytest.mark.asyncio
    async def test_hard_exit_fires_when_time_reached(self):
        """If hard_exit_ist is in the past, the timer should fire immediately."""
        m = _make_monitor()
        past_ist = datetime(2020, 1, 1, 16, 30, 0,
                            tzinfo=pytz.timezone("Asia/Kolkata"))
        await m._hard_exit_timer(past_ist)
        assert m._stop.is_set()
        assert m._exit_reason == "HARD_EXIT"

    @pytest.mark.asyncio
    async def test_hard_exit_not_fired_if_already_stopped(self):
        """If stop event already set (e.g. trail fired), hard exit must be a no-op."""
        m = _make_monitor()
        m._stop.set()
        m._exit_reason = "TRAIL"
        past_ist = datetime(2020, 1, 1, 16, 30, 0,
                            tzinfo=pytz.timezone("Asia/Kolkata"))
        await m._hard_exit_timer(past_ist)
        # Exit reason must NOT be overwritten
        assert m._exit_reason == "TRAIL"


# ══════════════════════════════════════════════════════════════════════════════
# 9. Failure watchdog
# ══════════════════════════════════════════════════════════════════════════════

class TestFailureWatchdog:
    @pytest.mark.asyncio
    async def test_watchdog_fires_after_silence(self):
        m = _make_monitor()
        import time
        m._last_update = time.time() - (trader.WS_FAIL_LIMIT_S + 1)

        # Run one cycle of watchdog (sleep(30) is short-circuited by patching)
        with patch('asyncio.sleep', new_callable=AsyncMock):
            # Wrap in a task with timeout so it doesn't block
            async def one_cycle():
                await asyncio.sleep(0)  # yield
                elapsed = time.time() - m._last_update
                if elapsed > trader.WS_FAIL_LIMIT_S:
                    m._exit_reason = "SAFETY_CLOSE"
                    m._stop.set()

            await one_cycle()

        assert m._stop.is_set()
        assert m._exit_reason == "SAFETY_CLOSE"


# ══════════════════════════════════════════════════════════════════════════════
# 10. run_trade_job — guards
# ══════════════════════════════════════════════════════════════════════════════

class TestRunTradeJobGuards:
    def setup_method(self):
        resume_trader()

    @pytest.mark.asyncio
    async def test_skip_on_non_trade_day(self):
        fake_now = MagicMock()
        fake_now.weekday.return_value = 1   # Tuesday
        fake_now.strftime.return_value = "Tuesday"
        with patch('trader._now_ist', return_value=fake_now), \
             patch('trader.find_atm_straddle', new_callable=AsyncMock) as mock_find:
            await trader.run_trade_job()
        mock_find.assert_not_called()

    @pytest.mark.asyncio
    async def test_skip_when_skip_flag_set(self):
        skip_today("test reason")
        fake_now = MagicMock()
        fake_now.weekday.return_value = 0   # Monday — trade day
        fake_now.strftime.return_value = "Monday"
        with patch('trader._now_ist', return_value=fake_now), \
             patch('trader.find_atm_straddle', new_callable=AsyncMock) as mock_find:
            await trader.run_trade_job()
        mock_find.assert_not_called()
        # Skip flag auto-resets after firing
        assert trader._skip_today is False

    @pytest.mark.asyncio
    async def test_aborts_when_no_atm_found(self):
        fake_now = MagicMock()
        fake_now.weekday.return_value = 0   # Monday
        fake_now.strftime.return_value = "Mon 15 Apr 2026"
        with patch('trader._now_ist', return_value=fake_now), \
             patch('trader.load_position', return_value=None), \
             patch('trader.find_atm_straddle', new_callable=AsyncMock, return_value=None), \
             patch('trader._telegram', new_callable=AsyncMock) as mock_tg:
            await trader.run_trade_job()
        mock_tg.assert_called_once()
        assert "No ATM" in mock_tg.call_args[0][0]

    @pytest.mark.asyncio
    async def test_aborts_when_entry_fails(self):
        fake_now = MagicMock()
        fake_now.weekday.return_value = 0
        fake_now.strftime.return_value = "Mon 15 Apr 2026"
        fake_now.isoformat.return_value = "2026-04-15T06:00:00+05:30"
        atm = {"symbol": "MV-BTC-68000-150426", "strike": 68000,
               "product_id": 99, "mark_price": 650.0}
        with patch('trader._now_ist', return_value=fake_now), \
             patch('trader.load_position', return_value=None), \
             patch('trader.find_atm_straddle', new_callable=AsyncMock, return_value=atm), \
             patch('trader.enter_trade', new_callable=AsyncMock, return_value=None), \
             patch('trader._telegram', new_callable=AsyncMock) as mock_tg:
            await trader.run_trade_job()
        mock_tg.assert_called_once()
        assert "failed" in mock_tg.call_args[0][0].lower()


# ══════════════════════════════════════════════════════════════════════════════
# 11. PnL scaling
# ══════════════════════════════════════════════════════════════════════════════

class TestPnlScaling:
    """
    pnl_usd = (entry_price - exit_price) * (contracts / 1000)
    100 lots: price change $150 → PnL $15.0
    1 lot:    price change $150 → PnL $0.15
    """

    def _calc(self, entry, exit_px, contracts):
        return round((entry - exit_px) * (contracts / 1000), 4)

    def test_100_lots_150_profit(self):
        assert self._calc(650.0, 500.0, 100) == 15.0

    def test_100_lots_150_loss(self):
        assert self._calc(650.0, 800.0, 100) == -15.0

    def test_1_lot_150_profit(self):
        assert self._calc(650.0, 500.0, 1) == 0.15

    def test_1_lot_full_decay(self):
        """Straddle expires worthless: full entry price captured."""
        assert self._calc(650.0, 0.0, 100) == 65.0

    def test_trail_fires_at_150_loss(self):
        """Trail $150 on 100 lots = $15 actual loss."""
        entry = 650.0
        trail_dist = 150.0
        # Price dips to 480, then bounces to 631 (480+151=631 > 630)
        exit_px = 631.0
        assert self._calc(entry, exit_px, 100) == pytest.approx(1.9, rel=0.01)


# ══════════════════════════════════════════════════════════════════════════════
# 12. get_position_status
# ══════════════════════════════════════════════════════════════════════════════

class TestGetPositionStatus:
    def test_no_position(self):
        trader._active_monitor = None
        with patch('trader.load_position', return_value=None):
            s = get_position_status()
        assert "No active" in s

    def test_with_active_monitor(self):
        m = _make_monitor(entry_price=650.0, running_min=500.0)
        trader._active_monitor = m
        s = get_position_status()
        trader._active_monitor = None
        assert "650" in s
        assert "500" in s
        assert "650.0" in s   # trail level: 500+150=650

    def test_with_persisted_but_no_monitor(self):
        trader._active_monitor = None
        saved = {
            "active": True,
            "symbol": "MV-BTC-68000-150426",
            "entry_price": 700.0,
            "running_min": 550.0,
        }
        with patch('trader.load_position', return_value=saved):
            s = get_position_status()
        assert "MV-BTC-68000" in s
        assert "700" in s
