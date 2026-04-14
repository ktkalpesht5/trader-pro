"""
test_integration_delta.py
--------------------------
Integration tests against the REAL Delta Exchange India API.

These tests make actual HTTP and WebSocket calls. They require:
  - DELTA_API_KEY and DELTA_API_SECRET in your environment / .env file
  - An active Delta Exchange India account with API access
  - Run during market hours for order/position tests

To run all integration tests:
    pytest tests/test_integration_delta.py -v -m integration

To run ONLY safe read-only tests (no orders placed):
    pytest tests/test_integration_delta.py -v -m "integration and not live_order"

To run the real 1-lot order test (REAL MONEY — use with care):
    pytest tests/test_integration_delta.py -v -m live_order

All order tests use ONLY 1 LOT (not 100).
"""

import asyncio
import json
import os
import sys
import time

import pytest
import pytz
import websockets
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from delta_client import DeltaClient
import trader

IST = pytz.timezone("Asia/Kolkata")
WS_URL = "wss://socket.india.delta.exchange"

pytestmark = pytest.mark.integration


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def require_keys():
    """Skip entire module if API keys not configured."""
    if not os.getenv("DELTA_API_KEY") or not os.getenv("DELTA_API_SECRET"):
        pytest.skip("DELTA_API_KEY / DELTA_API_SECRET not set")


@pytest.fixture(scope="module")
async def live_client():
    """Shared DeltaClient for the whole module."""
    async with DeltaClient() as client:
        yield client


# ══════════════════════════════════════════════════════════════════════════════
# 1. Public REST — no auth needed
# ══════════════════════════════════════════════════════════════════════════════

class TestPublicRest:
    @pytest.mark.asyncio
    async def test_btc_spot_returns_positive_price(self):
        """BTC spot price must be a positive number."""
        async with DeltaClient() as client:
            spot = await client.get_btc_spot()
        assert isinstance(spot, float)
        assert spot > 10_000, f"Suspicious BTC price: {spot}"
        print(f"\n  BTC Spot: ${spot:,.0f}")

    @pytest.mark.asyncio
    async def test_get_today_straddles_returns_list(self):
        """Should return at least one MV-BTC straddle expiring today."""
        async with DeltaClient() as client:
            straddles = await client.get_today_straddles()
        assert isinstance(straddles, list)
        # Straddles may be empty outside 5 AM–5:30 PM IST; don't fail on that
        for s in straddles:
            assert "symbol" in s
            assert s["symbol"].startswith("MV-BTC-")
            assert s["strike"] > 0
            assert s["mark_price"] >= 0
        print(f"\n  Straddles today: {len(straddles)}")
        if straddles:
            for s in straddles:
                print(f"    {s['symbol']:30s}  mark=${s['mark_price']:.2f}")

    @pytest.mark.asyncio
    async def test_straddle_mark_price_in_expected_range(self):
        """MV straddle prices must be in a reasonable USD range ($50–$5,000)."""
        async with DeltaClient() as client:
            straddles = await client.get_today_straddles()
        if not straddles:
            pytest.skip("No straddles available right now (outside trading hours?)")
        for s in straddles:
            if s["mark_price"] > 0:
                assert 50 <= s["mark_price"] <= 5_000, \
                    f"Unexpected price for {s['symbol']}: ${s['mark_price']}"

    @pytest.mark.asyncio
    async def test_btc_candles_returns_sorted_candles(self):
        async with DeltaClient() as client:
            candles = await client.get_btc_candles(resolution="1h", count=24)
        assert len(candles) > 0
        times = [c["time"] for c in candles]
        assert times == sorted(times), "Candles not sorted by time"
        for c in candles:
            assert c["close"] > 0
            assert c["high"] >= c["low"]

    @pytest.mark.asyncio
    async def test_hours_to_expiry_positive_during_trading_day(self):
        async with DeltaClient() as client:
            hrs = client.hours_to_expiry()
        from datetime import datetime
        now_ist = datetime.now(IST)
        cutoff_h, cutoff_m = 17, 30
        if now_ist.hour < cutoff_h or (now_ist.hour == cutoff_h and now_ist.minute < cutoff_m):
            assert hrs > 0, "hours_to_expiry should be positive before 17:30 IST"
        else:
            assert hrs == 0.0, "hours_to_expiry should be 0 after 17:30 IST"


# ══════════════════════════════════════════════════════════════════════════════
# 2. find_atm_straddle helper logic (without the full discovery loop sleep)
# ══════════════════════════════════════════════════════════════════════════════

class TestAtmDiscoveryLogic:
    @pytest.mark.asyncio
    async def test_try_find_atm_returns_valid_dict(self):
        """_try_find_atm should return a valid ATM dict when straddles exist."""
        async with DeltaClient() as client:
            straddles = await client.get_today_straddles()
        if not straddles:
            pytest.skip("No straddles listed right now")

        result = await trader._try_find_atm()
        assert result is not None
        assert "symbol"     in result
        assert "strike"     in result
        assert "product_id" in result
        assert "mark_price" in result
        assert result["product_id"] > 0
        assert result["mark_price"] > 0
        print(f"\n  ATM found: {result['symbol']} @ ${result['mark_price']:.2f} "
              f"(product_id={result['product_id']})")

    @pytest.mark.asyncio
    async def test_atm_strike_is_closest_to_spot(self):
        """The ATM strike must be the one closest to the current BTC spot."""
        async with DeltaClient() as client:
            spot      = await client.get_btc_spot()
            straddles = await client.get_today_straddles()
        if not straddles:
            pytest.skip("No straddles listed right now")

        atm = await trader._try_find_atm()
        assert atm is not None

        # Manually find best strike
        best = min(straddles, key=lambda s: (abs(s["strike"] - spot), s["strike"]))
        assert atm["strike"] == best["strike"], \
            f"Expected ATM {best['strike']}, got {atm['strike']} for spot ${spot:.0f}"

    @pytest.mark.asyncio
    async def test_product_id_is_valid_integer(self):
        result = await trader._try_find_atm()
        if result is None:
            pytest.skip("No straddles listed right now")
        assert isinstance(result["product_id"], int)
        assert result["product_id"] > 0


# ══════════════════════════════════════════════════════════════════════════════
# 3. Authenticated REST — requires API keys
# ══════════════════════════════════════════════════════════════════════════════

class TestAuthenticatedRest:
    @pytest.fixture(autouse=True)
    def need_keys(self, require_keys):
        pass

    @pytest.mark.asyncio
    async def test_get_product_id_for_atm(self):
        """get_product_id must return a positive int for a valid symbol."""
        async with DeltaClient() as client:
            straddles = await client.get_today_straddles()
        if not straddles:
            pytest.skip("No straddles listed")

        sym = straddles[0]["symbol"]
        async with DeltaClient() as client:
            pid = await client.get_product_id(sym)
        assert isinstance(pid, int) and pid > 0
        print(f"\n  {sym} → product_id={pid}")

    @pytest.mark.asyncio
    async def test_get_position_returns_none_or_dict(self):
        """get_position must not raise — returns None or position dict."""
        async with DeltaClient() as client:
            straddles = await client.get_today_straddles()
        if not straddles:
            pytest.skip("No straddles listed")

        try:
            async with DeltaClient() as client:
                pid = await client.get_product_id(straddles[0]["symbol"])
                pos = await client.get_position(pid)
        except Exception as e:
            if "401" in str(e) or "Unauthorized" in str(e):
                pytest.skip("API key invalid or missing — 401 Unauthorized")
            raise
        assert pos is None or isinstance(pos, dict)
        print(f"\n  Position for {straddles[0]['symbol']}: {pos}")

    @pytest.mark.asyncio
    async def test_sign_generates_different_signatures_per_call(self):
        """Each _sign() call must produce a unique signature (different timestamp)."""
        async with DeltaClient() as client:
            h1 = client._sign("GET", "/v2/orders")
            await asyncio.sleep(1.1)
            h2 = client._sign("GET", "/v2/orders")
        assert h1["signature"] != h2["signature"]
        assert h1["timestamp"] != h2["timestamp"]


# ══════════════════════════════════════════════════════════════════════════════
# 4. WebSocket connectivity
# ══════════════════════════════════════════════════════════════════════════════

class TestWebSocket:
    @pytest.mark.asyncio
    async def test_ws_connection_opens(self):
        """Must be able to open a WebSocket connection to Delta Exchange India."""
        import ssl as _ssl
        try:
            async with websockets.connect(
                WS_URL,
                ssl=_ssl.create_default_context(),
                open_timeout=10,
                ping_interval=None,
            ) as ws:
                # In websockets>=12, ClientConnection has no .open attribute.
                # Successfully entering the context manager means the connection
                # handshake completed. Send a ping to confirm it's truly alive.
                pong = await ws.ping()
                await asyncio.wait_for(asyncio.shield(pong), timeout=5.0)
        except Exception as e:
            pytest.fail(f"WebSocket connection failed: {e}")

    @pytest.mark.asyncio
    async def test_ws_subscribe_mark_price_no_error(self):
        """
        Subscribing to mark_price for an ATM symbol must not produce an error response.
        Waits up to 5 seconds for the first message (subscription ack or price update).
        """
        atm = await trader._try_find_atm()
        if atm is None:
            pytest.skip("No straddles listed right now")

        import ssl as _ssl
        async with websockets.connect(
            WS_URL, ssl=_ssl.create_default_context(),
            open_timeout=10, ping_interval=None
        ) as ws:
            sub = json.dumps({
                "type": "subscribe",
                "payload": {
                    "channels": [{"name": "mark_price", "symbols": [atm["symbol"]]}]
                }
            })
            await ws.send(sub)

            # Wait up to 5 seconds for any message
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                msg = json.loads(raw)
                print(f"\n  First WS message: {msg}")
                # Must NOT be an error
                assert msg.get("type") != "error", f"WS returned error: {msg}"
            except asyncio.TimeoutError:
                pytest.skip("No WS message received in 5s — exchange may be quiet")

    @pytest.mark.asyncio
    async def test_ws_receives_mark_price_message(self):
        """
        After subscribing, must receive at least one mark_price message within 30 seconds.
        Verifies: type="mark_price", product_id matches, price is a positive number.
        """
        atm = await trader._try_find_atm()
        if atm is None:
            pytest.skip("No straddles listed right now")

        received = []

        import ssl as _ssl
        async with websockets.connect(
            WS_URL, ssl=_ssl.create_default_context(),
            open_timeout=10, ping_interval=20
        ) as ws:
            sub = json.dumps({
                "type": "subscribe",
                "payload": {
                    "channels": [{"name": "mark_price", "symbols": [atm["symbol"]]}]
                }
            })
            await ws.send(sub)

            deadline = time.time() + 30
            while time.time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    msg = json.loads(raw)
                    if msg.get("type") == "mark_price":
                        received.append(msg)
                        break
                except asyncio.TimeoutError:
                    continue

        if not received:
            pytest.skip("No mark_price message in 30s — market may be closed")

        msg = received[0]
        print(f"\n  mark_price message: {msg}")
        assert "product_id" in msg, "Missing product_id in mark_price message"
        assert "price"      in msg, "Missing price in mark_price message"
        assert int(msg["product_id"]) == atm["product_id"], \
            f"product_id mismatch: got {msg['product_id']}, expected {atm['product_id']}"
        assert float(msg["price"]) > 0, f"Invalid price: {msg['price']}"

    @pytest.mark.asyncio
    async def test_trail_monitor_receives_ws_price(self):
        """
        TrailMonitor._ws_session must successfully receive and process at least
        one price update before we cancel it. Validates the full WS parsing path.
        """
        atm = await trader._try_find_atm()
        if atm is None:
            pytest.skip("No straddles listed right now")

        prices_seen = []

        # Subclass monitor to capture prices without actually triggering exit
        class PriceCapture(trader.TrailMonitor):
            async def _process_price(self, price: float):
                prices_seen.append(price)
                self._stop.set()   # stop after first price

        mon = PriceCapture(
            symbol=atm["symbol"],
            product_id=atm["product_id"],
            entry_price=atm["mark_price"],
            contracts=1,
            trail_dist=150.0,
        )

        try:
            await asyncio.wait_for(mon._ws_session(), timeout=30.0)
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            pass

        if not prices_seen:
            pytest.skip("No WS price received in 30s — market may be closed")

        print(f"\n  Prices received via WS: {prices_seen}")
        assert len(prices_seen) >= 1
        assert all(p > 0 for p in prices_seen)


# ══════════════════════════════════════════════════════════════════════════════
# 5. REST fallback polling
# ══════════════════════════════════════════════════════════════════════════════

class TestRestFallback:
    @pytest.mark.asyncio
    async def test_rest_ticker_returns_mark_price(self):
        """The REST ticker endpoint used by _rest_loop must return mark_price."""
        atm = await trader._try_find_atm()
        if atm is None:
            pytest.skip("No straddles listed right now")

        async with DeltaClient() as client:
            data = await client._get(f"/v2/tickers/{atm['symbol']}")

        result = data.get("result", {})
        assert "mark_price" in result, "mark_price missing from ticker response"
        px = float(result["mark_price"] or 0)
        assert px > 0, f"mark_price is zero or null for {atm['symbol']}"
        print(f"\n  REST mark_price for {atm['symbol']}: ${px:.2f}")

    @pytest.mark.asyncio
    async def test_rest_fallback_loop_processes_price(self):
        """
        Runs one iteration of _rest_loop and verifies it updates running_min.
        """
        atm = await trader._try_find_atm()
        if atm is None:
            pytest.skip("No straddles listed right now")

        prices_seen = []

        class PriceCapture(trader.TrailMonitor):
            async def _process_price(self, price: float):
                prices_seen.append(price)
                self._stop.set()

        mon = PriceCapture(
            symbol=atm["symbol"],
            product_id=atm["product_id"],
            entry_price=atm["mark_price"],
            contracts=1,
            trail_dist=150.0,
        )

        # Patch sleep so the loop fires immediately
        import unittest.mock as mock
        async def instant_sleep(_):
            pass

        with mock.patch('asyncio.sleep', side_effect=instant_sleep):
            try:
                await asyncio.wait_for(mon._rest_loop(), timeout=15.0)
            except asyncio.TimeoutError:
                pass

        assert len(prices_seen) >= 1, "REST fallback returned no prices"
        assert prices_seen[0] > 0
        print(f"\n  REST fallback price: ${prices_seen[0]:.2f}")


# ══════════════════════════════════════════════════════════════════════════════
# 6. PAPER trade full cycle (no real money)
# ══════════════════════════════════════════════════════════════════════════════

class TestPaperTradeCycle:
    @pytest.fixture(autouse=True)
    def need_keys(self, require_keys):
        pass

    @pytest.mark.asyncio
    async def test_paper_enter_returns_mark_price(self):
        atm = await trader._try_find_atm()
        if atm is None:
            pytest.skip("No straddles listed right now")

        import unittest.mock as mock
        with mock.patch('trader.PAPER_TRADE', True), \
             mock.patch('trader.ENTRY_LOTS', 1):
            result = await trader.enter_trade(atm)

        assert result is not None
        assert result["order_id"]  == "PAPER"
        assert result["contracts"] == 1
        assert result["fill_price"] == atm["mark_price"]
        print(f"\n  Paper entry: {atm['symbol']} @ ${result['fill_price']:.2f}")

    @pytest.mark.asyncio
    async def test_paper_close_returns_current_price(self):
        atm = await trader._try_find_atm()
        if atm is None:
            pytest.skip("No straddles listed right now")

        import unittest.mock as mock
        with mock.patch('trader.PAPER_TRADE', True):
            exit_px = await trader.close_position(
                atm["product_id"], atm["symbol"], 1
            )

        assert isinstance(exit_px, float)
        assert exit_px > 0
        print(f"\n  Paper exit price: ${exit_px:.2f}")

    @pytest.mark.asyncio
    async def test_paper_pnl_calculation(self):
        """Entry at live mark price, paper-exit at same price → PnL ≈ 0."""
        atm = await trader._try_find_atm()
        if atm is None:
            pytest.skip("No straddles listed right now")

        import unittest.mock as mock
        with mock.patch('trader.PAPER_TRADE', True), \
             mock.patch('trader.ENTRY_LOTS', 1):
            fill   = await trader.enter_trade(atm)
            exit_px = await trader.close_position(
                atm["product_id"], atm["symbol"], 1
            )

        entry_px = fill["fill_price"]
        pnl = (entry_px - exit_px) * (1 / 1000)
        print(f"\n  Paper trade: entry=${entry_px:.2f}, exit=${exit_px:.2f}, "
              f"PnL=${pnl:+.4f}")
        # PnL should be small (market moves slowly between two consecutive calls)
        assert abs(pnl) < 5.0, f"Suspiciously large PnL: ${pnl}"


# ══════════════════════════════════════════════════════════════════════════════
# 7. LIVE order test — 1 LOT REAL MONEY — explicit mark required
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live_order
class TestLiveOrder1Lot:
    """
    Places a REAL market order for 1 lot and immediately closes it.
    Net cost = 2 × fee ≈ $0.02 (plus any spread).

    Run ONLY with:
        pytest tests/test_integration_delta.py::TestLiveOrder1Lot -v -m live_order

    Requires DELTA_API_KEY + DELTA_API_SECRET + an active account.
    """

    @pytest.fixture(autouse=True)
    def need_keys(self, require_keys):
        pass

    @pytest.mark.asyncio
    async def test_place_and_immediately_close_1_lot(self):
        atm = await trader._try_find_atm()
        assert atm is not None, "Cannot find ATM straddle — is market open?"

        print(f"\n  ATM: {atm['symbol']} mark=${atm['mark_price']:.2f} "
              f"(product_id={atm['product_id']})")

        # ── SELL 1 LOT ─────────────────────────────────────────────────────
        async with DeltaClient() as client:
            sell_order = await client.place_order(
                product_id=atm["product_id"],
                side="sell",
                size=1,
                order_type="market_order",
            )
        sell_id = str(sell_order.get("id", ""))
        assert sell_id, f"Sell order returned no ID: {sell_order}"
        print(f"  Sell order placed: id={sell_id}")

        # Poll for fill
        sell_px = None
        for _ in range(12):   # 60s max
            await asyncio.sleep(5)
            async with DeltaClient() as client:
                o = await client.get_order(sell_id)
            print(f"  Sell state: {o.get('state')} avg_fill={o.get('average_fill_price')}")
            if o.get("state") in ("filled", "closed"):  # Delta India uses "closed"
                sell_px = float(o.get("average_fill_price", 0))
                break
            if o.get("state") in ("cancelled", "rejected"):
                pytest.fail(f"Sell order {sell_id} was {o['state']}")

        assert sell_px and sell_px > 0, "Sell order not filled within 60s"
        print(f"  SOLD 1 lot @ ${sell_px:.2f}")

        # ── BUY 1 LOT TO CLOSE ─────────────────────────────────────────────
        await asyncio.sleep(2)   # brief pause before close

        async with DeltaClient() as client:
            buy_order = await client.place_order(
                product_id=atm["product_id"],
                side="buy",
                size=1,
                order_type="market_order",
            )
        buy_id = str(buy_order.get("id", ""))
        assert buy_id, f"Buy order returned no ID: {buy_order}"
        print(f"  Buy order placed: id={buy_id}")

        buy_px = None
        for _ in range(12):
            await asyncio.sleep(5)
            async with DeltaClient() as client:
                o = await client.get_order(buy_id)
            print(f"  Buy state: {o.get('state')} avg_fill={o.get('average_fill_price')}")
            if o.get("state") in ("filled", "closed"):  # Delta India uses "closed"
                buy_px = float(o.get("average_fill_price", 0))
                break
            if o.get("state") in ("cancelled", "rejected"):
                pytest.fail(f"Buy order {buy_id} was {o['state']}")

        assert buy_px and buy_px > 0, "Buy order not filled within 60s"
        print(f"  BOUGHT 1 lot @ ${buy_px:.2f}")

        # ── Verify position is flat ─────────────────────────────────────────
        await asyncio.sleep(2)
        async with DeltaClient() as client:
            pos = await client.get_position(atm["product_id"])
        pos_size = int(pos.get("size", 0)) if pos else 0
        print(f"  Position after close: {pos_size} lots")
        assert pos_size == 0, \
            f"Position not flat after round-trip! size={pos_size}"

        # ── Summary ────────────────────────────────────────────────────────
        pnl = (sell_px - buy_px) * (1 / 1000)
        print(f"\n  Round-trip: sell=${sell_px:.2f} buy=${buy_px:.2f} "
              f"spread=${buy_px - sell_px:.2f} PnL=${pnl:+.4f}")
        # Should be a tiny number — not a catastrophic loss
        assert abs(pnl) < 2.0, \
            f"Unexpectedly large P&L on 1-lot round trip: ${pnl}"

    @pytest.mark.asyncio
    async def test_order_fill_state_transitions(self):
        """Verify order state goes open → filled (not open → cancelled)."""
        atm = await trader._try_find_atm()
        assert atm is not None

        async with DeltaClient() as client:
            order = await client.place_order(
                product_id=atm["product_id"],
                side="sell",
                size=1,
                order_type="market_order",
            )
        order_id = str(order.get("id", ""))
        assert order_id

        # Immediately query — might still be open
        await asyncio.sleep(1)
        async with DeltaClient() as client:
            o = await client.get_order(order_id)
        state = o.get("state", "")
        assert state in ("open", "filled", "closed"), f"Unexpected state: {state}"

        # Close if filled (Delta India reports "closed" for filled market orders)
        if state in ("filled", "closed"):
            async with DeltaClient() as client:
                await client.place_order(
                    product_id=atm["product_id"],
                    side="buy",
                    size=1,
                    order_type="market_order",
                )
        else:
            # Cancel if still open
            async with DeltaClient() as client:
                await client.cancel_order(order_id, atm["product_id"])

    @pytest.mark.asyncio
    async def test_cancel_unfilled_limit_order(self):
        """
        Place a limit order far from market price (won't fill),
        confirm it's open, then cancel it.
        """
        atm = await trader._try_find_atm()
        assert atm is not None

        # Sell limit far ABOVE current mark price — will never fill
        # (a sell limit fills only when market >= limit; 10x is unreachable)
        far_limit = round(atm["mark_price"] * 10.0, 2)

        async with DeltaClient() as client:
            order = await client.place_order(
                product_id=atm["product_id"],
                side="sell",
                size=1,
                order_type="limit_order",
                limit_price=far_limit,
            )
        order_id = str(order.get("id", ""))
        assert order_id, f"No order ID: {order}"
        print(f"\n  Limit order {order_id} placed @ ${far_limit:.2f}")

        # Verify it's open
        await asyncio.sleep(1)
        async with DeltaClient() as client:
            o = await client.get_order(order_id)
        assert o.get("state") == "open", \
            f"Expected 'open', got '{o.get('state')}'"

        # Cancel it
        async with DeltaClient() as client:
            result = await client.cancel_order(order_id, atm["product_id"])
        print(f"  Cancel result: {result.get('state', result)}")

        # Confirm cancelled
        await asyncio.sleep(1)
        async with DeltaClient() as client:
            o2 = await client.get_order(order_id)
        assert o2.get("state") in ("cancelled", "closed"), \
            f"Expected 'cancelled', got '{o2.get('state')}'"
        print("  Order successfully cancelled ✓")


# ══════════════════════════════════════════════════════════════════════════════
# 8. TrailMonitor full integration (paper, real price feed)
# ══════════════════════════════════════════════════════════════════════════════

class TestTrailMonitorIntegration:
    """
    Runs the TrailMonitor with the real WebSocket feed but with an
    unreachable trail level so it never actually fires.
    Verifies the monitor correctly receives prices and updates running_min.
    """

    @pytest.mark.asyncio
    async def test_monitor_tracks_real_prices_for_10s(self):
        atm = await trader._try_find_atm()
        if atm is None:
            pytest.skip("No straddles listed right now")

        prices_seen = []

        class ObservingMonitor(trader.TrailMonitor):
            async def _process_price(self, price: float):
                prices_seen.append(price)
                await super()._process_price(price)

        mon = ObservingMonitor(
            symbol=atm["symbol"],
            product_id=atm["product_id"],
            entry_price=atm["mark_price"],
            contracts=1,
            trail_dist=999_999,   # unreachable — monitor won't fire
            running_min=atm["mark_price"],
        )

        # Run for 10 seconds then cancel
        async def cancel_after_10s():
            await asyncio.sleep(10)
            mon._exit_reason = "TEST_TIMEOUT"
            mon._stop.set()

        import unittest.mock as mock
        with mock.patch.object(mon, '_persist'):   # don't write to disk
            tasks = [
                asyncio.create_task(mon._ws_loop()),
                asyncio.create_task(mon._rest_loop()),
                asyncio.create_task(cancel_after_10s()),
            ]
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()

        print(f"\n  Prices received in 10s: {len(prices_seen)}")
        print(f"  Prices: {[f'${p:.2f}' for p in prices_seen[:5]]}")

        if not prices_seen:
            pytest.skip("No prices received — market may be closed")

        assert len(prices_seen) >= 1
        assert all(p > 0 for p in prices_seen)
        # running_min must equal or be lower than the lowest seen price
        assert mon.running_min <= min(prices_seen) + 0.01  # tiny float tolerance
