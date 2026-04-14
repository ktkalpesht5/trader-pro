"""
conftest.py — shared fixtures and helpers for all tests.
"""
import asyncio
import os
import sys
import tempfile
from unittest.mock import AsyncMock

import pytest

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Event loop ────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ── API key guard ─────────────────────────────────────────────────────────────

@pytest.fixture
def require_api_keys():
    """Skip any test that needs real credentials if env vars not set."""
    if not os.getenv("DELTA_API_KEY") or not os.getenv("DELTA_API_SECRET"):
        pytest.skip("DELTA_API_KEY and DELTA_API_SECRET not set — skipping live test")


# ── Mock DeltaClient factory ──────────────────────────────────────────────────

def make_mock_client(**method_returns) -> AsyncMock:
    """
    Build a mock DeltaClient that works as an async context manager.

    Usage:
        client = make_mock_client(
            get_btc_spot=68000.0,
            get_today_straddles=[...],
        )
        with patch('trader.DeltaClient', return_value=client):
            ...
    """
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock)
    mock.__aexit__ = AsyncMock(return_value=False)
    for name, val in method_returns.items():
        setattr(mock, name, AsyncMock(return_value=val))
    return mock


# ── Temp trade log ────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_trade_log(tmp_path):
    """Returns a path to a temporary trade log file."""
    return str(tmp_path / "trades_test.json")


# ── Temp position state ───────────────────────────────────────────────────────

@pytest.fixture
def tmp_position_file(tmp_path, monkeypatch):
    """Redirect state_store to use a temp file for each test."""
    import state_store
    monkeypatch.setattr(state_store, "STATE_FILE", str(tmp_path / "position.json"))
    monkeypatch.setattr(state_store, "_TMP_FILE", str(tmp_path / "position.json.tmp"))


# ── Sample straddle data ──────────────────────────────────────────────────────

SAMPLE_STRADDLES = [
    {"symbol": "MV-BTC-67000-150426", "strike": 67000, "mark_price": 720.0,
     "greeks": {"delta": 0.02}, "iv": 45.0},
    {"symbol": "MV-BTC-67500-150426", "strike": 67500, "mark_price": 680.0,
     "greeks": {"delta": -0.03}, "iv": 44.0},
    {"symbol": "MV-BTC-68000-150426", "strike": 68000, "mark_price": 650.0,
     "greeks": {"delta": 0.01}, "iv": 43.5},
    {"symbol": "MV-BTC-68500-150426", "strike": 68500, "mark_price": 710.0,
     "greeks": {"delta": 0.08}, "iv": 44.5},
]
