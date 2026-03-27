"""
delta_client.py
---------------
Thin async wrapper around Delta Exchange India public REST API.
Fetches all raw data needed for the analysis engine.
No API key required for market data endpoints.
"""

import hashlib
import hmac
import httpx
import asyncio
import json
import logging
import os
import time
from typing import Optional
from datetime import datetime, timezone
import pytz

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
BASE_URL = "https://api.india.delta.exchange"


class DeltaClient:
    """
    Async HTTP client for Delta Exchange India public API.
    All methods return raw dicts/lists — parsing happens in the analysis engine.
    """

    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url
        self._client: Optional[httpx.AsyncClient] = None
        self.api_key    = os.getenv("DELTA_API_KEY", "")
        self.api_secret = os.getenv("DELTA_API_SECRET", "")
        self.paper_trade = os.getenv("PAPER_TRADE", "false").lower() == "true"
        self._product_id_cache: dict[str, int] = {}

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=15.0,
            headers={"Accept": "application/json"},
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    # ── Authentication ────────────────────────────────────────────────────────

    def _sign(self, method: str, path: str, body_str: str = "") -> dict:
        """
        Returns HMAC-SHA256 auth headers for Delta Exchange India private API.
        message = METHOD + timestamp + path + body_str
        """
        timestamp = str(int(time.time()))
        message   = method + timestamp + path + body_str
        signature = hmac.new(
            self.api_secret.encode(),
            message.encode(),
            hashlib.sha256,
        ).hexdigest()
        return {
            "api-key":   self.api_key,
            "timestamp": timestamp,
            "signature": signature,
        }

    async def _auth_post(self, path: str, body: dict) -> dict:
        body_str = json.dumps(body, separators=(",", ":"))
        headers  = self._sign("POST", path, body_str)
        headers["Content-Type"] = "application/json"
        try:
            r = await self._client.post(path, content=body_str, headers=headers)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Delta API POST error {path}: {e}")
            raise

    async def _auth_get(self, path: str, params: dict = None) -> dict:
        query_str = ""
        if params:
            query_str = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        full_path = path + query_str
        headers   = self._sign("GET", full_path)
        try:
            r = await self._client.get(path, params=params or {}, headers=headers)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Delta API auth GET error {path}: {e}")
            raise

    async def _auth_delete(self, path: str, body: dict = None) -> dict:
        body_str = json.dumps(body or {}, separators=(",", ":"))
        headers  = self._sign("DELETE", path, body_str)
        headers["Content-Type"] = "application/json"
        try:
            r = await self._client.request("DELETE", path, content=body_str, headers=headers)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Delta API DELETE error {path}: {e}")
            raise

    async def _get(self, path: str, params: dict = None) -> dict:
        try:
            r = await self._client.get(path, params=params or {})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Delta API error {path}: {e}")
            raise

    # ── BTC Spot Price ────────────────────────────────────────────────────────

    async def get_btc_spot(self) -> float:
        """Returns current BTC index price in USD."""
        data = await self._get("/v2/tickers/BTCUSD")
        return float(data["result"]["spot_price"])

    # ── Daily Straddle Chain ─────────────────────────────────────────────────

    async def get_today_straddles(self) -> list[dict]:
        """
        Returns all MV-BTC straddle contracts expiring today (IST date).
        Each dict has: symbol, strike, mark_price, volume_24h, oi, greeks.
        """
        now_ist = datetime.now(IST)
        expiry_date_str = now_ist.strftime("%d%m%y")  # e.g. "230326"

        # Get all products and filter for today's BTC straddles
        data = await self._get("/v2/products", params={
            "contract_types": "move_options",
            "underlying_asset_symbol": "BTC",
            "page_size": 200,
        })

        products = data.get("result", [])
        today_contracts = []

        for p in products:
            symbol = p.get("symbol", "")
            # Match MV-BTC-STRIKE-DDMMYY format for today
            if f"MV-BTC-" in symbol and symbol.endswith(expiry_date_str):
                today_contracts.append(p)

        if not today_contracts:
            logger.warning(f"No straddles found for expiry {expiry_date_str}")
            return []

        # Fetch tickers for each contract to get live prices + greeks
        result = []
        for contract in today_contracts:
            symbol = contract["symbol"]
            try:
                ticker_data = await self._get(f"/v2/tickers/{symbol}")
                ticker = ticker_data.get("result", {})

                # Parse strike from symbol: MV-BTC-68400-230326 → 68400
                parts = symbol.split("-")
                strike = int(parts[2]) if len(parts) >= 3 else 0

                result.append({
                    "symbol": symbol,
                    "strike": strike,
                    "mark_price": float(ticker.get("mark_price", 0) or 0),
                    "volume_24h": float(ticker.get("volume_24h", None) or ticker.get("volume", 0) or 0),
                    "oi": float(ticker.get("oi", 0) or 0),
                    "greeks": {
                        "delta": float(ticker.get("greeks", {}).get("delta", 0) or 0),
                        "gamma": float(ticker.get("greeks", {}).get("gamma", 0) or 0),
                        "theta": float(ticker.get("greeks", {}).get("theta", 0) or 0),
                        "vega": float(ticker.get("greeks", {}).get("vega", 0) or 0),
                    },
                    "iv": float(ticker.get("implied_volatility", 0) or 0),
                    "settlement_time": contract.get("settlement_time", ""),
                    "hours_to_expiry": self._hours_until(contract.get("settlement_time", "")),
                    "24h_high": float(ticker.get("high", 0) or 0),
                    "24h_low": float(ticker.get("low", 0) or 0),
                })
            except Exception as e:
                logger.warning(f"Failed to fetch ticker for {symbol}: {e}")
                continue

        # Sort by strike
        result.sort(key=lambda x: x["strike"])
        return result

    # ── Options Chain (for PCR + Max Pain calculation) ───────────────────────

    async def get_options_chain(self, expiry_date_str: str | None = None) -> list[dict]:
        """
        Fetches individual call and put options for the next active expiry.
        expiry_date_str: DDMMYY string to fetch (e.g. "280326"). If None, derives
        from the soonest live straddle so options and straddle data are always aligned.
        After today's 5:30 PM IST expiry, automatically uses the next day's chain.
        Used to calculate PCR and Max Pain from first principles.
        """
        if expiry_date_str is None:
            # Derive target date from the soonest live straddle (same logic as get_all_straddles)
            straddles = await self.get_all_straddles()
            if straddles:
                # settlement_time is UTC ISO — convert to IST date string for options filter
                settlement = straddles[0].get("settlement_time", "")
                if settlement:
                    try:
                        st_utc = datetime.fromisoformat(settlement.replace("Z", "+00:00"))
                        st_ist = st_utc.astimezone(IST)
                        expiry_date_str = st_ist.strftime("%d%m%y")
                    except Exception:
                        pass
            if expiry_date_str is None:
                # Fallback: use tomorrow if past today's settlement cutoff
                from datetime import timedelta
                now_ist = datetime.now(IST)
                cutoff  = now_ist.replace(hour=17, minute=30, second=0, microsecond=0)
                target  = now_ist + timedelta(days=1) if now_ist > cutoff else now_ist
                expiry_date_str = target.strftime("%d%m%y")

        data = await self._get("/v2/products", params={
            "contract_types": "put_options,call_options",
            "underlying_asset_symbol": "BTC",
            "page_size": 500,
        })

        products = data.get("result", [])
        today_options = []

        for p in products:
            symbol = p.get("symbol", "")
            # Filter: must be BTC option expiring today (C-BTC-... or P-BTC-...)
            if symbol.endswith(expiry_date_str) and (
                symbol.startswith("C-BTC-") or symbol.startswith("P-BTC-")
            ):
                today_options.append(p)

        # Fetch OI for each option
        result = []
        tasks = []

        async def fetch_option(product):
            symbol = product["symbol"]
            try:
                ticker_data = await self._get(f"/v2/tickers/{symbol}")
                ticker = ticker_data.get("result", {})
                contract_type = product.get("contract_type", "")

                # Parse strike — always at index 2: C-BTC-71000-260326 → parts[2] = "71000"
                parts = symbol.split("-")
                try:
                    strike = int(parts[2]) if len(parts) >= 4 else 0
                except (ValueError, IndexError):
                    strike = 0

                mark_price = float(ticker.get("mark_price", 0) or 0)
                oi = float(ticker.get("oi", 0) or 0)
                oi_value = float(ticker.get("oi_value", 0) or 0)
                if oi_value == 0 and oi > 0 and mark_price > 0:
                    oi_value = oi * mark_price
                return {
                    "symbol": symbol,
                    "strike": strike,
                    "type": "call" if "call" in contract_type.lower() or symbol.startswith("C-") else "put",
                    "oi": oi,
                    "oi_value": oi_value,
                    "mark_price": mark_price,
                    "volume": float(ticker.get("volume_24h", None) or ticker.get("volume", 0) or 0),
                }
            except Exception:
                return None

        # Run concurrently to avoid slowness
        tasks = [fetch_option(p) for p in today_options]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if r and not isinstance(r, Exception):
                result.append(r)

        return result

    # ── Historical OHLCV (for Realised Volatility) ───────────────────────────

    async def get_btc_candles(self, resolution: str = "1h", count: int = 48) -> list[dict]:
        """
        Fetches recent BTCUSD perpetual OHLCV candles.
        resolution: string — "1m", "5m", "15m", "30m", "1h", "2h", "4h", "1d"
        count: number of candles to fetch (max 500 per request)
        Returns list of {time, open, high, low, close, volume}
        """
        now_ts = int(datetime.now(timezone.utc).timestamp())

        # Map resolution string to seconds for calculating start time
        resolution_seconds = {
            "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
            "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "1d": 86400,
        }
        secs = resolution_seconds.get(resolution, 3600)
        start_ts = now_ts - (secs * count)

        data = await self._get("/v2/history/candles", params={
            "resolution": resolution,   # API expects string e.g. "1h", "5m"
            "symbol": "BTCUSD",
            "start": str(start_ts),
            "end": str(now_ts),
        })

        candles = data.get("result", [])
        if not candles:
            return []

        # Delta API returns candles as either:
        # - list of dicts: {"time": ..., "open": ..., "high": ..., "low": ..., "close": ..., "volume": ...}
        # - list of lists: [time, open, high, low, close, volume]
        # Handle both formats
        result = []
        for c in candles:
            if isinstance(c, dict):
                result.append({
                    "time": c.get("time", 0),
                    "open": float(c.get("open", 0)),
                    "high": float(c.get("high", 0)),
                    "low": float(c.get("low", 0)),
                    "close": float(c.get("close", 0)),
                    "volume": float(c.get("volume", 0)),
                })
            else:
                result.append({
                    "time": c[0],
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                })
        result.sort(key=lambda x: x["time"])  # ascending: oldest first, newest last
        return result

    # ── Time to Expiry ────────────────────────────────────────────────────────

    def hours_to_expiry(self) -> float:
        """Returns hours remaining until today's 5:30 PM IST settlement."""
        now_ist = datetime.now(IST)
        expiry_ist = now_ist.replace(hour=17, minute=30, second=0, microsecond=0)
        if now_ist > expiry_ist:
            return 0.0
        delta = expiry_ist - now_ist
        return delta.total_seconds() / 3600

    @staticmethod
    def _hours_until(settlement_time_str: str) -> float:
        """
        Returns hours remaining until a settlement_time string from the API.
        Delta Exchange returns settlement_time as a UTC ISO 8601 string.
        """
        if not settlement_time_str:
            return 0.0
        try:
            st = datetime.fromisoformat(settlement_time_str.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            return max(0.0, (st - now).total_seconds() / 3600)
        except Exception:
            return 0.0

    # ── All Straddles (multi-expiry) ──────────────────────────────────────────

    async def get_all_straddles(self, tenors: list[str] | None = None) -> list[dict]:
        """
        Fetches ALL live BTC MV straddle contracts, optionally filtered by tenor.

        tenors: list of tenor names to include. Supported values:
            "daily"   — expires within 30 hours
            "weekly"  — expires 30–200 hours from now
            "monthly" — expires 200+ hours from now
            None      — return all (no filter)

        Each returned dict has the same shape as get_today_straddles() plus
        a per-straddle "hours_to_expiry" field derived from settlement_time.
        """
        def _classify(hours: float) -> str:
            if hours <= 30:
                return "daily"
            elif hours <= 200:
                return "weekly"
            return "monthly"

        data = await self._get("/v2/products", params={
            "contract_types": "move_options",
            "underlying_asset_symbol": "BTC",
            "page_size": 200,
        })

        contracts_with_hours = []
        for p in data.get("result", []):
            symbol = p.get("symbol", "")
            if "MV-BTC-" not in symbol:
                continue
            hours = self._hours_until(p.get("settlement_time", ""))
            if hours <= 0:
                continue  # already expired
            if tenors and _classify(hours) not in tenors:
                continue
            contracts_with_hours.append((p, hours))

        if not contracts_with_hours:
            logger.warning("No live BTC straddle contracts found")
            return []

        # Fetch tickers concurrently
        result: list[dict] = []

        async def _fetch(product: dict, hours: float) -> None:
            symbol = product["symbol"]
            try:
                ticker_data = await self._get(f"/v2/tickers/{symbol}")
                ticker = ticker_data.get("result", {})
                parts = symbol.split("-")
                strike = int(parts[2]) if len(parts) >= 3 else 0
                result.append({
                    "symbol": symbol,
                    "strike": strike,
                    "mark_price": float(ticker.get("mark_price", 0) or 0),
                    "volume_24h": float(
                        ticker.get("volume_24h") or ticker.get("volume") or 0
                    ),
                    "oi": float(ticker.get("oi", 0) or 0),
                    "greeks": {
                        "delta": float(ticker.get("greeks", {}).get("delta", 0) or 0),
                        "gamma": float(ticker.get("greeks", {}).get("gamma", 0) or 0),
                        "theta": float(ticker.get("greeks", {}).get("theta", 0) or 0),
                        "vega":  float(ticker.get("greeks", {}).get("vega",  0) or 0),
                    },
                    "iv": float(ticker.get("implied_volatility", 0) or 0),
                    "settlement_time": product.get("settlement_time", ""),
                    "hours_to_expiry": hours,
                    "24h_high": float(ticker.get("high", 0) or 0),
                    "24h_low":  float(ticker.get("low",  0) or 0),
                })
            except Exception as e:
                logger.warning(f"Failed to fetch ticker for {symbol}: {e}")

        await asyncio.gather(*[_fetch(p, h) for p, h in contracts_with_hours])
        result.sort(key=lambda x: (x["hours_to_expiry"], x["strike"]))
        return result


    # ── Order Execution (authenticated) ──────────────────────────────────────

    async def place_order(
        self,
        product_id: int,
        side: str,
        size: int,
        order_type: str,
        limit_price: float | None = None,
    ) -> dict:
        """
        Place a move-options order.
        side: "sell" (entry short) or "buy" (exit / close)
        order_type: "limit_order" or "market_order"
        Returns the full order dict from the API (includes "id" and "state").
        """
        body: dict = {
            "product_id": product_id,
            "side":        side,
            "size":        size,
            "order_type":  order_type,
        }
        if order_type == "limit_order" and limit_price is not None:
            body["limit_price"] = str(limit_price)

        data = await self._auth_post("/v2/orders", body)
        return data.get("result", data)

    async def get_order(self, order_id: str) -> dict:
        """Returns current state of an order. state: open | filled | cancelled | rejected"""
        data = await self._auth_get(f"/v2/orders/{order_id}")
        return data.get("result", data)

    async def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order."""
        data = await self._auth_delete(f"/v2/orders/{order_id}", {"id": int(order_id)})
        return data.get("result", data)

    async def get_position(self, product_id: int) -> dict | None:
        """
        Returns the current open position for a product, or None.
        Used on startup to reconcile persisted state vs actual exchange state.
        """
        data = await self._auth_get("/v2/positions", params={"product_id": product_id})
        result = data.get("result")
        if not result:
            return None
        # API returns a list; find the matching position
        if isinstance(result, list):
            for pos in result:
                if pos.get("product_id") == product_id:
                    return pos
            return None
        return result

    async def get_product_id(self, symbol: str) -> int:
        """
        Returns the integer product_id for a given symbol.
        Caches results to avoid repeated API calls.
        """
        if symbol in self._product_id_cache:
            return self._product_id_cache[symbol]

        data = await self._get("/v2/products", params={
            "contract_types": "move_options",
            "underlying_asset_symbol": "BTC",
            "page_size": 200,
        })
        for p in data.get("result", []):
            sym = p.get("symbol", "")
            pid = p.get("id", 0)
            if pid:
                self._product_id_cache[sym] = int(pid)

        pid = self._product_id_cache.get(symbol, 0)
        if not pid:
            raise ValueError(f"Product ID not found for symbol: {symbol}")
        return pid


async def test_client():
    """Quick connectivity test."""
    async with DeltaClient() as client:
        spot = await client.get_btc_spot()
        print(f"BTC Spot: ${spot:,.0f}")

        straddles = await client.get_today_straddles()
        print(f"Found {len(straddles)} straddle contracts today")
        for s in straddles[:3]:
            print(f"  {s['symbol']}: ${s['mark_price']:.0f} | delta={s['greeks']['delta']:.2f}")


if __name__ == "__main__":
    asyncio.run(test_client())