"""
Module for WebSocket-based trading on Binance
Handles ultra-fast orders via persistent WebSocket connection
"""

import asyncio
import json
import hmac
import hashlib
import time
import logging
from decimal import Decimal
from typing import Dict, Optional
import websockets
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
import config

logger = logging.getLogger(__name__)

# H4 fix: the `.closed` attribute on WebSocketClientProtocol was removed in
# websockets >= 14 (replaced by `.state`). Helper compatible with both
# versions to avoid blocking future library upgrades.
try:
    from websockets.protocol import State as _WSState
except ImportError:
    _WSState = None


def _is_ws_alive(ws):
    if ws is None:
        return False
    if _WSState is not None and hasattr(ws, 'state'):
        try:
            return ws.state == _WSState.OPEN
        except Exception:
            pass
    if hasattr(ws, 'closed'):
        return not ws.closed
    return True

class BinanceWebSocketTrader:
    """WebSocket trader for ultra-fast orders on Binance"""

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        # C5 fix: trading endpoint (NOT market data endpoint).
        # Market data was wss://stream.binance.com:9443/ws/ which silently rejects order.place commands.
        self.ws_url = (
            "wss://ws-api.testnet.binance.vision/ws-api/v3"
            if config.DRY_RUN_MODE else
            "wss://ws-api.binance.com:443/ws-api/v3"
        )
        self.websocket = None
        self.connected = False
        self.last_ping = 0
        self.ping_interval = 20  # seconds
        self.request_id = 0

    async def connect(self):
        """Establishes persistent WebSocket connection"""
        try:
            if _is_ws_alive(self.websocket):
                return

            self.websocket = await websockets.connect(
                self.ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            )
            self.connected = True
            logger.info("✅ WebSocket trading connection established")

            # Start task to keep the connection alive
            asyncio.create_task(self._keep_alive())

        except Exception as e:
            logger.error(f"❌ WebSocket trading connection error: {e}")
            self.connected = False
            raise

    async def disconnect(self):
        """Closes the WebSocket connection"""
        if _is_ws_alive(self.websocket):
            await self.websocket.close()
            self.connected = False
            logger.info("🔌 WebSocket trading connection closed")

    async def _keep_alive(self):
        """Keeps the WebSocket connection alive"""
        while self.connected:
            try:
                await asyncio.sleep(self.ping_interval)
                if _is_ws_alive(self.websocket):
                    await self.websocket.ping()
                    self.last_ping = time.time()
            except Exception as e:
                logger.warning(f"⚠️ WebSocket keep-alive error: {e}")
                await self._reconnect()

    async def _reconnect(self):
        """Reconnects automatically"""
        try:
            logger.info("🔄 Reconnecting WebSocket trading...")
            await self.disconnect()
            await asyncio.sleep(1)
            await self.connect()
        except Exception as e:
            logger.error(f"❌ WebSocket reconnection error: {e}")

    def _generate_signature(self, params: Dict) -> str:
        """Generates HMAC signature for authentication"""
        query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    def _get_request_id(self) -> int:
        """Generates unique ID for requests"""
        self.request_id += 1
        return int(time.time() * 1000) + self.request_id

    async def place_market_order(self, symbol: str, side: str, quantity: Decimal) -> Dict:
        """Places a market order via WebSocket"""
        if not self.connected:
            await self.connect()

        timestamp = int(time.time() * 1000)

        # Order parameters
        params = {
            'symbol': symbol,
            'side': side,
            'type': 'MARKET',
            'quantity': f"{quantity:.8f}".rstrip('0').rstrip('.'),
            'timestamp': timestamp
        }

        # Generate signature
        signature = self._generate_signature(params)
        params['signature'] = signature

        # Prepare WebSocket request
        request = {
            'method': 'order.place',
            'id': self._get_request_id(),
            'params': params
        }

        start_time = time.time()

        try:
            # Send order
            await self.websocket.send(json.dumps(request))
            logger.info(f"⚡ WS ORDER SENT: {side} {quantity} {symbol}")

            # Wait for response with timeout
            response = await asyncio.wait_for(
                self.websocket.recv(),
                timeout=5.0  # 5 second timeout
            )

            response_data = json.loads(response)
            execution_time = (time.time() - start_time) * 1000

            # Verify response
            if 'result' in response_data and response_data['result'].get('status') == 'FILLED':
                result = response_data['result']
                logger.info(f"✅ WS ORDER SUCCESS: {side} {quantity} {symbol} ({execution_time:.1f}ms)")
                
                return {
                    'status': 'SUCCESS',
                    'symbol': symbol,
                    'side': side,
                    'quantity': Decimal(result.get('executedQty', str(quantity))),
                    'price': Decimal(result.get('price', '0')),
                    'execution_time': execution_time,
                    'order_id': result.get('orderId'),
                    'method': 'websocket'
                }
            else:
                error_msg = response_data.get('error', {}).get('msg', 'Unknown error')
                logger.error(f"❌ WS ORDER ERROR: {error_msg}")
                raise BinanceAPIException(f"WebSocket order failed: {error_msg}")
                
        except asyncio.TimeoutError:
            logger.error(f"⏰ WS ORDER TIMEOUT: {side} {quantity} {symbol}")
            raise BinanceAPIException("WebSocket order timeout")
        except Exception as e:
            logger.error(f"❌ WS ORDER EXCEPTION: {e}")
            raise
    
    async def get_account_info(self) -> Dict:
        """Gets account info via WebSocket"""
        if not self.connected:
            await self.connect()
        
        timestamp = int(time.time() * 1000)
        params = {'timestamp': timestamp}
        signature = self._generate_signature(params)
        params['signature'] = signature
        
        request = {
            'method': 'account.status',
            'id': self._get_request_id(),
            'params': params
        }
        
        try:
            await self.websocket.send(json.dumps(request))
            response = await asyncio.wait_for(self.websocket.recv(), timeout=5.0)
            return json.loads(response)
        except Exception as e:
            logger.error(f"❌ WS ACCOUNT INFO ERROR: {e}")
            raise
    
    def is_connected(self) -> bool:
        """Checks if the WebSocket connection is active"""
        return self.connected and _is_ws_alive(self.websocket)

class HybridTradingExecutor:
    """Hybrid executor using WebSocket with REST API fallback"""

    def __init__(self):
        self.ws_trader = None
        self.rest_client = None
        self.use_websocket = True
        self.ws_failures = 0
        self.max_ws_failures = 3

        # C4 fix: initialize BOTH clients if credentials are available.
        # The rest_client is the fallback when WebSocket fails >= max_ws_failures times.
        if config.BINANCE_API_KEY and config.BINANCE_SECRET_KEY:
            self.ws_trader = BinanceWebSocketTrader(
                config.BINANCE_API_KEY,
                config.BINANCE_SECRET_KEY
            )
            try:
                self.rest_client = Client(
                    config.BINANCE_API_KEY,
                    config.BINANCE_SECRET_KEY,
                    testnet=config.DRY_RUN_MODE
                )
                logger.info(f"✅ REST fallback client initialized (testnet={config.DRY_RUN_MODE})")
            except Exception as e:
                logger.error(f"❌ REST client init failed: {e}")
                self.rest_client = None

    async def execute_market_order(self, symbol: str, side: str, quantity: Decimal) -> Dict:
        """Executes a market order with automatic fallback"""

        # Try WebSocket if enabled and available
        if (self.use_websocket and
            self.ws_trader and
            self.ws_trader.is_connected()):

            try:
                result = await self.ws_trader.place_market_order(symbol, side, quantity)
                self.ws_failures = 0  # Reset failure counter
                return result

            except Exception as e:
                self.ws_failures += 1
                logger.warning(f"⚠️ WebSocket failed ({self.ws_failures}/{self.max_ws_failures}): {e}")

                # Disable WebSocket if too many failures
                if self.ws_failures >= self.max_ws_failures:
                    logger.warning("🔄 Too many WebSocket failures, switching to REST API")
                    self.use_websocket = False

        # Fallback to REST API
        if self.rest_client:
            logger.info(f"📡 Using REST API for {side} {quantity} {symbol}")
            return await self._execute_rest_order(symbol, side, quantity)
        else:
            raise Exception("No trading client available")

    async def _execute_rest_order(self, symbol: str, side: str, quantity: Decimal) -> Dict:
        """Executes order via REST API (fallback). C4 fix: real implementation (was placeholder)."""
        if not self.rest_client:
            raise Exception("REST client not initialized (Binance credentials missing)")

        quantity_str = f"{quantity:.8f}".rstrip('0').rstrip('.')
        params = {
            'symbol': symbol,
            'side': side,
            'type': 'MARKET',
            'quantity': quantity_str,
        }
        start_time = time.time()
        try:
            if config.DRY_RUN_MODE:
                # Test order: validates parameters but does not execute
                await asyncio.to_thread(self.rest_client.create_test_order, **params)
                logger.info(f"🧪 REST TEST ORDER: {side} {quantity_str} {symbol}")
                return {
                    'status': 'TEST_SUCCESS',
                    'symbol': symbol,
                    'side': side,
                    'quantity': quantity,
                    'price': None,
                    'execution_time': (time.time() - start_time) * 1000,
                    'method': 'rest_api',
                }
            else:
                result = await asyncio.to_thread(self.rest_client.create_order, **params)
                logger.info(f"📈 REST REAL ORDER: {side} {quantity_str} {symbol}")
                price = Decimal(result['fills'][0]['price']) if result.get('fills') else None
                return {
                    'status': 'SUCCESS',
                    'symbol': symbol,
                    'side': side,
                    'quantity': Decimal(result['executedQty']),
                    'price': price,
                    'execution_time': (time.time() - start_time) * 1000,
                    'order_id': result.get('orderId'),
                    'method': 'rest_api',
                }
        except BinanceAPIException as e:
            logger.error(f"REST API Binance error for {symbol}: {e}")
            return {'status': 'API_ERROR', 'error': str(e), 'method': 'rest_api'}
        except BinanceOrderException as e:
            logger.error(f"REST order Binance error for {symbol}: {e}")
            return {'status': 'ORDER_ERROR', 'error': str(e), 'method': 'rest_api'}
        except Exception as e:
            logger.error(f"REST generic error for {symbol}: {e}")
            return {'status': 'GENERAL_ERROR', 'error': str(e), 'method': 'rest_api'}

    async def connect_websocket(self):
        """Connects the WebSocket trader"""
        if self.ws_trader:
            await self.ws_trader.connect()

    async def disconnect_websocket(self):
        """Disconnects the WebSocket trader"""
        if self.ws_trader:
            await self.ws_trader.disconnect()

    def get_performance_stats(self) -> Dict:
        """Returns performance statistics"""
        return {
            'websocket_enabled': self.use_websocket,
            'websocket_connected': self.ws_trader.is_connected() if self.ws_trader else False,
            'websocket_failures': self.ws_failures,
            'method_preference': 'websocket' if self.use_websocket else 'rest_api'
        } 