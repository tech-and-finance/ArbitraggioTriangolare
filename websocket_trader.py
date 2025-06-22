"""
Modulo per il trading via WebSocket su Binance
Gestisce ordini ultra-veloci con connessione WebSocket persistente
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
from binance.exceptions import BinanceAPIException
import config

logger = logging.getLogger(__name__)

class BinanceWebSocketTrader:
    """Trader WebSocket per ordini ultra-veloci su Binance"""
    
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.ws_url = "wss://stream.binance.com:9443/ws/"
        self.websocket = None
        self.connected = False
        self.last_ping = 0
        self.ping_interval = 20  # secondi
        self.request_id = 0
        
    async def connect(self):
        """Stabilisce connessione WebSocket persistente"""
        try:
            if self.websocket and not self.websocket.closed:
                return
                
            self.websocket = await websockets.connect(
                self.ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            )
            self.connected = True
            logger.info("✅ Connessione WebSocket trading stabilita")
            
            # Avvia task per mantenere la connessione
            asyncio.create_task(self._keep_alive())
            
        except Exception as e:
            logger.error(f"❌ Errore connessione WebSocket trading: {e}")
            self.connected = False
            raise
    
    async def disconnect(self):
        """Chiude la connessione WebSocket"""
        if self.websocket and not self.websocket.closed:
            await self.websocket.close()
            self.connected = False
            logger.info("🔌 Connessione WebSocket trading chiusa")
    
    async def _keep_alive(self):
        """Mantiene la connessione WebSocket attiva"""
        while self.connected:
            try:
                await asyncio.sleep(self.ping_interval)
                if self.websocket and not self.websocket.closed:
                    await self.websocket.ping()
                    self.last_ping = time.time()
            except Exception as e:
                logger.warning(f"⚠️ Errore keep-alive WebSocket: {e}")
                await self._reconnect()
    
    async def _reconnect(self):
        """Riconnette automaticamente"""
        try:
            logger.info("🔄 Riconnessione WebSocket trading...")
            await self.disconnect()
            await asyncio.sleep(1)
            await self.connect()
        except Exception as e:
            logger.error(f"❌ Errore riconnessione WebSocket: {e}")
    
    def _generate_signature(self, params: Dict) -> str:
        """Genera firma HMAC per autenticazione"""
        query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _get_request_id(self) -> int:
        """Genera ID univoco per le richieste"""
        self.request_id += 1
        return int(time.time() * 1000) + self.request_id
    
    async def place_market_order(self, symbol: str, side: str, quantity: Decimal) -> Dict:
        """Piazza ordine di mercato via WebSocket"""
        if not self.connected:
            await self.connect()
        
        timestamp = int(time.time() * 1000)
        
        # Parametri dell'ordine
        params = {
            'symbol': symbol,
            'side': side,
            'type': 'MARKET',
            'quantity': f"{quantity:.8f}".rstrip('0').rstrip('.'),
            'timestamp': timestamp
        }
        
        # Genera firma
        signature = self._generate_signature(params)
        params['signature'] = signature
        
        # Prepara richiesta WebSocket
        request = {
            'method': 'order.place',
            'id': self._get_request_id(),
            'params': params
        }
        
        start_time = time.time()
        
        try:
            # Invia ordine
            await self.websocket.send(json.dumps(request))
            logger.info(f"⚡ WS ORDER SENT: {side} {quantity} {symbol}")
            
            # Attendi risposta con timeout
            response = await asyncio.wait_for(
                self.websocket.recv(), 
                timeout=5.0  # 5 secondi timeout
            )
            
            response_data = json.loads(response)
            execution_time = (time.time() - start_time) * 1000
            
            # Verifica risposta
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
        """Ottiene informazioni account via WebSocket"""
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
        """Verifica se la connessione WebSocket è attiva"""
        return self.connected and self.websocket and not self.websocket.closed

class HybridTradingExecutor:
    """Executor ibrido che usa WebSocket con fallback a REST API"""
    
    def __init__(self):
        self.ws_trader = None
        self.rest_client = None
        self.use_websocket = True
        self.ws_failures = 0
        self.max_ws_failures = 3
        
        # Inizializza WebSocket trader se le credenziali sono disponibili
        if config.BINANCE_API_KEY and config.BINANCE_SECRET_KEY:
            self.ws_trader = BinanceWebSocketTrader(
                config.BINANCE_API_KEY, 
                config.BINANCE_SECRET_KEY
            )
    
    async def execute_market_order(self, symbol: str, side: str, quantity: Decimal) -> Dict:
        """Esegue ordine di mercato con fallback automatico"""
        
        # Prova WebSocket se abilitato e disponibile
        if (self.use_websocket and 
            self.ws_trader and 
            self.ws_trader.is_connected()):
            
            try:
                result = await self.ws_trader.place_market_order(symbol, side, quantity)
                self.ws_failures = 0  # Reset contatore fallimenti
                return result
                
            except Exception as e:
                self.ws_failures += 1
                logger.warning(f"⚠️ WebSocket fallito ({self.ws_failures}/{self.max_ws_failures}): {e}")
                
                # Disabilita WebSocket se troppi fallimenti
                if self.ws_failures >= self.max_ws_failures:
                    logger.warning("🔄 Troppi fallimenti WebSocket, passaggio a REST API")
                    self.use_websocket = False
        
        # Fallback a REST API
        if self.rest_client:
            logger.info(f"📡 Usando REST API per {side} {quantity} {symbol}")
            return await self._execute_rest_order(symbol, side, quantity)
        else:
            raise Exception("Nessun client trading disponibile")
    
    async def _execute_rest_order(self, symbol: str, side: str, quantity: Decimal) -> Dict:
        """Esegue ordine via REST API (fallback)"""
        # Implementazione REST API (già presente nel trading_executor.py)
        # Per ora restituiamo un placeholder
        return {
            'status': 'REST_FALLBACK',
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'method': 'rest_api'
        }
    
    async def connect_websocket(self):
        """Connette il WebSocket trader"""
        if self.ws_trader:
            await self.ws_trader.connect()
    
    async def disconnect_websocket(self):
        """Disconnette il WebSocket trader"""
        if self.ws_trader:
            await self.ws_trader.disconnect()
    
    def get_performance_stats(self) -> Dict:
        """Restituisce statistiche di performance"""
        return {
            'websocket_enabled': self.use_websocket,
            'websocket_connected': self.ws_trader.is_connected() if self.ws_trader else False,
            'websocket_failures': self.ws_failures,
            'method_preference': 'websocket' if self.use_websocket else 'rest_api'
        } 