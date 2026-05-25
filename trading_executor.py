"""
Module for automated execution of triangular arbitrage trading
Handles safe trade execution with error handling and emergency liquidation
"""

import asyncio
import time
import logging
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
import config
from websocket_trader import HybridTradingExecutor

# Logging configuration
logger = logging.getLogger(__name__)

class TradingExecutor:
    """Automated trading executor for triangular arbitrage"""

    def __init__(self):
        self.client = None
        self.hybrid_executor = None
        self.is_trading = False
        self.trade_count = 0
        self.success_count = 0
        self.failure_count = 0

        # Initialize the Binance client
        self._init_binance_client()

        # Initialize the WebSocket/REST hybrid executor
        self._init_hybrid_executor()

    def _init_binance_client(self):
        """Initializes the Binance client with the appropriate credentials"""
        try:
            if config.AUTO_TRADE_ENABLED:
                if not config.BINANCE_API_KEY or not config.BINANCE_SECRET_KEY:
                    raise ValueError("Binance credentials not configured")

                self.client = Client(
                    config.BINANCE_API_KEY,
                    config.BINANCE_SECRET_KEY,
                    testnet=config.DRY_RUN_MODE
                )
                logger.info(f"[OK] Binance client initialized (Testnet: {config.DRY_RUN_MODE})")
            else:
                logger.info("[INFO] Automated trading disabled - client not initialized")
        except Exception as e:
            logger.error(f"[ERR] Binance client initialization error: {e}")
            self.client = None

    def _init_hybrid_executor(self):
        """Initializes the WebSocket/REST hybrid executor"""
        try:
            if config.AUTO_TRADE_ENABLED and config.BINANCE_API_KEY and config.BINANCE_SECRET_KEY:
                self.hybrid_executor = HybridTradingExecutor()
                logger.info("[OK] WebSocket/REST hybrid executor initialized")
            else:
                logger.info("[INFO] Hybrid executor not initialized (trading disabled or credentials missing)")
        except Exception as e:
            logger.error(f"[ERR] Hybrid executor initialization error: {e}")
            self.hybrid_executor = None

    def _log_trade_result(self, result: Dict, is_error: bool = False):
        """Logs the trade result to file"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        filename = config.TRADING_ERROR_LOG_FILE if is_error else config.TRADING_LOG_FILE

        with open(filename, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {result}\n")

    def _send_telegram_notification(self, message: str):
        """Sends Telegram notification (reuses existing function)"""
        try:
            # Import the function from the main module
            from arbitraggio import send_telegram_message
            send_telegram_message(message)
        except Exception as e:
            logger.error(f"Telegram notification send error: {e}")

    async def get_account_balance(self, asset: str) -> Decimal:
        """Gets the balance of a specific asset"""
        try:
            if not self.client:
                return Decimal("0")

            account = self.client.get_account()
            for balance in account['balances']:
                if balance['asset'] == asset:
                    return Decimal(balance['free'])
            return Decimal("0")
        except Exception as e:
            logger.error(f"Balance retrieval error for {asset}: {e}")
            return Decimal("0")

    async def execute_market_order(self, symbol: str, side: str, quantity: Decimal) -> Dict:
        """Executes a market order using the hybrid executor"""
        try:
            # Use the hybrid executor if available
            if self.hybrid_executor:
                return await self.hybrid_executor.execute_market_order(symbol, side, quantity)

            # Fallback to original method if the hybrid executor is unavailable
            if not self.client:
                raise ValueError("Binance client not initialized")

            # Round the quantity to comply with stepSize
            quantity_str = f"{quantity:.8f}".rstrip('0').rstrip('.')

            order_params = {
                'symbol': symbol,
                'side': side,
                'type': 'MARKET',
                'quantity': quantity_str
            }

            if config.DRY_RUN_MODE:
                # Test mode - use test order
                result = self.client.create_test_order(**order_params)
                logger.info(f"[TEST] TEST ORDER: {side} {quantity_str} {symbol}")
                return {
                    'status': 'TEST_SUCCESS',
                    'symbol': symbol,
                    'side': side,
                    'quantity': quantity,
                    'price': None,  # Price not available in test mode
                    'method': 'rest_api'
                }
            else:
                # Real order
                result = self.client.create_order(**order_params)
                logger.info(f" REAL ORDER: {side} {quantity_str} {symbol}")
                return {
                    'status': 'SUCCESS',
                    'symbol': symbol,
                    'side': side,
                    'quantity': Decimal(result['executedQty']),
                    'price': Decimal(result['fills'][0]['price']) if result['fills'] else None,
                    'method': 'rest_api'
                }

        except BinanceAPIException as e:
            logger.error(f"Binance API error for {symbol}: {e}")
            return {'status': 'API_ERROR', 'error': str(e)}
        except BinanceOrderException as e:
            logger.error(f"Binance order error for {symbol}: {e}")
            return {'status': 'ORDER_ERROR', 'error': str(e)}
        except Exception as e:
            logger.error(f"Generic error for {symbol}: {e}")
            return {'status': 'GENERAL_ERROR', 'error': str(e)}

    async def emergency_liquidation(self, asset: str, target_asset: str, quantity: Decimal) -> Dict:
        """Emergency liquidation: sell `asset` (held) to obtain `target_asset`.

        H3 fix: side was hardcoded to 'SELL' regardless of the symbol direction.
        If the Binance pair was BTCUSDT and the held asset was USDT (quote),
        SELL would have sold BTC we don't own (insufficient balance or inverted trade).
        """
        try:
            candidate_base_asset = f"{asset}{target_asset}"      # asset is base
            candidate_quote_asset = f"{target_asset}{asset}"     # asset is quote

            symbol = None
            side = None
            if self._symbol_exists(candidate_base_asset):
                symbol = candidate_base_asset
                side = 'SELL'  # sell asset (base) for target (quote)
            elif self._symbol_exists(candidate_quote_asset):
                symbol = candidate_quote_asset
                side = 'BUY'   # use asset (quote) to buy target (base)
            else:
                raise ValueError(f"Trading pair not found for {asset}/{target_asset}")

            result = await self.execute_market_order(symbol, side, quantity)

            if result['status'] in ['SUCCESS', 'TEST_SUCCESS']:
                logger.warning(f"[SOS] Emergency liquidation completed via {side} {symbol}: {quantity} {asset} -> {target_asset}")
                return result
            else:
                logger.error(f"[ERR] Emergency liquidation failed: {result}")
                return result

        except Exception as e:
            logger.error(f"Emergency liquidation error: {e}")
            return {'status': 'LIQUIDATION_ERROR', 'error': str(e)}

    def _symbol_exists(self, symbol: str) -> bool:
        """Checks if a symbol exists"""
        try:
            if not self.client:
                return False
            self.client.get_symbol_info(symbol)
            return True
        except:
            return False

    async def execute_arbitrage(self, trading_data: Dict) -> Dict:
        """Executes the complete triangular arbitrage"""
        start_time = time.time()
        self.trade_count += 1

        # Extract data
        path = trading_data['path']
        pairs = trading_data['pairs']
        prices = trading_data['prices']
        timestamp = trading_data['timestamp']

        # Path parsing
        steps = path.split('->')
        if len(steps) != 4:
            return {'status': 'INVALID_PATH', 'error': f'Invalid path: {path}'}

        start_asset = steps[0]
        intermediate1 = steps[1]
        intermediate2 = steps[2]
        end_asset = steps[3]  # Should equal start_asset

        logger.info(f" Starting arbitrage: {path}")

        # Safety check
        if self.is_trading:
            return {'status': 'ALREADY_TRADING', 'error': 'Trading already in progress'}

        if not self.client and not self.hybrid_executor:
            return {'status': 'CLIENT_NOT_READY', 'error': 'Binance client not initialized'}

        # Set trading flag
        self.is_trading = True

        # Dictionary to track timings
        timing = {
            'preparation': 0,
            'balance_check': 0,
            'trade1': 0,
            'trade2': 0,
            'trade3': 0,
            'total': 0
        }

        try:
            # Step 1: Verify initial balance
            balance_start = time.time()
            initial_balance = await self.get_account_balance(start_asset)
            timing['balance_check'] = (time.time() - balance_start) * 1000

            if initial_balance < config.TRADE_BUDGET_USDT:
                raise ValueError(f"Insufficient balance: {initial_balance} {start_asset}")

            logger.info(f" Initial balance: {initial_balance} {start_asset} (check: {timing['balance_check']:.1f}ms)")

            # Step 2: Trade 1 (start_asset -> intermediate1)
            trade1_start = time.time()
            trade1_result = await self.execute_market_order(
                pairs[0], 'BUY', config.TRADE_BUDGET_USDT
            )
            timing['trade1'] = (time.time() - trade1_start) * 1000

            if trade1_result['status'] not in ['SUCCESS', 'TEST_SUCCESS']:
                raise ValueError(f"Trade 1 failed: {trade1_result}")

            quantity1 = trade1_result['quantity']
            method1 = trade1_result.get('method', 'unknown')
            logger.info(f"[OK] Trade 1 completed: {quantity1} {intermediate1} (time: {timing['trade1']:.1f}ms, method: {method1})")

            # Step 3: Trade 2 (intermediate1 -> intermediate2)
            trade2_start = time.time()
            trade2_result = await self.execute_market_order(
                pairs[1], 'SELL', quantity1
            )
            timing['trade2'] = (time.time() - trade2_start) * 1000

            if trade2_result['status'] not in ['SUCCESS', 'TEST_SUCCESS']:
                # Emergency liquidation
                logger.warning(f"[WARN] Trade 2 failed, emergency liquidation...")
                liquidation_result = await self.emergency_liquidation(
                    intermediate1, start_asset, quantity1
                )
                raise ValueError(f"Trade 2 failed, liquidation: {liquidation_result}")

            quantity2 = trade2_result['quantity']
            method2 = trade2_result.get('method', 'unknown')
            logger.info(f"[OK] Trade 2 completed: {quantity2} {intermediate2} (time: {timing['trade2']:.1f}ms, method: {method2})")

            # Step 4: Trade 3 (intermediate2 -> start_asset)
            trade3_start = time.time()
            trade3_result = await self.execute_market_order(
                pairs[2], 'SELL', quantity2
            )
            timing['trade3'] = (time.time() - trade3_start) * 1000

            if trade3_result['status'] not in ['SUCCESS', 'TEST_SUCCESS']:
                # Emergency liquidation
                logger.warning(f"[WARN] Trade 3 failed, emergency liquidation...")
                liquidation_result = await self.emergency_liquidation(
                    intermediate2, start_asset, quantity2
                )
                raise ValueError(f"Trade 3 failed, liquidation: {liquidation_result}")

            final_quantity = trade3_result['quantity']
            method3 = trade3_result.get('method', 'unknown')
            logger.info(f"[OK] Trade 3 completed: {final_quantity} {start_asset} (time: {timing['trade3']:.1f}ms, method: {method3})")

            # Profit/loss calculation
            profit = final_quantity - config.TRADE_BUDGET_USDT
            profit_percentage = (profit / config.TRADE_BUDGET_USDT) * 100

            # Total time calculation
            timing['total'] = (time.time() - start_time) * 1000

            # Final result
            result = {
                'status': 'SUCCESS',
                'path': path,
                'initial_amount': config.TRADE_BUDGET_USDT,
                'final_amount': final_quantity,
                'profit': profit,
                'profit_percentage': profit_percentage,
                'execution_time': timing['total'] / 1000,  # in seconds
                'timing_breakdown': timing,
                'trades': [trade1_result, trade2_result, trade3_result],
                'methods_used': [method1, method2, method3]
            }

            # Detailed timing log
            logger.info(f" TIMING BREAKDOWN:")
            logger.info(f"  - Balance check: {timing['balance_check']:.1f}ms")
            logger.info(f"  - Trade 1: {timing['trade1']:.1f}ms ({method1})")
            logger.info(f"  - Trade 2: {timing['trade2']:.1f}ms ({method2})")
            logger.info(f"  - Trade 3: {timing['trade3']:.1f}ms ({method3})")
            logger.info(f"  - TOTAL: {timing['total']:.1f}ms")

            # Hybrid executor performance stats
            if self.hybrid_executor:
                perf_stats = self.hybrid_executor.get_performance_stats()
                logger.info(f" PERFORMANCE STATS: {perf_stats}")

            # Log and notify
            self._log_trade_result(result)
            self.success_count += 1

            # Telegram notification with timing and methods
            if profit_percentage > 0:
                message = f"[OK] ARBITRAGE COMPLETED\n\n Path: {path}\n Profit: {profit_percentage:.4f}%\n Gain: {profit:.4f} {start_asset}\n Total Time: {timing['total']:.1f}ms\n Breakdown:\n  - Balance: {timing['balance_check']:.1f}ms\n  - Trade 1: {timing['trade1']:.1f}ms ({method1})\n  - Trade 2: {timing['trade2']:.1f}ms ({method2})\n  - Trade 3: {timing['trade3']:.1f}ms ({method3})"
            else:
                message = f"[WARN] ARBITRAGE COMPLETED (LOSS)\n\n Path: {path}\n Loss: {profit_percentage:.4f}%\n Loss amount: {abs(profit):.4f} {start_asset}\n Total Time: {timing['total']:.1f}ms\n Breakdown:\n  - Balance: {timing['balance_check']:.1f}ms\n  - Trade 1: {timing['trade1']:.1f}ms ({method1})\n  - Trade 2: {timing['trade2']:.1f}ms ({method2})\n  - Trade 3: {timing['trade3']:.1f}ms ({method3})"

            self._send_telegram_notification(message)

            return result

        except Exception as e:
            # Error handling
            timing['total'] = (time.time() - start_time) * 1000
            error_result = {
                'status': 'FAILED',
                'path': path,
                'error': str(e),
                'execution_time': timing['total'] / 1000,
                'timing_breakdown': timing
            }

            self._log_trade_result(error_result, is_error=True)
            self.failure_count += 1

            # Telegram notification with timing
            message = f"[ERR] ARBITRAGE FAILED\n\n Path: {path}\n[ALERT] Error: {str(e)}\n Time: {timing['total']:.1f}ms"
            self._send_telegram_notification(message)

            return error_result

        finally:
            # Reset trading flag
            self.is_trading = False

def trading_worker_with_affinity(trading_data: Dict) -> Dict:
    """Trading worker with dedicated CPU affinity"""
    import os
    import psutil

    try:
        # Set CPU affinity for this process
        current_pid = os.getpid()
        process = psutil.Process(current_pid)
        process.cpu_affinity([config.TOTAL_CORES - 1])  # Last core

        # Create the executor and run
        executor = TradingExecutor()

        # Execute synchronously (the worker is already in a separate process)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            result = loop.run_until_complete(executor.execute_arbitrage(trading_data))
            return result
        finally:
            loop.close()

    except Exception as e:
        return {
            'status': 'WORKER_ERROR',
            'error': str(e),
            'path': trading_data.get('path', 'Unknown')
        }
