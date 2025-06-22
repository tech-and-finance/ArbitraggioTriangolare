"""
Modulo per l'esecuzione automatica del trading di arbitraggio triangolare
Gestisce l'esecuzione sicura dei trade con gestione degli errori e liquidazione d'emergenza
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

# Configurazione logging
logger = logging.getLogger(__name__)

class TradingExecutor:
    """Esecutore di trading automatico per arbitraggio triangolare"""
    
    def __init__(self):
        self.client = None
        self.hybrid_executor = None
        self.is_trading = False
        self.trade_count = 0
        self.success_count = 0
        self.failure_count = 0
        
        # Inizializza il client Binance
        self._init_binance_client()
        
        # Inizializza l'executor ibrido WebSocket/REST
        self._init_hybrid_executor()
    
    def _init_binance_client(self):
        """Inizializza il client Binance con le credenziali appropriate"""
        try:
            if config.AUTO_TRADE_ENABLED:
                if not config.BINANCE_API_KEY or not config.BINANCE_SECRET_KEY:
                    raise ValueError("Credenziali Binance non configurate")
                
                self.client = Client(
                    config.BINANCE_API_KEY, 
                    config.BINANCE_SECRET_KEY,
                    testnet=config.DRY_RUN_MODE
                )
                logger.info(f"✅ Client Binance inizializzato (Testnet: {config.DRY_RUN_MODE})")
            else:
                logger.info("ℹ️ Trading automatico disabilitato - client non inizializzato")
        except Exception as e:
            logger.error(f"❌ Errore inizializzazione client Binance: {e}")
            self.client = None
    
    def _init_hybrid_executor(self):
        """Inizializza l'executor ibrido WebSocket/REST"""
        try:
            if config.AUTO_TRADE_ENABLED and config.BINANCE_API_KEY and config.BINANCE_SECRET_KEY:
                self.hybrid_executor = HybridTradingExecutor()
                logger.info("✅ Executor ibrido WebSocket/REST inizializzato")
            else:
                logger.info("ℹ️ Executor ibrido non inizializzato (trading disabilitato o credenziali mancanti)")
        except Exception as e:
            logger.error(f"❌ Errore inizializzazione executor ibrido: {e}")
            self.hybrid_executor = None
    
    def _log_trade_result(self, result: Dict, is_error: bool = False):
        """Logga il risultato del trade su file"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        filename = config.TRADING_ERROR_LOG_FILE if is_error else config.TRADING_LOG_FILE
        
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {result}\n")
    
    def _send_telegram_notification(self, message: str):
        """Invia notifica Telegram (riutilizza la funzione esistente)"""
        try:
            # Importa la funzione dal modulo principale
            from arbitraggio import send_telegram_message
            send_telegram_message(message)
        except Exception as e:
            logger.error(f"Errore invio notifica Telegram: {e}")
    
    async def get_account_balance(self, asset: str) -> Decimal:
        """Ottiene il saldo di un asset specifico"""
        try:
            if not self.client:
                return Decimal("0")
            
            account = self.client.get_account()
            for balance in account['balances']:
                if balance['asset'] == asset:
                    return Decimal(balance['free'])
            return Decimal("0")
        except Exception as e:
            logger.error(f"Errore ottenimento saldo {asset}: {e}")
            return Decimal("0")
    
    async def execute_market_order(self, symbol: str, side: str, quantity: Decimal) -> Dict:
        """Esegue un ordine di mercato usando l'executor ibrido"""
        try:
            # Usa l'executor ibrido se disponibile
            if self.hybrid_executor:
                return await self.hybrid_executor.execute_market_order(symbol, side, quantity)
            
            # Fallback al metodo originale se l'executor ibrido non è disponibile
            if not self.client:
                raise ValueError("Client Binance non inizializzato")
            
            # Arrotonda la quantità per rispettare stepSize
            quantity_str = f"{quantity:.8f}".rstrip('0').rstrip('.')
            
            order_params = {
                'symbol': symbol,
                'side': side,
                'type': 'MARKET',
                'quantity': quantity_str
            }
            
            if config.DRY_RUN_MODE:
                # Modalità test - usa order test
                result = self.client.create_test_order(**order_params)
                logger.info(f"🧪 TEST ORDER: {side} {quantity_str} {symbol}")
                return {
                    'status': 'TEST_SUCCESS',
                    'symbol': symbol,
                    'side': side,
                    'quantity': quantity,
                    'price': None,  # Prezzo non disponibile in test mode
                    'method': 'rest_api'
                }
            else:
                # Ordine reale
                result = self.client.create_order(**order_params)
                logger.info(f"📈 REAL ORDER: {side} {quantity_str} {symbol}")
                return {
                    'status': 'SUCCESS',
                    'symbol': symbol,
                    'side': side,
                    'quantity': Decimal(result['executedQty']),
                    'price': Decimal(result['fills'][0]['price']) if result['fills'] else None,
                    'method': 'rest_api'
                }
                
        except BinanceAPIException as e:
            logger.error(f"Errore API Binance per {symbol}: {e}")
            return {'status': 'API_ERROR', 'error': str(e)}
        except BinanceOrderException as e:
            logger.error(f"Errore ordine Binance per {symbol}: {e}")
            return {'status': 'ORDER_ERROR', 'error': str(e)}
        except Exception as e:
            logger.error(f"Errore generico per {symbol}: {e}")
            return {'status': 'GENERAL_ERROR', 'error': str(e)}
    
    async def emergency_liquidation(self, asset: str, target_asset: str, quantity: Decimal) -> Dict:
        """Liquidazione d'emergenza per tornare all'asset di partenza"""
        try:
            # Trova la coppia di trading
            symbol = f"{asset}{target_asset}"
            
            # Prova anche l'ordine inverso
            if not self._symbol_exists(symbol):
                symbol = f"{target_asset}{asset}"
            
            if not self._symbol_exists(symbol):
                raise ValueError(f"Coppia di trading non trovata per {asset}/{target_asset}")
            
            # Esegui la liquidazione
            result = await self.execute_market_order(symbol, 'SELL', quantity)
            
            if result['status'] in ['SUCCESS', 'TEST_SUCCESS']:
                logger.warning(f"🆘 Liquidazione d'emergenza completata: {quantity} {asset} -> {target_asset}")
                return result
            else:
                logger.error(f"❌ Liquidazione d'emergenza fallita: {result}")
                return result
                
        except Exception as e:
            logger.error(f"Errore liquidazione d'emergenza: {e}")
            return {'status': 'LIQUIDATION_ERROR', 'error': str(e)}
    
    def _symbol_exists(self, symbol: str) -> bool:
        """Verifica se un simbolo esiste"""
        try:
            if not self.client:
                return False
            self.client.get_symbol_info(symbol)
            return True
        except:
            return False
    
    async def execute_arbitrage(self, trading_data: Dict) -> Dict:
        """Esegue l'arbitraggio triangolare completo"""
        start_time = time.time()
        self.trade_count += 1
        
        # Estrai i dati
        path = trading_data['path']
        pairs = trading_data['pairs']
        prices = trading_data['prices']
        timestamp = trading_data['timestamp']
        
        # Parsing del percorso
        steps = path.split('→')
        if len(steps) != 4:
            return {'status': 'INVALID_PATH', 'error': f'Percorso non valido: {path}'}
        
        start_asset = steps[0]
        intermediate1 = steps[1]
        intermediate2 = steps[2]
        end_asset = steps[3]  # Dovrebbe essere uguale a start_asset
        
        logger.info(f"🚀 Inizio arbitraggio: {path}")
        
        # Check di sicurezza
        if self.is_trading:
            return {'status': 'ALREADY_TRADING', 'error': 'Trading già in corso'}
        
        if not self.client and not self.hybrid_executor:
            return {'status': 'CLIENT_NOT_READY', 'error': 'Client Binance non inizializzato'}
        
        # Imposta flag di trading
        self.is_trading = True
        
        # Dizionario per tracciare i tempi
        timing = {
            'preparation': 0,
            'balance_check': 0,
            'trade1': 0,
            'trade2': 0,
            'trade3': 0,
            'total': 0
        }
        
        try:
            # Step 1: Verifica saldo iniziale
            balance_start = time.time()
            initial_balance = await self.get_account_balance(start_asset)
            timing['balance_check'] = (time.time() - balance_start) * 1000
            
            if initial_balance < config.TRADE_BUDGET_USDT:
                raise ValueError(f"Saldo insufficiente: {initial_balance} {start_asset}")
            
            logger.info(f"💰 Saldo iniziale: {initial_balance} {start_asset} (verifica: {timing['balance_check']:.1f}ms)")
            
            # Step 2: Trade 1 (start_asset -> intermediate1)
            trade1_start = time.time()
            trade1_result = await self.execute_market_order(
                pairs[0], 'BUY', config.TRADE_BUDGET_USDT
            )
            timing['trade1'] = (time.time() - trade1_start) * 1000
            
            if trade1_result['status'] not in ['SUCCESS', 'TEST_SUCCESS']:
                raise ValueError(f"Trade 1 fallito: {trade1_result}")
            
            quantity1 = trade1_result['quantity']
            method1 = trade1_result.get('method', 'unknown')
            logger.info(f"✅ Trade 1 completato: {quantity1} {intermediate1} (tempo: {timing['trade1']:.1f}ms, metodo: {method1})")
            
            # Step 3: Trade 2 (intermediate1 -> intermediate2)
            trade2_start = time.time()
            trade2_result = await self.execute_market_order(
                pairs[1], 'SELL', quantity1
            )
            timing['trade2'] = (time.time() - trade2_start) * 1000
            
            if trade2_result['status'] not in ['SUCCESS', 'TEST_SUCCESS']:
                # Liquidazione d'emergenza
                logger.warning(f"⚠️ Trade 2 fallito, liquidazione d'emergenza...")
                liquidation_result = await self.emergency_liquidation(
                    intermediate1, start_asset, quantity1
                )
                raise ValueError(f"Trade 2 fallito, liquidazione: {liquidation_result}")
            
            quantity2 = trade2_result['quantity']
            method2 = trade2_result.get('method', 'unknown')
            logger.info(f"✅ Trade 2 completato: {quantity2} {intermediate2} (tempo: {timing['trade2']:.1f}ms, metodo: {method2})")
            
            # Step 4: Trade 3 (intermediate2 -> start_asset)
            trade3_start = time.time()
            trade3_result = await self.execute_market_order(
                pairs[2], 'SELL', quantity2
            )
            timing['trade3'] = (time.time() - trade3_start) * 1000
            
            if trade3_result['status'] not in ['SUCCESS', 'TEST_SUCCESS']:
                # Liquidazione d'emergenza
                logger.warning(f"⚠️ Trade 3 fallito, liquidazione d'emergenza...")
                liquidation_result = await self.emergency_liquidation(
                    intermediate2, start_asset, quantity2
                )
                raise ValueError(f"Trade 3 fallito, liquidazione: {liquidation_result}")
            
            final_quantity = trade3_result['quantity']
            method3 = trade3_result.get('method', 'unknown')
            logger.info(f"✅ Trade 3 completato: {final_quantity} {start_asset} (tempo: {timing['trade3']:.1f}ms, metodo: {method3})")
            
            # Calcolo profitto/perdita
            profit = final_quantity - config.TRADE_BUDGET_USDT
            profit_percentage = (profit / config.TRADE_BUDGET_USDT) * 100
            
            # Calcolo tempo totale
            timing['total'] = (time.time() - start_time) * 1000
            
            # Risultato finale
            result = {
                'status': 'SUCCESS',
                'path': path,
                'initial_amount': config.TRADE_BUDGET_USDT,
                'final_amount': final_quantity,
                'profit': profit,
                'profit_percentage': profit_percentage,
                'execution_time': timing['total'] / 1000,  # in secondi
                'timing_breakdown': timing,
                'trades': [trade1_result, trade2_result, trade3_result],
                'methods_used': [method1, method2, method3]
            }
            
            # Log dettagliato dei tempi
            logger.info(f"⏱️ TIMING BREAKDOWN:")
            logger.info(f"  - Verifica saldo: {timing['balance_check']:.1f}ms")
            logger.info(f"  - Trade 1: {timing['trade1']:.1f}ms ({method1})")
            logger.info(f"  - Trade 2: {timing['trade2']:.1f}ms ({method2})")
            logger.info(f"  - Trade 3: {timing['trade3']:.1f}ms ({method3})")
            logger.info(f"  - TOTALE: {timing['total']:.1f}ms")
            
            # Statistiche performance executor ibrido
            if self.hybrid_executor:
                perf_stats = self.hybrid_executor.get_performance_stats()
                logger.info(f"📊 PERFORMANCE STATS: {perf_stats}")
            
            # Log e notifica
            self._log_trade_result(result)
            self.success_count += 1
            
            # Notifica Telegram con timing e metodi
            if profit_percentage > 0:
                message = f"✅ ARBITRAGGIO COMPLETATO\n\n🔄 Percorso: {path}\n💰 Profitto: {profit_percentage:.4f}%\n💵 Guadagno: {profit:.4f} {start_asset}\n⏱️ Tempo Totale: {timing['total']:.1f}ms\n📊 Breakdown:\n  • Saldo: {timing['balance_check']:.1f}ms\n  • Trade 1: {timing['trade1']:.1f}ms ({method1})\n  • Trade 2: {timing['trade2']:.1f}ms ({method2})\n  • Trade 3: {timing['trade3']:.1f}ms ({method3})"
            else:
                message = f"⚠️ ARBITRAGGIO COMPLETATO (PERDITA)\n\n🔄 Percorso: {path}\n📉 Perdita: {profit_percentage:.4f}%\n💸 Perdita: {abs(profit):.4f} {start_asset}\n⏱️ Tempo Totale: {timing['total']:.1f}ms\n📊 Breakdown:\n  • Saldo: {timing['balance_check']:.1f}ms\n  • Trade 1: {timing['trade1']:.1f}ms ({method1})\n  • Trade 2: {timing['trade2']:.1f}ms ({method2})\n  • Trade 3: {timing['trade3']:.1f}ms ({method3})"
            
            self._send_telegram_notification(message)
            
            return result
            
        except Exception as e:
            # Gestione errori
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
            
            # Notifica Telegram con timing
            message = f"❌ ARBITRAGGIO FALLITO\n\n🔄 Percorso: {path}\n🚨 Errore: {str(e)}\n⏱️ Tempo: {timing['total']:.1f}ms"
            self._send_telegram_notification(message)
            
            return error_result
            
        finally:
            # Reset flag di trading
            self.is_trading = False

def trading_worker_with_affinity(trading_data: Dict) -> Dict:
    """Worker di trading con affinità CPU dedicata"""
    import os
    import psutil
    
    try:
        # Imposta l'affinità CPU per questo processo
        current_pid = os.getpid()
        process = psutil.Process(current_pid)
        process.cpu_affinity([config.TOTAL_CORES - 1])  # Ultimo core
        
        # Crea l'executor e esegui
        executor = TradingExecutor()
        
        # Esegui in modo sincrono (il worker è già in un processo separato)
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