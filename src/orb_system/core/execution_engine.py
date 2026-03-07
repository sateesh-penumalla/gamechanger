from datetime import datetime
from typing import Optional, Dict
from loguru import logger
from src.orb_system.config.database import db
from src.orb_system.config.dhan_config import dhan_client
from src.orb_system.models.data_models import TradingSignal, Position, SignalType

class ExecutionEngine:
    """Handles order execution with DhanHQ and initializes trade records"""
    
    def __init__(self):
        self.dhan = dhan_client.get_client()
        
    def execute_trade(self, signal: TradingSignal) -> Optional[Position]:
        """Place order on Dhan and initialize trade in database"""
        logger.info(f"Executing {signal.signal_type.value} trade for {signal.symbol}...")
        
        try:
             # 1. Calculate Quantity based on position size and risk
             # For now, we'll use a fixed quantity or basic logic
             quantity = 10 # Placeholder
             
             # 2. Place Order on Dhan
             order_id = self._place_dhan_order(signal, quantity)
             
             if order_id:
                 # 3. Create Trade Record in DB
                 trade_id = self._initialize_trade_db(signal, quantity, order_id)
                 
                 if trade_id:
                     # 4. Return Position object for monitoring
                     return Position(
                         trade_id=trade_id,
                         symbol=signal.symbol,
                         entry_price=signal.entry_price,
                         current_price=signal.entry_price,
                         quantity=quantity,
                         entry_time=signal.signal_time,
                         current_stop=signal.stop_loss,
                         current_target=signal.target_1,
                         next_checkpoint_type="5MIN",
                         next_checkpoint_due=signal.signal_time + timedelta(minutes=5)
                     )
             return None
             
        except Exception as e:
            logger.error(f"Error executing trade for {signal.symbol}: {e}")
            return None

    def _place_dhan_order(self, signal: TradingSignal, quantity: int) -> Optional[str]:
        """Internal call to DhanHQ order placement API"""
        if not self.dhan:
            logger.warning("Dhan client not available. Simulating order placement...")
            return "SIM_ORDER_" + datetime.now().strftime("%H%M%S")
            
        try:
             # Map signal type to Dhan transaction type
             # 'BUY' for breakout, 'SELL' for breakdown
             transaction_type = 'BUY' if signal.signal_type == SignalType.BREAKOUT else 'SELL'
             
             res = self.dhan.place_order(
                 security_id=signal.symbol, # Note: Needs correct Dhan security ID mapping
                 exchange_segment='NSE_EQ',
                 transaction_type=transaction_type,
                 quantity=quantity,
                 order_type='MARKET',
                 product_type='INTRADAY',
                 price=0 # Market order
             )
             
             if res and res.get('status') == 'success':
                 logger.info(f"Dhan order placed: {res.get('data', {}).get('orderId')}")
                 return res.get('data', {}).get('orderId')
             else:
                 logger.error(f"Dhan order failed: {res}")
                 return None
        except Exception as e:
            logger.error(f"Dhan API error: {e}")
            return None

    def _initialize_trade_db(self, signal: TradingSignal, quantity: int, order_id: str) -> Optional[int]:
        """Insert trade record into orb_trades table"""
        try:
            with db.get_cursor() as cursor:
                # Get the signal ID from orb_signals based on symbol and last signal
                cursor.execute("""
                    SELECT id FROM orb_signals 
                    WHERE symbol = %s AND status = 'PENDING' 
                    ORDER BY signal_time DESC LIMIT 1
                """, (signal.symbol,))
                sig_row = cursor.fetchone()
                signal_db_id = sig_row['id'] if sig_row else None
                
                cursor.execute("""
                    INSERT INTO orb_trades 
                    (signal_id, date, symbol, entry_time, entry_price, quantity, 
                     position_size_pct, trade_type, conviction_score, stop_loss, 
                     target_1, target_2, status, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    signal_db_id, signal.signal_time.date(), signal.symbol, 
                    signal.signal_time, signal.entry_price, quantity,
                    signal.position_size_pct, signal.signal_type.value, 
                    signal.total_conviction, signal.stop_loss, 
                    signal.target_1, signal.target_2, 'OPEN', f"OrderId: {order_id}"
                ))
                
                trade_id = cursor.lastrowid
                
                # Update signal status
                if signal_db_id:
                    cursor.execute("UPDATE orb_signals SET status = 'ENTERED' WHERE id = %s", (signal_db_id,))
                
                return trade_id
        except Exception as e:
            logger.error(f"Error initializing trade in DB: {e}")
            return None
