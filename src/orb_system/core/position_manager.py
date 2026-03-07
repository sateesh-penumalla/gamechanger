from datetime import datetime, timedelta
from typing import List, Dict, Optional
from loguru import logger
from src.orb_system.config.database import db
from src.orb_system.config.settings import settings
from src.orb_system.models.data_models import Position, TradeCheckpoint, ExitReason, SignalType
from src.orb_system.services.market_data import MarketDataService
from src.orb_system.services.technical_indicators import TechnicalIndicators
from src.agents.chartist import ChartistAgent

class PositionManager:
    """Monitors active trades and manages exits/checkpoints"""
    
    def __init__(self, market_data_service: MarketDataService):
        self.mds = market_data_service
        self.ti = TechnicalIndicators()
        self.chartist = ChartistAgent()
        
    def monitor_positions(self, positions: List[Position]):
        """Iterate through all active positions and check for exits/checkpoints"""
        for pos in positions:
            try:
                # 1. Get Latest Quote
                quote = self.mds.get_live_quote(pos.symbol)
                if not quote: continue
                
                last_price = float(quote.get('lastPrice', pos.current_price))
                pos.current_price = last_price
                
                # 2. Check for Hard Exits (Targets/SL)
                if self._check_hard_exits(pos):
                    continue
                
                # 3. Check for Checkpoints (5m/15m)
                self._manage_checkpoints(pos)
                
                # 4. Active Defense (Technical failures)
                self._check_active_defense(pos)
                
                # 5. Swing Extension (Daily EMA Trail)
                self._manage_swing_extension(pos)
                
                # 6. Update database status
                self._update_live_position_db(pos)
                
            except Exception as e:
                logger.error(f"Error monitoring position for {pos.symbol}: {e}")

    def _check_hard_exits(self, pos: Position) -> bool:
        """Check if price hit Target or Stop Loss"""
        exit_reason = None
        
        # Long Exits
        if pos.quantity > 0: # We'll use positive for long, negative for short conventionally or separate trade_type
            if pos.current_price >= pos.current_target:
                exit_reason = ExitReason.TARGET_1 # or Target 2 logic
            elif pos.current_price <= pos.current_stop:
                exit_reason = ExitReason.STOP_LOSS
        # Short Exits
        else:
             if pos.current_price <= pos.current_target:
                 exit_reason = ExitReason.TARGET_1
             elif pos.current_price >= pos.current_stop:
                 exit_reason = ExitReason.STOP_LOSS
                 
        if exit_reason:
            self.close_position(pos, exit_reason)
            return True
        return False

    def _manage_checkpoints(self, pos: Position):
        """Check if 5m/15m checkpoints are due and evaluate performance"""
        now = datetime.now()
        
        if pos.next_checkpoint_due and now >= pos.next_checkpoint_due:
             logger.info(f"Checkpoint {pos.next_checkpoint_type} due for {pos.symbol}")
             
             # Calculate move %
             move_pct = ((pos.current_price - pos.entry_price) / pos.entry_price) * 100
             if pos.quantity < 0: move_pct = -move_pct # Flip for short
             
             threshold = settings.CHECKPOINT_5MIN_MOVE_PCT if pos.next_checkpoint_type == "5MIN" else settings.CHECKPOINT_15MIN_MOVE_PCT
             
             result = "PASS" if move_pct >= threshold else ("NEUTRAL" if move_pct > 0 else "FAIL")
             
             logger.info(f"Checkpoint Result: {result} ({move_pct:.2f}%)")
             
             # Store checkpoint in database
             self._log_checkpoint(pos, move_pct, result)
             
             # Action based on result
             if result == "FAIL":
                  if pos.next_checkpoint_type == "5MIN":
                       # Optional: Exit half or tighten SL
                       logger.warning(f"5MIN Checkpoint fail for {pos.symbol}. Tightening SL.")
                       pos.current_stop = pos.entry_price # Move to BE
                  elif pos.next_checkpoint_type == "15MIN":
                       self.close_position(pos, ExitReason.CHECKPOINT_15MIN_FAIL)
                       return
             
             # Setup next checkpoint
             if pos.next_checkpoint_type == "5MIN":
                  pos.next_checkpoint_type = "15MIN"
                  pos.next_checkpoint_due = pos.entry_time + timedelta(minutes=15)
             else:
                  pos.next_checkpoint_due = None # All timed checkpoints done

    def _check_active_defense(self, pos: Position):
        """Monitor technical pulse for signs of structural failure"""
        # We fetch 1-min data for the last few candles
        df = self.mds.get_cached_data(pos.symbol, datetime.now() - timedelta(minutes=10), datetime.now())
        if df.empty or len(df) < 5: return
        
        # Logic for MACD zero cross or Supertrend flip against position
        # If it happens, trigger ExitReason.ACTIVE_DEFENSE
        pass

    def _manage_swing_extension(self, pos: Position):
         """Apply EMA-based trailing SL for high-conviction trades"""
         if pos.trail_activated:
              # Fetch daily candles for EMA trail
              pass

    def close_position(self, pos: Position, reason: ExitReason):
        """Close order on Dhan and finalize database records"""
        logger.info(f"Closing position for {pos.symbol}. Reason: {reason.value}")
        # 1. Place exit order on Dhan
        # 2. Update orb_trades table
        # 3. Update stock_performance learning table
        # 4. Remove from live_positions table
        try:
            with db.get_cursor() as cursor:
                 cursor.execute("DELETE FROM live_positions WHERE trade_id = %s", (pos.trade_id,))
                 cursor.execute("""
                     UPDATE orb_trades 
                     SET exit_time = %s, exit_price = %s, exit_reason = %s, status = 'CLOSED' 
                     WHERE id = %s
                 """, (datetime.now(), pos.current_price, reason.value, pos.trade_id))
        except Exception as e:
            logger.error(f"Error closing position: {e}")

    def _update_live_position_db(self, pos: Position):
        """Sync live position object to database monitor table"""
        try:
            with db.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO live_positions 
                    (trade_id, symbol, entry_price, current_price, quantity, 
                     next_checkpoint_type, next_checkpoint_due, current_stop, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    current_price=VALUES(current_price), status=VALUES(status)
                """, (
                    pos.trade_id, pos.symbol, pos.entry_price, pos.current_price,
                    pos.quantity, pos.next_checkpoint_type, pos.next_checkpoint_due,
                    pos.current_stop, 'ACTIVE'
                ))
        except Exception as e:
            logger.error(f"Error updating live position DB: {e}")

    def _log_checkpoint(self, pos: Position, move_pct: float, result: str):
        """Update trade record with checkpoint result"""
        col = 'checkpoint_5min' if pos.next_checkpoint_type == "5MIN" else 'checkpoint_15min'
        price_col = 'checkpoint_5min_price' if pos.next_checkpoint_type == "5MIN" else 'checkpoint_15min_price'
        
        try:
            with db.get_cursor() as cursor:
                cursor.execute(f"""
                    UPDATE orb_trades SET {col} = %s, {price_col} = %s 
                    WHERE id = %s
                """, (result, pos.current_price, pos.trade_id))
        except Exception as e:
            logger.error(f"Error logging checkpoint: {e}")
