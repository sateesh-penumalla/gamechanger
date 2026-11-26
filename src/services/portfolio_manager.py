
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import os
import time
import json
from datetime import datetime
from src.db.schema import Position, IntradayTick, SystemJob
from src.data.dhan_client import DhanDataClient
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

class PortfolioManager:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.dhan_client = DhanDataClient() # For execution

    def _safe_audit_log(self, pos, message):
        """Append to agent_audit_log safely, handling NoneType."""
        curr = pos.agent_audit_log or ""
        pos.agent_audit_log = curr + message

    def _get_config(self, session):
        """Standardized config loader: JSON presets + DB overrides."""
        try:
            # 1. Load Strategy Presets (Base)
            preset_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'strategy_presets.json')
            with open(preset_file, "r") as f:
                presets = json.load(f)
            config = presets.get("sateesh", {}).copy()

            # 2. Merge with DB Job Config (Overrides)
            job = session.query(SystemJob).filter_by(job_id='portfolio_manager').first()
            if job and job.config:
                config.update(job.config)
            
            return config
        except Exception as e:
            logger.error(f"Error loading PortfolioManager config: {e}")
            # Fallback to DB only
            job = session.query(SystemJob).filter_by(job_id='portfolio_manager').first()
            return job.config if job and job.config else {}

    def run_cycle(self):
        """Manages lifecycle of positions: Pending -> Open -> Closed."""
        session = self.Session()
        try:
            # 1. Process PENDING signals (Entry Execution)
            config = self._get_config(session)
            
            pending_pos = session.query(Position).filter(Position.status == 'PENDING').all()
            for pos in pending_pos:
                # Initialize TP/SL based on latest config if not set
                if not pos.tp:
                    tp_mult = 1 + (config.get('tp_pct', 2.0) / 100) if pos.side == 'LONG' else 1 - (config.get('tp_pct', 2.0) / 100)
                    pos.tp = pos.entry_price * tp_mult

                self._execute_entry(session, pos)

            # 2. Manage OPEN positions (Exit Logic)
            open_pos = session.query(Position).filter(Position.status == 'OPEN').all()
            for pos in open_pos:
                self._manage_trade(session, pos, config)
            
            session.commit()
            if pending_pos or open_pos:
                logger.info(f"PortfolioManager: Managed {len(pending_pos)} pending, {len(open_pos)} open positions.")
                
        except Exception as e:
            logger.error(f"PortfolioManager Error: {e}")
            session.rollback()
        finally:
            session.close()

    def _round_to_tick(self, price, tick=0.05):
        """Standardized price rounding for Dhan/NSE to avoid tick-size rejections."""
        if price is None: return 0.0
        return round(round(float(price) / tick) * tick, 2)

    def _execute_entry(self, session, pos) -> tuple[bool, str]:
        """Places order on Dhan and updates status to OPEN. Returns (success, message)."""
        # --- DOUBLE-ENTRY GUARD ---
        # 1. Proactive Sync: If we think it's OPEN, check Dhan one last time.
        # This handles cases where Dhan closed the position but our DB is lagging.
        existing = session.query(Position).filter(
            Position.symbol == pos.symbol,
            Position.status == 'OPEN'
        ).first()
        
        if existing:
            try:
                logger.info(f"Double-Entry Guard: Checking Dhan status for existing {pos.symbol} position...")
                d_pos = self.dhan_client.get_positions()
                remote_qty = 0
                for dp in d_pos:
                    if dp.get('tradingSymbol', '').replace(".NS", "") == pos.symbol:
                        remote_qty = abs(int(dp.get('netQty', 0)))
                        break
                
                if remote_qty == 0:
                    logger.info(f"✅ Dhan confirmed {pos.symbol} is CLOSED. Syncing DB before allowing new entry.")
                    existing.status = 'CLOSED'
                    existing.exit_time = datetime.now()
                    existing.exit_reason = "Exchange Sync"
                    self._safe_audit_log(existing, "\n[Sync] Closed on Dhan. Guard cleared.")
                    session.flush()
                    existing = None # Clear the guard!
            except Exception as e:
                logger.error(f"Error during proactive guard sync: {e}")

        if existing:
            msg = f"Skipped: Position already OPEN for {pos.symbol}"
            logger.warning(msg)
            self._safe_audit_log(pos, f"\n[Order Skipped] {msg}")
            pos.status = 'FAILED'
            pos.qty = 0 # Mark as effectively empty
            return False, msg

        logger.info(f"Executing Entry for {pos.symbol} ({pos.side})...")
        
        # 1. Calculate Limit Price with 0.05% buffer
        entry_price = float(pos.entry_price or 0)
        buffer_pct = 0.0005 # 0.05%
        limit_price = entry_price * (1 + buffer_pct) if pos.side == 'LONG' else entry_price * (1 - buffer_pct)
        # Final rounding to nearest 0.05 and sanitized to 2 decimal places
        limit_price = self._round_to_tick(limit_price)

        # --- DYNAMIC QUANTITY CALCULATION ---
        calculated_qty = 1
        try:
            available_cash = self.dhan_client.get_fund_limits()
            logger.info(f"Funds Available: ₹{available_cash}")
            
            job_config = self._get_config(session)
            max_capital = float(job_config.get('max_capital', 50000)) 
            min_capital = float(job_config.get('min_capital', 5000)) 

            deployable = min(max_capital, available_cash * 0.95)
            
            if deployable < min_capital:
                msg = f"Skipped: Funds (₹{deployable:.2f}) < Min Capital (₹{min_capital})"
                logger.warning(msg)
                self._safe_audit_log(pos, f"\n[Order Skipped] {msg}")
                return False, msg
            
            calculated_qty = int(deployable // limit_price)
            
            # --- LOT SIZE AWARENESS ---
            lot_size = self.dhan_client.get_lot_size(pos.symbol)
            if lot_size > 1:
                # Round DOWN to nearest lot multiple
                adjusted_qty = (calculated_qty // lot_size) * lot_size
                logger.info(f"Lot Size Adjustment: {pos.symbol} qty {calculated_qty} -> {adjusted_qty} (Lot: {lot_size})")
                calculated_qty = adjusted_qty

            if calculated_qty < lot_size: 
                msg = f"Skipped: Calculated Quantity {calculated_qty} < Min Lot {lot_size}"
                logger.warning(msg)
                # Safe append
                log_curr = pos.agent_audit_log or ""
                pos.agent_audit_log = log_curr + f"\n[Order Skipped] {msg}"
                return False, msg
            
            logger.info(f"Position Sizing: {pos.symbol} @ {limit_price} | Cap: {max_capital} | Avail: {available_cash} -> Qty: {calculated_qty}")
            
        except Exception as e:
            logger.error(f"Error calculating quantity: {e}")
            # If we hit an exception during calculation, we must NOT default to 1 if it's a lot-size sensitive stock
            return False, f"Quantity calculation error: {e}"

        pos.qty = calculated_qty

        # 2. Real API Call to Dhan
        if pos.sl and pos.tp:
            # Round SL and TP strings/floats right before sending to be safe
            tp_sanitized = self._round_to_tick(pos.tp)
            sl_sanitized = self._round_to_tick(pos.sl)

            logger.info(f"Placing SUPER order for {pos.symbol} at {limit_price} | TP: {tp_sanitized} | SL: {sl_sanitized}...")
            order_res = self.dhan_client.place_super_order(
                symbol=pos.symbol,
                side=pos.side,
                entry_price=limit_price,
                target_price=tp_sanitized,
                sl_price=sl_sanitized,
                qty=pos.qty or 1
            )

            if order_res and order_res.get('status') == 'success':
                entry_id = order_res.get('entry_order_id')
                target_id = order_res.get('target_order_id')
                sl_id = order_res.get('sl_order_id')

                pos.status = 'OPEN'
                pos.entry_time = datetime.now()
                pos.entry_order_id = entry_id
                pos.target_order_id = target_id
                pos.sl_order_id = sl_id
                self._safe_audit_log(pos, f"\n[Super Order] Linked Trio Placed. Entry: {entry_id}, Target: {target_id}, SL: {sl_id}")
                
                logger.info(f"Position {pos.symbol} is now OPEN via SUPER ORDER.")
                return True, "Executed Successfully"
            else:
                err = order_res.get('error') if order_res else "Dhan Super Order API Failed"
                pos.status = 'SIGNALED'
                self._safe_audit_log(pos, f"\n[Super Order] FAILED: {err}")
                return False, f"Dhan Super Order Failed: {err}"

        # --- FALLBACK: Standard Order Placement ---
        transaction_type = "BUY" if pos.side == 'LONG' else "SELL"
        logger.info(f"Placing LIMIT order for {pos.symbol} at {limit_price} (Buffer applied)...")
        
        order_res = self.dhan_client.place_order(
            symbol=pos.symbol,
            transaction_type=transaction_type,
            qty=pos.qty or 1,
            order_type="LIMIT",
            price=limit_price
        )

        if order_res and order_res.get('orderId'):
            order_id = order_res.get('orderId')
            
            time.sleep(0.5) 
            order_details = self.dhan_client.get_order_status(order_id)
            details_str = str(order_details) if order_details else "Details unavailable"

            pos.status = 'OPEN' 
            pos.entry_time = datetime.now()
            pos.entry_order_id = order_id
            self._safe_audit_log(pos, f"\n[Order] Placed Dhan {transaction_type} LIMIT Order ID: {order_id}")
            self._safe_audit_log(pos, f"\n[Order Metadata]: {details_str}")
            
            logger.info(f"Position {pos.symbol} is now OPEN (OrderID: {order_id}).")

            # --- 2. Place Exchange-Level Stop Loss for Safety ---
            if pos.sl and pos.sl > 0:
                sl_side = "SELL" if pos.side == 'LONG' else "BUY"
                try:
                    sl_res = self.dhan_client.place_order(
                        symbol=pos.symbol,
                        transaction_type=sl_side,
                        qty=pos.qty or 1,
                        order_type="STOP_LOSS_MARKET",
                        trigger_price=float(pos.sl)
                    )
                    if sl_res and sl_res.get('orderId'):
                        pos.sl_order_id = sl_res.get('orderId')
                        self._safe_audit_log(pos, f"\n[SL] Placed Exchange SL-M Order ID: {pos.sl_order_id} at {pos.sl}")
                except Exception as sle:
                    self._safe_audit_log(pos, f"\n[SL] FATAL ERROR during SL placement: {sle}")
            
            return True, "Executed Successfully"
        else:
            pos.status = 'SIGNALED' 
            self._safe_audit_log(pos, f"\n[Order] FAILED to place Dhan order.")
            return False, "Dhan API Order Placement Failed"

    def _execute_exit(self, session, pos, reason):
        """Closes position on Dhan and local DB."""
        logger.info(f"Executing EXIT for {pos.symbol} ({reason})...")
        
        transaction_type = "SELL" if pos.side == 'LONG' else "BUY"
        order_res = self.dhan_client.place_order(
            symbol=pos.symbol,
            transaction_type=transaction_type,
            qty=pos.qty or 1
        )

        if order_res and order_res.get('orderId'):
            order_id = order_res.get('orderId')
            pos.status = 'CLOSED'
            pos.exit_time = datetime.now()
            pos.exit_reason = reason
            self._safe_audit_log(pos, f"\n[Exit] Placed Dhan {transaction_type} Order ID: {order_id}")
            logger.info(f"Position {pos.symbol} is now CLOSED (Exit OrderID: {order_id}).")
        else:
            self._safe_audit_log(pos, f"\n[Exit] FAILED to place Dhan exit order.")
            logger.info(f"Position {pos.symbol} exit FAILED.")

    def _manage_trade(self, session, pos, config):
        """Checks SL/TP/Trailing for an open position."""
        # --- PROACTIVE SYNC: Check if Dhan closed this already ---
        if pos.status == 'OPEN':
            try:
                # Throttle Dhan API calls? Maybe only every 10 seconds per symbol?
                # For now, lean check:
                d_pos = self.dhan_client.get_positions()
                remote_qty = 0
                for dp in d_pos:
                    if dp.get('tradingSymbol', '').replace(".NS", "") == pos.symbol:
                        remote_qty = abs(int(dp.get('netQty', 0)))
                        break
                
                if remote_qty == 0:
                    logger.info(f"✅ Auto-Sync: {pos.symbol} found CLOSED on Dhan. Updating DB.")
                    pos.status = 'CLOSED'
                    pos.exit_time = datetime.now()
                    pos.exit_reason = "Dhan Sync"
                    self._safe_audit_log(pos, "\n[Sync] Detected closed on Dhan exchange side.")
                    return # No more management needed
            except Exception as e:
                logger.error(f"Error during manage_trade sync: {e}")

        # Get latest price
        last_tick = session.query(IntradayTick).filter(IntradayTick.symbol == pos.symbol)\
                    .order_by(IntradayTick.timestamp.desc()).first()
        
        if not last_tick: return
        
        curr_price = last_tick.close
        
        # 1. Update Max Profit (High Watermark)
        if not pos.entry_price or pos.entry_price == 0:
            cur_pnl_pct = 0.0
        elif pos.side == 'LONG':
            cur_pnl_pct = ((curr_price - pos.entry_price) / pos.entry_price) * 100
        else:
            cur_pnl_pct = ((pos.entry_price - curr_price) / pos.entry_price) * 100
            
        if cur_pnl_pct > pos.max_profit:
            pos.max_profit = cur_pnl_pct
            
        # 2. Check Exits
        exit_triggered = False
        reason = ""
        
        # Stop Loss (Safety check for None)
        if pos.sl is not None:
            if (pos.side == 'LONG' and curr_price <= pos.sl) or \
               (pos.side == 'SHORT' and curr_price >= pos.sl):
                exit_triggered = True
                reason = "SL Hit"
            
        # Take Profit (Safety check for None)
        if not exit_triggered and pos.tp is not None:
            if (pos.side == 'LONG' and curr_price >= pos.tp) or \
               (pos.side == 'SHORT' and curr_price <= pos.tp):
                exit_triggered = True
                reason = "TP Hit"
            
        # Trailing SL (Rider Logic - Simplified for now)
        # if pos.rider_active and ...
        
        if exit_triggered:
            # If it's a Super Order, check Dhan first to avoid double-entry/exit
            if pos.target_order_id or pos.sl_order_id:
                logger.info(f"Exit Triggered locally for {pos.symbol}. Checking Dhan status...")
                try:
                    d_pos = self.dhan_client.get_positions()
                    remote_qty = 0
                    for dp in d_pos:
                        if dp.get('tradingSymbol', '').replace(".NS", "") == pos.symbol:
                            remote_qty = abs(int(dp.get('netQty', 0)))
                            break
                    
                    if remote_qty == 0:
                        logger.info(f"✅ Dhan already closed {pos.symbol} (Super Order Hit). Syncing status.")
                        pos.status = 'CLOSED'
                        pos.exit_time = datetime.now()
                        pos.exit_reason = reason
                        pos.pnl_pct = cur_pnl_pct
                        pos.pnl_abs = (curr_price - pos.entry_price) * pos.qty if pos.side == 'LONG' else (pos.entry_price - curr_price) * pos.qty
                        self._safe_audit_log(pos, f"\n[Super Order Sync] Verified CLOSED on Dhan via {reason}.")
                        return
                    else:
                        logger.warning(f"⚠️ Dhan position still open for {pos.symbol}. Manual exit required or wait for Super Order.")
                except Exception as e:
                    logger.error(f"Error syncing Super Order status: {e}")

            # Standard exit if not a super order or still open/unknown
            self._execute_exit(session, pos, reason)
            # Update PnL
            pos.pnl_pct = cur_pnl_pct
            pos.pnl_abs = (curr_price - pos.entry_price) * pos.qty if pos.side == 'LONG' else (pos.entry_price - curr_price) * pos.qty

if __name__ == "__main__":
    pm = PortfolioManager()
    pm.run_cycle()