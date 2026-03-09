
import pandas as pd
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func
import redis
import os
import time
import pytz
from datetime import datetime, timedelta, time as dt_time
from src.db.schema import IntradayTick, DailyFocus, Position, SystemJob, ORBSignal, ORBSignalNearMiss
from dotenv import load_dotenv
import json
from src.services.analysis_service import meets_filters, calculate_indicators
from src.utils.notifications import notify_new_signal
from src.services.portfolio_manager import PortfolioManager
from loguru import logger

load_dotenv()

def get_ist_now():
    ist_tz = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist_tz).replace(tzinfo=None)

# --- SERVICE CLASS ---
class SignalGenerator:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        
        # Redis setup for Market Depth
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True
        )
        self.pubsub = self.redis_client.pubsub()
        self.port_mgr = PortfolioManager()
        
        # State Cache
        self.focus_stocks = []
        self.strategy_config = {}
        self.last_cache_refresh = 0
        self.REDIS_CHANNEL = "market_depth:LIVE"
        
        # Performance Cache
        self.bars_cache = {} # {symbol: DataFrame}
        self.last_bar_pull = {} # {symbol: float}

    def _get_config(self, session):
        job = session.query(SystemJob).filter_by(job_id='signal_generator').first()
        return job.config if job and job.config else {}

    def _refresh_cache(self, force=False):
        """Refresh focus stocks and config every 60 seconds."""
        now = time.time()
        if not force and (now - self.last_cache_refresh) < 60:
            return

        session = self.Session()
        try:
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()
            # 1. Update Focus Stocks
            focus_objs = session.query(DailyFocus).filter(DailyFocus.date == today).all()
            if not focus_objs:
                # Fallback: Use the most recent focus date if today is empty (to handle clock drift)
                latest_date = session.query(func.max(DailyFocus.date)).scalar()
                if latest_date:
                    logger.warning(f"No focus stocks for {today}. Falling back to latest available: {latest_date}")
                    focus_objs = session.query(DailyFocus).filter(DailyFocus.date == latest_date).all()
                    today = latest_date.date() # Align internal 'today' for other queries

            self.focus_stocks = []
            for fo in focus_objs:
                self.focus_stocks.append({
                    "symbol": fo.symbol,
                    "orb_high": fo.orb_high,
                    "orb_low": fo.orb_low,
                    "orb_high_clean": fo.orb_high_clean,
                    "orb_low_clean": fo.orb_low_clean,
                    "orb_direction": fo.orb_direction,
                    "oracle_status": fo.oracle_status,
                    "avg_daily_turnover": fo.avg_daily_turnover,
                    "weekly_rsi": fo.weekly_rsi,
                    "weekly_sma": fo.weekly_sma,
                    "orb_range_pct": fo.orb_range_pct,
                    "orb_window": fo.orb_window
                })
            
            # 2. Update Strategy Config
            preset_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'strategy_presets.json')
            with open(preset_file, "r") as f:
                presets = json.load(f)
            p = presets.get("sateesh", {}).copy()
            db_config = self._get_config(session)
            if db_config:
                p.update(db_config)
            self.strategy_config = p
            
            # 3. Audit existing signals
            self._audit_existing_signals(session, p, today)
            
            # 4. Update Heartbeat
            job = session.query(SystemJob).filter_by(job_id='signal_generator').first()
            if job:
                job.last_run = datetime.now()
                job.status = "RUNNING"
            
            session.commit() # CRITICAL FIX: Commit the audit changes!
            
            self.last_cache_refresh = now
            logger.info(f"SignalGenerator Cache Refreshed: {len(self.focus_stocks)} stocks loaded.")
            logger.debug(f"Current Strategy Config: {json.dumps(p)}")
        except Exception as e:
            logger.error(f"Error refreshing SignalGenerator cache: {e}")
        finally:
            session.close()

    def _process_stock_event(self, symbol, live_packet):
        """Process a single stock event (tick) from Pub/Sub."""
        # 1. Find matching focus stock
        stock = next((s for s in self.focus_stocks if s['symbol'].split('.')[0].replace('NSE:', '').strip() == symbol), None)
        if not stock:
            # logger.debug(f"Symbol {symbol} not in focus list.")
            return
        
        logger.debug(f"Processing focus stock: {symbol} | Oracle: {stock['oracle_status']}")

        p = self.strategy_config
        ist_tz = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist_tz)
        curr_time_str = now_ist.strftime("%H:%M")

        # Time Window Check
        strat_start = p.get('start', '09:31')[:5]
        strat_end = p.get('end', '15:25')[:5]
        if not (strat_start <= curr_time_str <= strat_end):
            return

        session = self.Session()
        try:
            # 3. Get Intraday Data (Buffered from Redis)
            bar_key = f"bars:{symbol}"
            now = time.time()
            
            # Refresh bars from Redis every 30 seconds
            if symbol not in self.bars_cache or (now - self.last_bar_pull.get(symbol, 0)) > 30:
                redis_bars = self.redis_client.lrange(bar_key, 0, -1)
                data = []
                for b_json in redis_bars:
                    b = json.loads(b_json)
                    ts_ist = pd.to_datetime(b['timestamp'])
                    # Ensure it is naive IST
                    if ts_ist.tzinfo:
                        ts_ist = ts_ist.tz_convert('Asia/Kolkata').tz_localize(None)
                    
                    data.append({
                        'Datetime': ts_ist,
                        'Open': b['open'], 'High': b['high'], 'Low': b['low'], 
                        'Close': b['close'], 'Volume': b['volume'],
                        'imbalance': b.get('mean_imbalance', 0),
                        'iceberg_score': b.get('iceberg_score', 0)
                    })
                if data:
                    df_raw = pd.DataFrame(data).set_index('Datetime')
                    # Filter for today only
                    ist_tz = pytz.timezone('Asia/Kolkata')
                    today = datetime.now(ist_tz).date()
                    self.bars_cache[symbol] = df_raw[df_raw.index.date == today]
                    self.last_bar_pull[symbol] = now
            
            df_history = self.bars_cache.get(symbol, pd.DataFrame())
            
            # 4. Create "Live Row" for the current forming bar
            ltp = float(live_packet['ltp'])
            imbalance = live_packet.get('imbalance', 0.0)
            iceberg_score = live_packet.get('iceberg_score', 0.0)
            ts_now = now_ist.replace(tzinfo=None)
            
            live_row = pd.DataFrame([{
                'Open': ltp, 'High': ltp, 'Low': ltp, 'Close': ltp, 
                'Volume': float(live_packet.get('current_bar_volume', 1.0)),
                'imbalance': imbalance, 'iceberg_score': iceberg_score
            }], index=[ts_now])
            live_row.index.name = 'Datetime'
            
            # Combine history with live tick
            if df_history.empty:
                df_all = live_row
            else:
                # If the latest bar in history is the same minute as now, just update it
                # Otherwise, append the live row
                if df_history.index[-1].minute == ts_now.minute and df_history.index[-1].hour == ts_now.hour:
                    # Update rather than append if within same minute
                    df_all = df_history.copy()
                    df_all.iloc[-1, df_all.columns.get_loc('Close')] = ltp
                    df_all.iloc[-1, df_all.columns.get_loc('High')] = max(df_all.iloc[-1]['High'], ltp)
                    df_all.iloc[-1, df_all.columns.get_loc('Low')] = min(df_all.iloc[-1]['Low'], ltp)
                    # Don't update volume here, as the history bar already contains the minute's volume
                else:
                    df_all = pd.concat([df_history, live_row])
            
            # Boundaries
            orb_h = stock['orb_high_clean'] if stock['orb_high_clean'] else stock['orb_high']
            orb_l = stock['orb_low_clean'] if stock['orb_low_clean'] else stock['orb_low']
            range_dir = stock['orb_direction'] or "NEUTRAL"
            if not orb_h or not orb_l: return

            # Determine Indicators on the full augmented history
            oracle_status = stock['oracle_status'] or "NEUTRAL"
            df_processed = calculate_indicators(df_all.copy(), side='LONG' if oracle_status == 'UP_SNIPER' else 'SHORT')
            
            # The last row now has the correct indicators (including the live tick effect)
            last_row = df_processed.iloc[-1]

            htf_metrics = {
                'adtv_cr': stock['avg_daily_turnover'],
                'weekly_rsi': stock['weekly_rsi'],
                'weekly_sma': stock['weekly_sma']
            }

            ts = now_ist.replace(tzinfo=None)
            
            # 1. Check for LONG Breakout
            if ltp > orb_h:
                is_sniper_long = oracle_status in p.get('long_snipers', ['UP_SNIPER'])
                is_valid, reasons, _ = meets_filters(last_row, 'LONG', p, stock.get('orb_high', 0), stock.get('orb_low', 0), range_dir, orb_h_cln=stock.get('orb_high_clean'), orb_l_cln=stock.get('orb_low_clean'), htf_metrics=htf_metrics, imbalance=imbalance, is_live=True, bid_pct=live_packet.get('bid_pct'), ask_pct=live_packet.get('ask_pct'))
                
                if is_sniper_long and is_valid:
                    self._create_signal(session, stock, last_row, ts, 'LONG', 'BREAKOUT', p, imbalance=imbalance, iceberg=iceberg_score, live_packet=live_packet)
                else:
                    self._create_near_miss(session, stock, last_row, ts, 'LONG', 'BREAKOUT', reasons, p, imbalance=imbalance, iceberg=iceberg_score, live_packet=live_packet)

            # 2. Check for SHORT Breakdown
            if ltp < orb_l:
                is_sniper_short = oracle_status in p.get('short_snipers', ['DOWN_SNIPER'])
                is_valid, reasons, _ = meets_filters(last_row, 'SHORT', p, stock.get('orb_high', 0), stock.get('orb_low', 0), range_dir, orb_h_cln=stock.get('orb_high_clean'), orb_l_cln=stock.get('orb_low_clean'), htf_metrics=htf_metrics, imbalance=imbalance, is_live=True, bid_pct=live_packet.get('bid_pct'), ask_pct=live_packet.get('ask_pct'))
                
                if is_sniper_short and is_valid:
                    self._create_signal(session, stock, last_row, ts, 'SHORT', 'BREAKDOWN', p, imbalance=imbalance, iceberg=iceberg_score, live_packet=live_packet)
                else:
                    self._create_near_miss(session, stock, last_row, ts, 'SHORT', 'BREAKDOWN', reasons, p, imbalance=imbalance, iceberg=iceberg_score, live_packet=live_packet)

            session.commit()
        except Exception as e:
            logger.error(f"Error processing stock event for {symbol}: {e}")
            session.rollback()
        finally:
            session.close()

    def start(self):
        """Main entry point: Listens to Redis Pub/Sub for events."""
        logger.info(f"SignalGenerator: Event-Driven Mode Starting. Subscribing to {self.REDIS_CHANNEL}...")
        self.pubsub.subscribe(self.REDIS_CHANNEL)
        
        # Initial Cache Fill
        self._refresh_cache(force=True)

        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    payload = json.loads(message['data'])
                    symbol = payload.get('symbol')
                    
                    # Periodic Tasks (Heartbeat) - refreshes every 60s
                    self._refresh_cache()
                    
                    if symbol:
                        # logger.debug(f"Main Loop: Received {symbol}")
                        self._process_stock_event(symbol, payload)
                        
                except Exception as e:
                    logger.error(f"SignalGenerator Main Loop Error: {e}")

    def _create_near_miss(self, session, stock, row, ts, side, signal_type, reasons, p, imbalance=None, iceberg=0.0, live_packet=None):
        """Logs a signal that failed filters."""
        
        # Check if a REAL signal already exists for this symbol today
        real_exists = session.query(ORBSignal).filter(
            ORBSignal.symbol == stock['symbol'],
            func.date(ORBSignal.date) == ts.date()
        ).first()
        if real_exists: return

        # Deduplicate Near Miss
        exists = session.query(ORBSignalNearMiss).filter(
            ORBSignalNearMiss.symbol == stock['symbol'],
            ORBSignalNearMiss.side == side,
            ORBSignalNearMiss.timestamp == ts
        ).first()
        if exists: return

        # Only log one near miss per side if no breakout has happened yet
        # or if the latest signal is already handled.
        latest_any = session.query(ORBSignalNearMiss).filter(
            ORBSignalNearMiss.symbol == stock['symbol'],
            ORBSignalNearMiss.side == side,
            func.date(ORBSignalNearMiss.date) == ts.date()
        ).order_by(ORBSignalNearMiss.timestamp.desc()).first()
        
        if latest_any and (ts - latest_any.timestamp).total_seconds() < 300:
            return # Cooldown for logging near misses

        # SL Calculation
        entry_price = round(float(row['Close']), 2)
        if p.get('sl_type') == 'ORB_BOUNDARY':
            sl_price = stock['orb_low'] if side == 'LONG' else stock['orb_high']
        else: # FIXED_PCT
            sl_pct = p.get('sl_pct', 0.5)
            sl_price = entry_price * (1 - sl_pct/100) if side == 'LONG' else entry_price * (1 + sl_pct/100)
        
        # Target Calculation
        tp_pct = p.get('tp_pct', 1.0)
        tp_price = entry_price * (1 + tp_pct/100) if side == 'LONG' else entry_price * (1 - tp_pct/100)

        print(f"--- NEAR MISS LOGGED: {stock['symbol']} {side} (Filter Fail: {reasons}) ---")
        
        nm = ORBSignalNearMiss(
            symbol=stock['symbol'],
            side=side,
            date=ts.date(),
            timestamp=ts,
            entry_price=entry_price,
            signal_type=signal_type,
            sl=round(float(sl_price), 2),
            tp=round(float(tp_price), 2),
            fail_reasons=reasons,
            metrics={
                "rsi": round(float(row['RSI']), 2),
                "macd": round(float(row['MACD']), 2),
                "vol_surge": round(float(row['Vol_Surge']), 2),
                "vqs": round(float(row.get('VQS', 0)), 2),
                "slope": round(float(row.get('Slope', 0)), 2),
                "bid_pct": live_packet.get('bid_pct', 50.0) if live_packet else 50.0,
                "ask_pct": live_packet.get('ask_pct', 50.0) if live_packet else 50.0,
                "orb_h": round(float(stock.get('orb_high', 0)), 2),
                "orb_l": round(float(stock.get('orb_low', 0)), 2),
                "range_pct": round(float(stock.get('orb_range_pct', 0) or 0), 2),
                "range_pct_clean": round(float(((stock.get('orb_high_clean', 0) or 0 - (stock.get('orb_low_clean', 0) or 0))/(stock.get('orb_low_clean', 1) or 1))*100), 2) if stock.get('orb_low_clean') else 0,
                "oracle_status": stock.get('oracle_status', 'NEUTRAL'),
                "weekly_rsi": stock.get('weekly_rsi'),
                "weekly_sma": stock.get('weekly_sma'),
                "adtv_cr": round(float(stock.get('avg_daily_turnover', 0) or 0), 2),
                "direction": stock.get('orb_direction', 'NEUTRAL'),
                "imbalance": round(float(imbalance), 2) if imbalance is not None else 0.0,
                "iceberg_score": round(float(iceberg), 2)
            }
        )
        session.add(nm)


    def _audit_existing_signals(self, session, p, today):
        """Checks if PENDING signals hit TP/SL using intraday data."""
        pending_sigs = session.query(ORBSignal).filter(
            ORBSignal.status.in_(["PENDING", "EXECUTED"]),
            ORBSignal.date >= today
        ).all()

        if not pending_sigs: return
        print(f"SignalGenerator: Auditing {len(pending_sigs)} pending signals...")

        for sig in pending_sigs:
            # 1. Faster Check: If this signal was executed, check the positions table.
            if sig.status == "EXECUTED" and sig.execution_pos_id:
                pos = session.query(Position).filter(Position.id == sig.execution_pos_id).first()
                if pos and pos.status == "CLOSED":
                    sig.status = "TARGET_HIT" if pos.exit_reason == "TP Hit" else ("SL_HIT" if pos.exit_reason == "SL Hit" else "FINISHED")
                    print(f"AUDIT: Signal {sig.id} for {sig.symbol} cleared via Position closure.")
                    continue

            # 2. Backup Check: Get ticks AFTER signal timestamp
            ticks = session.query(IntradayTick).filter(
                IntradayTick.symbol == sig.symbol,
                IntradayTick.timestamp >= sig.timestamp
            ).order_by(IntradayTick.timestamp.asc()).all()

            for t in ticks:
                hit_tp = False
                hit_sl = False

                if sig.side == 'LONG':
                    if t.high >= sig.tp: hit_tp = True
                    elif t.low <= sig.sl: hit_sl = True
                else: # SHORT
                    if t.low <= sig.tp: hit_tp = True
                    elif t.high >= sig.sl: hit_sl = True
                
                if hit_tp:
                    print(f"AUDIT: {sig.symbol} {sig.side} TARGET HIT @ {t.timestamp}")
                    sig.status = "TARGET_HIT"
                    break
                if hit_sl:
                    print(f"AUDIT: {sig.symbol} {sig.side} SL HIT @ {t.timestamp}")
                    sig.status = "SL_HIT"
                    break

    def _create_signal(self, session, stock, row, ts, side, signal_type, p, imbalance=None, iceberg=0.0, live_packet=None):
        from sqlalchemy import func
        # Check for active signal or position
        active = session.query(ORBSignal).filter(
            ORBSignal.symbol == stock['symbol'],
            ORBSignal.status.in_(['PENDING', 'EXECUTED']),
            func.date(ORBSignal.date) == ts.date()
        ).first()
        
        if active:
            # If we are in "Sliding" mode, we'd update. 
            # But the requirement is to STOP sliding and allow DISCRETE entries.
            # So if PENDING exists, we just wait for it to be hit or cancelled.
            return 

        print(f"!!! DISCRETE SIGNAL DETECTED: {stock['symbol']} {side} (Staging to ORB_SIGNALS) !!!")
        entry_price = round(float(row['Close']), 2)
        if p.get('sl_type') == 'ORB_BOUNDARY':
            sl_price = stock['orb_low'] if side == 'LONG' else stock['orb_high']
        else: # FIXED_PCT
            sl_pct = p.get('sl_pct', 0.5)
            sl_price = entry_price * (1 - sl_pct/100) if side == 'LONG' else entry_price * (1 + sl_pct/100)
        
        tp_pct = p.get('tp_pct', 1.0)
        tp_price = entry_price * (1 + tp_pct/100) if side == 'LONG' else entry_price * (1 - tp_pct/100)

        sig = ORBSignal(
            symbol=stock['symbol'],
            side=side,
            date=ts.date(),
            timestamp=ts,
            entry_price=entry_price,
            signal_type=signal_type,
            status="PENDING",
            sl=round(float(sl_price), 2),
            tp=round(float(tp_price), 2),
            metrics={
                "rsi": round(float(row['RSI']), 2),
                "macd": round(float(row['MACD']), 2),
                "vol_surge": round(float(row['Vol_Surge']), 2),
                "vqs": round(float(row.get('VQS', 0)), 2),
                "slope": round(float(row.get('Slope', 0)), 2),
                "bid_pct": live_packet.get('bid_pct', 50.0),
                "ask_pct": live_packet.get('ask_pct', 50.0),
                "orb_h": round(float(stock.get('orb_high', 0)), 2),
                "orb_l": round(float(stock.get('orb_low', 0)), 2),
                "range_pct": round(float(stock.get('orb_range_pct', 0) or 0), 2),
                "range_pct_clean": round(float(((stock.get('orb_high_clean', 0) or 0 - (stock.get('orb_low_clean', 0) or 0))/(stock.get('orb_low_clean', 1) or 1))*100), 2) if stock.get('orb_low_clean') else 0,
                "oracle_status": stock.get('oracle_status', 'NEUTRAL'),
                "weekly_rsi": stock.get('weekly_rsi'),
                "weekly_sma": stock.get('weekly_sma'),
                "adtv_cr": round(float(stock.get('avg_daily_turnover', 0) or 0), 2),
                "direction": stock.get('orb_direction', 'NEUTRAL'),
                "imbalance": round(float(imbalance), 2) if imbalance is not None else 0.0,
                "iceberg_score": round(float(iceberg), 2)
            }
        )
        try:
            session.add(sig)
            session.flush() # Get ID
            logger.info(f"DEBUG: Successfully flushed ORBSignal for {stock['symbol']} (ID: {sig.id})")
        except Exception as e:
            logger.error(f"DATABASE ERROR: Failed to insert ORBSignal for {stock['symbol']}: {e}")
            raise

        # --- AUTO-EXECUTE LOGIC ---
        auto_exec = False
        if side == 'LONG' and p.get('auto_execute_long', False): 
            auto_exec = True
        elif side == 'SHORT' and p.get('auto_execute_short', False):
            auto_exec = True
            
        if auto_exec:
            print(f"SignalGenerator: AUTO-EXECUTE ENABLED for {stock['symbol']}. Firing order...")
            # 1. Create Position Record in PENDING status
            pos = Position(
                symbol=sig.symbol,
                side=sig.side,
                date=sig.date,
                entry_time=datetime.now(),
                entry_price=sig.entry_price,
                qty=1, 
                signal_type=sig.signal_type,
                status="PENDING", 
                sl=sig.sl,
                tp=sig.tp,
                sl_type="ORB_BOUNDARY",
                entry_metrics=sig.metrics,
                agent_audit_log=f"Automated execution trigger for ORB Signal {sig.id}"
            )
            try:
                session.add(pos)
                session.flush()
                logger.debug(f"DEBUG: Successfully flushed Position for {stock['symbol']} (ID: {pos.id})")
            except Exception as e:
                logger.error(f"DATABASE ERROR: Failed to insert Position for {stock['symbol']}: {e}")
                raise
            
            # 2. Trigger PortfolioManager Execution Logic
            try:
                # Fetch remote positions for the double-entry guard
                remote_positions = self.port_mgr.dhan_client.get_positions()
                success, message = self.port_mgr._execute_entry(session, pos, remote_positions)
                if success and pos.status == 'OPEN':
                    sig.status = "EXECUTED"
                    sig.execution_pos_id = pos.id
                    sig.target_order_id = pos.target_order_id
                    sig.sl_order_id = pos.sl_order_id
                    print(f"SignalGenerator: Successfully AUTO-EXECUTED {sig.symbol}")
                else:
                    print(f"SignalGenerator: Auto-Execute failed/skipped for {sig.symbol}: {message}")
            except Exception as e:
                print(f"SignalGenerator: Auto-Execute Error for {stock['symbol']}: {e}")

        try:
            notify_new_signal(sig.symbol, sig.side, sig.signal_type, sig.entry_price)
            logger.debug(f"DEBUG: Notification triggered for {stock['symbol']}")
        except Exception as e:
            logger.warning(f"NOTIFICATION ERROR: Failed to notify for {stock['symbol']}: {e}")

        try:
            session.commit()
            logger.info(f"DEBUG: Final commit success for signal {sig.symbol}")
        except Exception as e:
            logger.error(f"DATABASE ERROR: Final commit failure for {sig.symbol}: {e}")
            session.rollback()
            raise

if __name__ == "__main__":
    svc = SignalGenerator()
    svc.start()
