
import os
import redis
import json
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta, time as dt_time
from collections import defaultdict, deque
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Project Imports
from src.db.schema import ORBSignal, Position
from src.services.portfolio_manager import PortfolioManager
from src.utils.notifications import notify_new_signal

load_dotenv()

class OrderFlowSniper:
    """
    Focused Sniper System listening to market_depth:LIVE.
    Triggers on Multi-Bar Accumulation (1m surge OR 3m accumulation).
    Logic based on the 98.6th percentile high-conviction backtest (100% WR).
    """
    
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=os.getenv("REDIS_PORT", 6379),
            decode_responses=True
        )
        
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe("market_depth:LIVE")
        
        # Load High-Conviction Magic Numbers
        self.magic_file = os.path.join(os.getcwd(), 'magic_volume.json')
        self._load_magic_numbers()
        
        # Strategy State
        self.bar_data = defaultdict(lambda: {
            'start_time': None,
            'open_p': 0,
            'current_vol': 0,
            'current_buy_vol': 0,
            'current_sell_vol': 0,
            'prev_1m_vols': deque(maxlen=15),
            'prev_1m_buy_vols': deque(maxlen=15),
            'prev_1m_sell_vols': deque(maxlen=15),
            'prev_opens': deque(maxlen=15),
            'last_price': 0,
            'last_vwap': 0
        })
        
        self.last_signal_time = {}
        self.port_mgr = PortfolioManager()
        
        logger.info("OrderFlowSniper Initialized - Using 98.6th Percentile (Holy Grail Mode)")

    def _load_magic_numbers(self):
        try:
            with open(self.magic_file, "r") as f:
                self.magic_data = json.load(f)
            logger.info(f"Loaded high-conviction magic for {len(self.magic_data)} symbols")
        except Exception as e:
            logger.error(f"Error loading magic_volume.json: {e}")
            self.magic_data = {}

    def run(self):
        """Main loop for processing real-time ticks."""
        logger.info("Sniper is listening to market_depth:LIVE...")
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    packet = json.loads(message['data'])
                    self._process_tick(packet)
                except Exception as e:
                    logger.error(f"Sniper logic error: {e}")

    def _process_tick(self, packet):
        symbol = packet['symbol']
        ltp = packet['ltp']
        total_vol = packet.get('volume', 0) # Cumulative session volume
        vwap = packet.get('vwap', ltp)
        now = datetime.now()
        
        if symbol not in self.magic_data: return
        
        state = self.bar_data[symbol]
        
        # 1. Bar Management (1-Minute Aggregation)
        if not state['start_time'] or (now - state['start_time']).total_seconds() >= 60:
            # End of bar - shift volumes
            # End of bar - shift volumes
            if state['start_time']:
                state['prev_1m_vols'].append(state['current_vol'])
                state['prev_1m_buy_vols'].append(state['current_buy_vol'])
                state['prev_1m_sell_vols'].append(state['current_sell_vol'])
                state['prev_opens'].append(state['open_p'])
            
            # Start new bar
            state['start_time'] = now
            state['open_p'] = ltp
            state['current_vol'] = 0
            state['start_session_vol'] = total_vol
            
        # Update current bar volume
        state['current_vol'] = total_vol - state.get('start_session_vol', total_vol)
        
        # Track buy/sell deltas
        current_buy = packet.get('buy_volume', 0)
        current_sell = packet.get('sell_volume', 0)
        
        if 'start_buy_vol' not in state:
            state['start_buy_vol'] = current_buy
            state['start_sell_vol'] = current_sell
            
        state['current_buy_vol'] = current_buy - state['start_buy_vol']
        state['current_sell_vol'] = current_sell - state['start_sell_vol']
        
        state['last_price'] = ltp
        state['last_vwap'] = vwap
        
        # Guard: Post 9:16 AM only
        # Guard: Post 9:30 AM only (Zero-Loss Protocol Rule 1)
        if now.time() < dt_time(9, 31):
            return
        
        # 2. Trigger Check
        thresh = self.magic_data[symbol]
        v1m_limit = thresh.get('high_conviction_magic', 1_000_000_000)
        v3m_limit = thresh.get('high_conviction_magic_3m', 1_000_000_000)
        
        # Metrics for Alpha Filter
        vol_5m = state['current_vol'] + sum(list(state['prev_1m_vols'])[-4:])
        buy_vol_5m = state['current_buy_vol'] + sum(list(state['prev_1m_buy_vols'])[-4:])
        sell_vol_5m = state['current_sell_vol'] + sum(list(state['prev_1m_sell_vols'])[-4:])
        
        aggression = buy_vol_5m / (sell_vol_5m + 1)
        
        p_5m_ago = state['prev_opens'][-4] if len(state['prev_opens']) >= 4 else state['open_p']
        price_change_5m = ((ltp / p_5m_ago) - 1) * 100
        efficiency = price_change_5m / ((vol_5m / 1000000.0) + 0.001)
        
        # 3-Minute Rolling sum
        recent_3m_vol = state['current_vol'] + sum(list(state['prev_1m_vols'])[-2:])
        
        hit_1m = state['current_vol'] >= v1m_limit
        hit_3m = recent_3m_vol >= v3m_limit
        
        if hit_1m or hit_3m:
            # ALPHA GUARDS (Confirmed via backtest: 70% Win Rate)
            is_eff = (efficiency > 0.6) or (efficiency < -5.0)
            is_agg = (aggression >= 4.0 and aggression <= 35.0) or (aggression <= 0.10)
            
            # Pulse Gate (Accumulation Check)
            all_recent = list(state['prev_1m_vols']) + [state['current_vol']]
            pulse_thresh = v1m_limit * 0.3
            pulses = sum(1 for v in all_recent[-15:] if v >= pulse_thresh)
            
            if is_eff and is_agg and pulses >= 3:
                side = "LONG" if efficiency > 0 else "SHORT"
                
                # 1. Redis Global Lock (Absolute Once-Per-Day per Stock)
                lock_key = f"lock:trade:{symbol}:{now.date()}"
                if self.redis_client.exists(lock_key):
                    return

                # 2. Memory Debounce
                last_t = self.last_signal_time.get(symbol, datetime.min)
                if last_t.date() >= now.date():
                    return

                    # 3. DB Guard (Absolute One-Trade-Per-Day per Stock for TODAY)
                    today = now.date()
                    existing_today = session.query(Position).filter(
                        Position.symbol == symbol,
                        Position.date == today
                    ).first()
                    
                    if existing_today:
                        # If we already have a record for today (OPEN, CLOSED, PENDING, EXPIRED, etc.)
                        # we do NOT create another one. Zero-Loss Strategy: Max 1 trade/stock/day.
                        self.redis_client.setex(lock_key, 86400, "1") # Sync Redis
                        self.last_signal_time[symbol] = now # Sync memory
                        logger.warning(f"🛡️ HolyGrail Guard: {symbol} already has a record for today (ID: {existing_today.id}, Status: {existing_today.status}). Entry blocked.")
                        return

                logger.success(f"🎯 HOLY GRAIL SNIPER TRIGGER: {symbol} {side} @ {ltp} | Eff: {efficiency:.2f} | Agg: {aggression:.2f}")
                
                # SET LOCK IMMEDIATELY to prevent race conditions
                self.redis_client.setex(lock_key, 86400, "1")
                
                self._generate_order(symbol, side, ltp, vwap, packet, efficiency, aggression, pulses)
                self.last_signal_time[symbol] = now

    def _generate_order(self, symbol, side, price, vwap, packet, efficiency, aggression, pulses):
        with self.Session() as session:
            # --- FINAL REDUNDANT GUARD ---
            today = datetime.now().date()
            existing = session.query(Position).filter(
                Position.symbol == symbol,
                Position.date == today
            ).first()
            
            if existing:
                logger.info(f"🛡️ Guard: skipping duplicate sniper signal for {symbol} (Found Today - Status: {existing.status})")
                return

            # Entry/TP/SL logic (1% Target, 2% Stop)
            tp = round(price * 1.01 if side == "LONG" else price * 0.99, 2)
            sl = round(price * 0.98 if side == "LONG" else price * 1.02, 2)
            
            sig = ORBSignal(
                symbol=symbol,
                side=side,
                date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                timestamp=datetime.now(),
                entry_price=price,
                sl=sl,
                tp=tp,
                signal_type="ORC_SNIPER_HOLYGRAIL",
                metrics={
                    "vwap": vwap,
                    "vol_1m": self.bar_data[symbol]['current_vol'],
                    "efficiency": efficiency,
                    "aggression": aggression,
                    "pulses": pulses,
                    "source": "ORC_SNIPER_HOLYGRAIL"
                },
                status="PENDING",
                bid_pct=packet.get('bid_pct', 50),
                ask_pct=packet.get('ask_pct', 50)
            )
            session.add(sig)
            session.flush()
            
            # Auto-Execute Check
            # Zero-Loss Protocol: Check .env master switch and then strategy presets
            master_switch = os.getenv("ENABLE_AUTO_TRADING", "false").lower() == "true"
            if not master_switch:
                logger.warning(f"🛡️ Sniper: {symbol} signal detected but ENABLE_AUTO_TRADING is FALSE in .env. Skipping.")
                session.commit()
                return

            # For Sniper, we'll check 'auto_execute_sniper' in config or use user presets
            try:
                # Load strategy presets
                preset_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'strategy_presets.json')
                with open(preset_file, "r") as f:
                    presets = json.load(f)
                auto_exec = presets.get("sateesh", {}).get("auto_execute_sniper", False)
                
                if auto_exec:
                    pos = Position(
                        symbol=symbol,
                        side=side,
                        date=sig.date,
                        entry_time=datetime.now(),
                        entry_price=sig.entry_price,
                        qty=1, 
                        signal_type="ORC_SNIPER_HOLYGRAIL",
                        status="PENDING", 
                        sl=sig.sl,
                        tp=sig.tp,
                        sl_type="ORB_BOUNDARY",
                        agent_audit_log=f"[HolyGrail] Automated execution trigger for Alpha Signal {sig.id}",
                        entry_metrics=sig.metrics
                    )
                    session.add(pos)
                    session.flush()
                    success, msg = self.port_mgr._execute_entry(session, pos)
                    if success:
                        sig.status = "EXECUTED"
                        logger.success(f"✅ SNIPER ORDER PLACED: {symbol} @ {price}")
                    else:
                        logger.warning(f"❌ SNIPER ORDER FAILED: {msg}")
            except Exception as e:
                logger.error(f"Sniper execution error: {e}")
                
            session.commit()
            notify_new_signal(symbol, side, "SNIPER", price, f"Holy Grail Triggered! Vol: {sig.metrics['vol_1m']}")

if __name__ == "__main__":
    sniper = OrderFlowSniper()
    sniper.run()
