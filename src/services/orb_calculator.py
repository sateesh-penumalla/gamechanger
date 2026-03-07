
import pandas as pd
import pytz
from datetime import datetime, time
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
import os
import json
from dotenv import load_dotenv
from loguru import logger

from src.db.schema import Ticker, DailyFocus, SystemJob
from src.data.dhan_client import DhanDataClient

load_dotenv()

class ORBCalculator:
    def __init__(self, data_client=None):
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.data_client = data_client or DhanDataClient()

    def run_cycle(self):
        """
        Job scheduled for ~9:31 AM IST.
        1. Identifies SNIPER candidates from Tickers table.
        2. Fetches 9:15 - 9:30 data.
        3. Calculates ORB (Standard & Clean).
        4. Populates DailyFocus for today.
        """
        session = self.Session()
        try:
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)
            today = now_ist.date()
            
            # 1. Identify SNIPER candidates (Oracle marked them at 8 AM)
            snipers = session.query(Ticker).filter(Ticker.oracle_status.in_(['UP_SNIPER', 'DOWN_SNIPER'])).all()
            if not snipers:
                logger.warning("ORBCalculator: No SNIPER candidates found in Tickers table.")
                return
            
            # Priority Sort: Process high-score and high-liquidity stocks first
            snipers.sort(key=lambda x: (x.tech_score or 0, x.avg_daily_turnover or 0), reverse=True)
            
            symbols = [s.symbol for s in snipers]
            logger.info(f"ORBCalculator: calculating ORB for {len(symbols)} candidates (Prioritized)...")

            # 2. Fetch 9:15 - 9:30 data from internal DB
            from src.db.schema import IntradayTick
            
            # 3. Load Strategy Preset (for range_pct filtering)
            with open("src/config/strategy_presets.json", "r") as f:
                presets = json.load(f)
            p = presets.get("sateesh", {})
            range_min, range_max = p.get("range_pct", [0.5, 3.0])
            mode = p.get("boundary", "STANDARD")
            
            # Process each symbol
            count = 0
            for sniper in snipers:
                sym = sniper.symbol
                
                # Fetch all 1m ticks for the ORB window from DB
                # Filter for today and between 09:15 and 09:30
                ticks = session.query(IntradayTick).filter(
                    IntradayTick.symbol == sym,
                    IntradayTick.timestamp >= today.strftime("%Y-%m-%d") + " 09:15:00",
                    IntradayTick.timestamp <= today.strftime("%Y-%m-%d") + " 09:30:00"
                ).order_by(IntradayTick.timestamp.asc()).all()

                if not ticks:
                    logger.warning(f"ORBCalculator: No internal tick data found for {sym} in ORB window.")
                    continue
                
                # Convert to simple list of dicts for calculation
                # Standard ORB
                h_std = max(t.high for t in ticks)
                l_std = min(t.low for t in ticks)
                
                # CLEAN ORB (Body only)
                h_cln = max(max(t.open, t.close) for t in ticks)
                l_cln = min(min(t.open, t.close) for t in ticks)
                
                # 5. Upsert / Update DailyFocus
                focus = session.query(DailyFocus).filter_by(symbol=sym, date=today).first()
                if not focus:
                    focus = DailyFocus(symbol=sym, date=today)
                    session.add(focus)

                # Range Pct Filtering
                if mode == 'CLEAN':
                    r_pct = ((h_cln - l_cln) / l_cln) * 100
                else:
                    r_pct = ((h_std - l_std) / l_std) * 100
                    
                if r_pct < range_min or r_pct > range_max:
                    logger.info(f"ORBCalculator: Rejecting {sym} - Range {r_pct:.2f}% (Target: {range_min}-{range_max}%)")
                    focus.oracle_status = "REJECTED_RANGE"
                    focus.orb_range_pct = float(r_pct)
                    session.commit()
                    count += 1
                    continue

                # DIRECTION (First tick Open vs Last tick Close)
                o_price = float(ticks[0].open)
                c_price = float(ticks[-1].close)
                dir_label = "NEUTRAL"
                if (c_price - o_price) / o_price > 0.0005: dir_label = "BULLISH"
                elif (c_price - o_price) / o_price < -0.0005: dir_label = "BEARISH"
                
                # ADTV Guard (Institutional Floor - 0.5 Cr by default if not set in preset)
                adtv_cr = sniper.avg_daily_turnover or 0
                if adtv_cr < 0.5:
                    logger.info(f"ORBCalculator: Rejecting {sym} - Liquidity {adtv_cr} Cr")
                    focus.oracle_status = "REJECTED_LIQUIDITY"
                    session.commit()
                    count += 1
                    continue

                # 5. Upsert into DailyFocus
                focus.sector = sniper.sector or "Unknown"
                focus.oracle_status = sniper.oracle_status or "SNIPER"
                focus.orb_high = h_std
                focus.orb_low = l_std
                focus.orb_high_clean = h_cln
                focus.orb_low_clean = l_cln
                focus.orb_direction = dir_label
                focus.orb_range_pct = float(r_pct)
                focus.orb_window = 15
                focus.scout_score = sniper.scout_score or 0
                focus.tech_score = sniper.tech_score or 0
                focus.mood_score = sniper.mood_score or 0
                focus.avg_daily_turnover = float(adtv_cr)
                
                count += 1
                session.commit()
            logger.info(f"ORBCalculator: Successfully populated focus list for {count} symbols from INTERNAL DB.")

        except Exception as e:
            logger.error(f"ORBCalculator Error: {e}")
            session.rollback()
        finally:
            session.close()

if __name__ == "__main__":
    calc = ORBCalculator()
    calc.run_cycle()
