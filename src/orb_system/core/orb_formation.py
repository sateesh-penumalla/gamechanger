import pandas as pd
from datetime import datetime, time, timedelta
from typing import List, Dict, Optional
from loguru import logger
from src.orb_system.config.database import db
from src.orb_system.config.settings import settings
from src.orb_system.models.data_models import ORBSetup
from src.orb_system.services.market_data import MarketDataService
from src.agents.sector_general import SectorGeneralAgent
from src.agents.newsroom import NewsroomAgent
from src.data.yfinance_client import YahooFinanceData

class ORBFormationAnalyzer:
    """Analyzes price action during the ORB window and scores setup quality"""
    
    def __init__(self, market_data_service: MarketDataService):
        self.mds = market_data_service
        self.sector_agent = SectorGeneralAgent(data_client=YahooFinanceData())
        self.news_agent = NewsroomAgent()
        
    def analyze_orb(self, 
                    symbol: str, 
                    trade_date: datetime.date, 
                    orb_window_mins: int = 30, 
                    orb_style: str = "STANDARD",
                    range_filter: Optional[List[float]] = None,
                    vol_quality_min: Optional[float] = None,
                    min_quality: Optional[int] = None) -> Optional[ORBSetup]:
        """Calculate ORB boundaries and score the setup for a symbol"""
        logger.info(f"Analyzing {orb_style} ORB ({orb_window_mins}m) for {symbol} on {trade_date}...")
        
        try:
             # Define ORB window
             orb_start = datetime.combine(trade_date, settings.MARKET_OPEN)
             orb_end = orb_start + timedelta(minutes=orb_window_mins)
             
             # Fetch 1-min data for the ORB window
             df = self.mds.get_historical_data(symbol, orb_start, orb_end, interval="1")
             
             if df.empty or len(df) < (orb_window_mins * 0.5): # Use 50% data threshold
                 logger.warning(f"Insufficient data for ORB analysis: {symbol} (Found {len(df)} rows)")
                 return None
                  
             # 1. Calculate Standard Boundaries
             std_high = df['high'].max()
             std_low = df['low'].min()
             
             # 2. Calculate Clean Boundaries (Skip first 5 mins 9:15-9:20)
             clean_cutoff = orb_start + timedelta(minutes=5)
             clean_df = df[df['timestamp'] >= clean_cutoff]
             if not clean_df.empty:
                 clean_high = clean_df['high'].max()
                 clean_low = clean_df['low'].min()
             else:
                 clean_high, clean_low = std_high, std_low

             # Select boundaries based on style
             orb_high = clean_high if orb_style == "CLEAN" else std_high
             orb_low = clean_low if orb_style == "CLEAN" else std_low
             
             orb_range_pct = ((orb_high - orb_low) / orb_low) * 100
             orb_avg_volume = df['volume'].mean()
             
             # 3. Integrate Agents
             sector_data = self.sector_agent.validate_trend(symbol, trade_date=trade_date)
             news_data = self.news_agent.analyze_sentiment(symbol)
             
              # 4. Score Components
             # A. Range Score (0-30): Prefer ranges between 0.5% and 2.0% (or dynamic range if preset)
             range_score = 0
             if range_filter:
                 if range_filter[0] <= orb_range_pct <= range_filter[1]: range_score = 30
                 elif (range_filter[0]*0.5) <= orb_range_pct <= (range_filter[1]*1.2): range_score = 15
             else:
                 if 0.5 <= orb_range_pct <= 1.5: range_score = 30
                 elif 0.2 <= orb_range_pct < 0.5 or 1.5 < orb_range_pct <= 2.5: range_score = 15
             
             # B. Volume Score (0-25): Relative to past 20 days
             avg_historical_vol = self.mds.calculate_average_volume(symbol, reference_date=trade_date)
             # Adjust avg historical vol to the ORB window size
             volume_ratio = (orb_avg_volume / (avg_historical_vol / (7 * 60))) if avg_historical_vol > 0 else 1.0
             volume_score = 25 if volume_ratio > 1.5 else (15 if volume_ratio > 1.0 else 5)
             
             # C. Price Action Score (0-25)
             first_close = df['close'].iloc[0]
             last_close = df['close'].iloc[-1]
             pa_trend = ((last_close - first_close) / first_close) * 100
             price_action_score = 25 if abs(pa_trend) > 0.3 else 10
             
             # D. Sector Alignment & Market Regime
             # Market Regime defaults to 50 if neutral, sector gives additional alignment
             sector_score = 20 if sector_data.get('status') != 'NEUTRAL' else 10
             market_regime = 50 # Default to Neutral if we can't verify historical index
             if sector_data.get('status') == 'BULLISH': market_regime = 75
             elif sector_data.get('status') == 'BEARISH': market_regime = 25
             
             # Total Quality Score
             quality_score = range_score + volume_score + price_action_score + sector_score
             
             # Final Tradeability Check with Preset/Param Overrides
             min_q = min_quality or settings.MIN_ORB_QUALITY_SCORE
             is_tradeable = quality_score >= min_q
             
             if vol_quality_min and volume_ratio < vol_quality_min:
                 is_tradeable = False
                 logger.info(f"Setup rejected for {symbol}: Volume Quality {volume_ratio:.2f} < {vol_quality_min}")
             
             # 5. Final Setup Object
             setup = ORBSetup(
                 symbol=symbol,
                 date=trade_date,
                 orb_high=orb_high,
                 orb_low=orb_low,
                 orb_range_pct=round(orb_range_pct, 2),
                 orb_formation_volume=orb_avg_volume,
                 
                 clean_orb_high=clean_high,
                 clean_orb_low=clean_low,
                 orb_window_mins=orb_window_mins,
                 orb_style=orb_style,
                 
                 orb_quality_score=quality_score,
                 range_score=range_score,
                 volume_score=volume_score,
                 price_action_score=price_action_score,
                 sector_alignment_score=sector_score,
                 market_regime_score=market_regime, # Properly assigned
                 is_tradeable=is_tradeable,
                 news_sentiment_score=news_data.get('score', 50),
                 tradeable_reason=f"Quality: {quality_score} | Range: {orb_range_pct:.2f}% | Style: {orb_style}"
             )
             
             # Persist to DB
             self._log_setup(setup)
             return setup
             
        except Exception as e:
            logger.error(f"Error analyzing ORB for {symbol}: {e}")
            return None

    def _log_setup(self, setup: ORBSetup):
        """Log ORB setup to database"""
        try:
            with db.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO orb_setup 
                    (date, symbol, orb_high, orb_low, orb_range_pct, orb_formation_volume,
                     orb_quality_score, range_score, volume_score, price_action_score,
                     volatility_score, sector_alignment_score, is_tradeable, tradeable_reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    orb_high=VALUES(orb_high), orb_low=VALUES(orb_low), 
                    orb_quality_score=VALUES(orb_quality_score), is_tradeable=VALUES(is_tradeable)
                """, (
                    setup.date, setup.symbol, setup.orb_high, setup.orb_low,
                    setup.orb_range_pct, setup.orb_formation_volume, setup.orb_quality_score,
                    setup.range_score, setup.volume_score, setup.price_action_score,
                    setup.volatility_score, setup.sector_alignment_score,
                    setup.is_tradeable, setup.tradeable_reason
                ))
        except Exception as e:
            logger.error(f"Error logging ORB setup for {setup.symbol}: {e}")
