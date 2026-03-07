from datetime import datetime, date
from typing import List, Dict, Optional
from loguru import logger
from src.orb_system.config.database import db
from src.orb_system.config.settings import settings
from src.orb_system.models.data_models import MarketRegime
from src.agents.oracle import OracleAgent
from src.agents.librarian import LibrarianAgent
from src.agents.globalist import GlobalistAgent
from src.data.yfinance_client import YahooFinanceData
from sqlalchemy import create_engine
import os

class PreMarketAnalyzer:
    """Handles pre-market analysis, watchlist generation, and agent synchronization"""
    
    def __init__(self):
        # Initialize existing agents
        # Note: OracleAgent expects a db session (SQLAlchemy)
        db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(db_url)
        # For OracleAgent, we might need to wrap its sync logic or use it directly
        self.oracle = OracleAgent(data_client=YahooFinanceData(), db_session=None)
        self.librarian = LibrarianAgent(db_session=None) # We'll handle its DB needs separately or inject
        self.globalist = GlobalistAgent(data_client=YahooFinanceData())
        
    def sync_oracle_if_needed(self):
        """Checks if Oracle agent has run for the day and triggers it if not"""
        logger.info("Checking Oracle synchronization status...")
        today = date.today()
        
        try:
            with db.get_cursor() as cursor:
                # Check if any ticker was updated today in the main tickers table
                cursor.execute("SELECT COUNT(*) as count FROM bharatquant_sniper.tickers WHERE DATE(last_updated) = %s", (today,))
                result = cursor.fetchone()
                
                if result['count'] == 0:
                    logger.info("Oracle has not run today. Triggering Oracle sync...")
                    # Get all active symbols to sync
                    cursor.execute("SELECT symbol FROM bharatquant_sniper.tickers WHERE status = 'ACTIVE'")
                    symbols = [row['symbol'] for row in cursor.fetchall()]
                    
                    if symbols:
                        # Direct call to sync_ticker_db
                        self.oracle.sync_ticker_db(symbols)
                        logger.info("Oracle sync completed successfully.")
                else:
                    logger.info("Oracle data is already up-to-date for today.")
                    
        except Exception as e:
            logger.error(f"Error in Oracle synchronization: {e}")

    def analyze_market_regime(self) -> MarketRegime:
        """Analyze Nifty/BankNifty mood and overall regime"""
        logger.info("Analyzing market regime...")
        mood_data = self.globalist.analyze_market_mood()
        
        # Calculate India VIX and AD Ratio (Placeholder logic or fetch from Dhan)
        # For now, we'll use the Globalist output to form the regime
        
        regime_score = mood_data.get('conviction', 50)
        
        # Adjust regime score based on details
        # e.g., if Nifty and BankNifty are both trending hard, boost score
        
        regime = MarketRegime(
            date=datetime.now(),
            nifty_trend=mood_data.get('mood', 'NEUTRAL').upper(),
            nifty_ema20=0.0, # Placeholder
            nifty_ema50=0.0, # Placeholder
            india_vix=0.0,   # Placeholder
            advance_decline_ratio=mood_data.get('avg_pct', 0.0),
            regime_score=regime_score,
            trade_orb=regime_score >= settings.MIN_MARKET_REGIME_SCORE,
            reason=f"Mood: {mood_data.get('mood')}. Details: {', '.join(mood_data.get('details', []))}"
        )
        
        # Log to DB
        self._log_regime(regime)
        return regime

    def generate_watchlist(self) -> List[str]:
        """Generate watchlist from tickers table filtering for SNIPER status and Librarian Veto"""
        logger.info("Generating watchlist for ORB system...")
        watchlist = []
        
        try:
            # First, ensure Oracle is synced
            self.sync_oracle_if_needed()
            
            with db.get_cursor() as cursor:
                # Query SNIPER stocks
                cursor.execute("""
                    SELECT symbol, oracle_status, scout_score, tech_score 
                    FROM bharatquant_sniper.tickers 
                    WHERE oracle_status IN ('UP_SNIPER', 'DOWN_SNIPER')
                    AND status = 'ACTIVE'
                """)
                candidates = cursor.fetchall()
                
                for cand in candidates:
                    symbol = cand['symbol']
                    
                    # Librarian Veto: Check if this stock has failed recently
                    # We'll use a local implementation of the check for now
                    if self._check_librarian_veto(symbol):
                        logger.warning(f"Librarian VETO for {symbol}: Recent failures detected.")
                        continue
                    
                    watchlist.append(symbol)
                    
            logger.info(f"Generated watchlist with {len(watchlist)} SNIPER symbols.")
            return watchlist
            
        except Exception as e:
            logger.error(f"Error generating watchlist: {e}")
            return []

    def _check_librarian_veto(self, symbol: str) -> bool:
        """Internal implementation of Librarian veto logic querying the performance log"""
        try:
            with db.get_cursor() as cursor:
                # Count failures for this symbol in the last 48 hours
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM orb_trades 
                    WHERE symbol = %s 
                    AND status = 'CLOSED' 
                    AND pnl_pct < 0 
                    AND date >= DATE_SUB(CURDATE(), INTERVAL 2 DAY)
                """, (symbol,))
                result = cursor.fetchone()
                return result['count'] >= 2
        except Exception as e:
            logger.error(f"Error checking Librarian veto: {e}")
            return False

    def _log_regime(self, regime: MarketRegime):
        """Persist market regime analysis to database"""
        try:
            with db.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO market_regime 
                    (date, nifty_trend, nifty_ema20, nifty_ema50, india_vix, 
                     advance_decline_ratio, regime_score, trade_orb)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    nifty_trend=VALUES(nifty_trend), regime_score=VALUES(regime_score),
                    trade_orb=VALUES(trade_orb)
                """, (
                    regime.date.date(), regime.nifty_trend, regime.nifty_ema20,
                    regime.nifty_ema50, regime.india_vix, regime.advance_decline_ratio,
                    regime.regime_score, regime.trade_orb
                ))
        except Exception as e:
            logger.error(f"Error logging market regime: {e}")
