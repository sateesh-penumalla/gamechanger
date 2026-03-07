from loguru import logger
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from src.db.schema import Ticker, get_ist_now
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

class OracleAgent:
    def __init__(self, data_client, db_session: Session):
        self.data_client = data_client
        self.db = db_session
        logger.info("Oracle Agent Initialized")

    def analyze_weekly_trend(self, symbol: str) -> Dict:
        """
        Fetches weekly data and determines institutional trend.
        Uses 20-week SMA and Weekly RSI (14).
        """
        # Fetch 2 years of weekly data to calculate SMA
        data = self.data_client.fetch_realtime_data(symbol, period="2y", interval="1wk")
    
        if data is None or data.empty:
            return {"symbol": symbol, "status": "IGNORE", "reason": "Delisted or Invalid Symbol"}

        if len(data) < 20:
            return {"symbol": symbol, "status": "FILTERED", "reason": "Insufficient weekly data"}

        # Calculate Indicators manually
        # SMA 20
        data['SMA_20'] = data['Close'].rolling(window=20).mean()
        
        # RSI 14
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss.replace(0, np.nan)
        data['RSI_14'] = 100 - (100 / (1 + rs))

        current = data.iloc[-1]
        price = current['Close']
        sma_20 = current['SMA_20']
        rsi_14 = current['RSI_14']
        
        # Calculate Average Daily Turnover (₹ Crores)
        # Turnover = Price * Volume. Since it's weekly data, we divide by 5 for daily avg.
        # 1 Crore = 10,000,000
        data['Turnover'] = data['Close'] * data['Volume']
        avg_weekly_turnover = data['Turnover'].rolling(window=2).mean().iloc[-1]
        adtv_cr = (avg_weekly_turnover / 5) / 10000000 if pd.notnull(avg_weekly_turnover) else 0

        # Revised Directional "Sniper" Logic:
        # UP_SNIPER: Price > SMA20 AND RSI > 60 AND ADTV > 5 Cr
        # DOWN_SNIPER: Price < SMA20 AND RSI < 40 AND ADTV > 5 Cr
        status = "FILTERED"
        if pd.notnull(price) and pd.notnull(sma_20) and pd.notnull(rsi_14):
            # Pure Technical Sniper Logic (Liquidity Filtered at Execution Level)
            if price > sma_20 and rsi_14 > 60:
                status = "UP_SNIPER"
            elif price < sma_20 and rsi_14 < 40:
                status = "DOWN_SNIPER"
        
        return {
            "symbol": symbol,
            "status": status,
            "rsi": round(rsi_14, 2) if pd.notnull(rsi_14) else 0,
            "sma": round(sma_20, 2) if pd.notnull(sma_20) else 0,
            "price": round(price, 2) if pd.notnull(price) else 0,
            "adtv_cr": round(adtv_cr, 2)
        }

    def sync_ticker_db(self, symbols: Optional[List[str]] = None, news_agent: Optional[Any] = None, max_workers: int = 10):
        """
        Updates the 'tickers' table with weekly analysis and optional sentiment.
        Uses ThreadPoolExecutor for high-speed parallel sync.
        """
        if symbols is None:
            symbols = [t.symbol for t in self.db.query(Ticker.symbol).all()]
            logger.info(f"Oracle discovered {len(symbols)} symbols from 'tickers' table.")

        logger.info(f"Oracle initiating parallel sync for {len(symbols)} symbols (workers={max_workers})...")
        
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {executor.submit(self.analyze_weekly_trend, symbol): symbol for symbol in symbols}
            
            for i, future in enumerate(as_completed(future_to_symbol)):
                symbol = future_to_symbol[future]
                try:
                    analysis = future.result()
                    results.append(analysis)
                    if i % 20 == 0:
                        logger.info(f"Oracle Progress: {i}/{len(symbols)} analyzed...")
                except Exception as e:
                    logger.error(f"Failed to analyze {symbol}: {e}")

        # Bulk Update in DB
        logger.info("Oracle applying analysis to database...")
        for i, analysis in enumerate(results):
            try:
                symbol = analysis["symbol"]
                ticker = self.db.query(Ticker).filter(Ticker.symbol == symbol).first()
                
                if not ticker:
                    ticker = Ticker(symbol=symbol)
                    self.db.add(ticker)
                
                ticker.weekly_rsi = analysis.get("rsi")
                ticker.weekly_sma = analysis.get("sma")
                ticker.oracle_status = analysis["status"]
                ticker.avg_daily_turnover = analysis.get("adtv_cr")
                
                # Initialize scores if null
                ticker.scout_score = ticker.scout_score or 0
                ticker.tech_score = ticker.tech_score or 0
                ticker.mood_score = ticker.mood_score or 0
                ticker.confidence_score = ticker.confidence_score or 0
                
                if news_agent:
                    sent = news_agent.analyze_sentiment(symbol)
                    ticker.sentiment_score = sent["score"]
                    ticker.sentiment_updated_at = get_ist_now()
                
                ticker.last_updated = get_ist_now()
                
                # Commit in batches
                if i % 20 == 0:
                    self.db.commit()
            except Exception as e:
                logger.error(f"Error persisting {symbol}: {e}")
                
        self.db.commit()
        
        # --- NEW: Populate DailyFocus immediately for dashboard visibility ---
        try:
            from src.db.schema import DailyFocus
            today = get_ist_now().date()
            logger.info("Oracle: Populating DailyFocus with identified SNIPERS...")
            
            # Re-query Tickers to get all snipers (including those just updated)
            snipers = self.db.query(Ticker).filter(Ticker.oracle_status.in_(['UP_SNIPER', 'DOWN_SNIPER'])).all()
            
            count = 0
            for t in snipers:
                # Upsert DailyFocus
                focus = self.db.query(DailyFocus).filter(
                    DailyFocus.symbol == t.symbol, 
                    DailyFocus.date == today # Implicit cast to datetime? DB stores datetime.
                ).first()
                
                if not focus:
                    # Note: We use datetime for 'date' column in schema, but filter by date part usually.
                    # Standardizing to start of day for PK
                    from datetime import datetime
                    ts = datetime.combine(today, datetime.min.time())
                    
                    focus = DailyFocus(symbol=t.symbol, date=ts)
                    self.db.add(focus)
                
                # Update basic info
                focus.sector = t.sector
                focus.oracle_status = t.oracle_status
                focus.weekly_rsi = t.weekly_rsi
                focus.weekly_sma = t.weekly_sma
                focus.scout_score = t.scout_score
                focus.tech_score = t.tech_score
                focus.avg_daily_turnover = t.avg_daily_turnover
                
                count += 1
                
            self.db.commit()
            logger.info(f"Oracle: Added {count} symbols to DailyFocus (Pre-Market).")
            
        except Exception as e:
            logger.error(f"Oracle DailyFocus Population Error: {e}")

        logger.info("Oracle parallel sync complete.")
