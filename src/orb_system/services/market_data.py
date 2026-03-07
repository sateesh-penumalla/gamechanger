from __future__ import annotations
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional
import pandas as pd
from src.orb_system.config.database import db
from src.orb_system.config.dhan_config import dhan_client
from src.orb_system.services.technical_indicators import TechnicalIndicators
from loguru import logger
import os

class MarketDataService:
    """Fetch and manage market data from DhanHQ and Local Cache"""
    
    def __init__(self):
        self.dhan = dhan_client.get_client()
        self.ti = TechnicalIndicators()
    
    def get_historical_data(self, 
                           symbol: str, 
                           from_date: datetime, 
                           to_date: datetime,
                           interval: str = "1") -> pd.DataFrame:
        """
        Fetch historical data from DhanHQ
        interval: "1" for 1-min, "5" for 5-min, "15" for 15-min, "D" for daily
        """
        try:
            # 1. Try local CSV discovery
            # For backtesting, we often need data from specific folders
            date_str = from_date.strftime('%Y-%m-%d')
            
            # Try plain symbol, then symbol.NS, then case-insensitive
            search_folders = [f"data/history/{date_str}"]
            # If lookback spans days, we might need to search other folders too, 
            # but for ORB we usually just need the trade day's data.
            # For volume lookback (interval='D'), we return the single day's avg if multi-day logic isn't here.
            
            df_list = []
            
            # Simple single-day lookup for now, but more robust pathing
            symbol_clean = symbol.strip().upper()
            potential_files = [f"{symbol_clean}.csv", f"{symbol_clean}.NS.csv", f"{symbol_clean.lower()}.csv"]
            
            for folder in search_folders:
                if not os.path.exists(folder): continue
                
                found_file = None
                for pf in potential_files:
                    path = os.path.join(folder, pf)
                    if os.path.exists(path):
                        found_file = path
                        break
                
                if found_file:
                    logger.info(f"Using local CSV data: {found_file}")
                    df = pd.read_csv(found_file)
                    df.columns = [c.lower() for c in df.columns]
                    if 'datetime' in df.columns:
                        df = df.rename(columns={'datetime': 'timestamp'})
                    
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        # Filter for requested range
                        mask = (df['timestamp'] >= from_date) & (df['timestamp'] <= to_date)
                        df = df.loc[mask].sort_values('timestamp')
                        if not df.empty:
                            return df

            # 2. DhanHQ API call as fallback
            if not self.dhan:
                logger.warning(f"Dhan client not initialized and no local data for {symbol}")
                return pd.DataFrame()
            
            # Check if method exists (handle different SDK versions)
            if hasattr(self.dhan, 'historical_data'):
                data = self.dhan.historical_data(
                    symbol=symbol,
                    exchange_segment='NSE_EQ',
                    instrument_type='EQUITY',
                    from_date=from_date.strftime('%Y-%m-%d'),
                    to_date=to_date.strftime('%Y-%m-%d'),
                    interval=interval
                )
            else:
                logger.warning(f"Dhan client lacks 'historical_data' method. Skipping API call.")
                return pd.DataFrame()
            
            if data and 'data' in data:
                df = pd.DataFrame(data['data'])
                # Standardize column names if needed
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp')
                return df
            else:
                logger.warning(f"No data received for {symbol}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {str(e)}")
            return pd.DataFrame()
    
    def get_live_quote(self, symbol: str) -> Optional[Dict]:
        """Get live quote for symbol"""
        if not self.dhan: return None
        try:
            quote = self.dhan.get_quote(
                symbol=symbol,
                exchange_segment='NSE_EQ'
            )
            # Standardize output format
            if quote and 'status' in quote and quote['status'] == 'success':
                 return quote.get('data', {})
            return quote
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {str(e)}")
            return None
    
    def cache_1min_data(self, symbol: str, df: pd.DataFrame):
        """Cache 1-min data to database for quick access"""
        try:
            with db.get_cursor() as cursor:
                # Optimized batch insert
                rows_to_insert = []
                for _, row in df.iterrows():
                    # Calculate basic indicators for cache
                    historical_closes = df[df['timestamp'] <= row['timestamp']]['close'].tolist()
                    
                    rsi = self.ti.calculate_rsi(historical_closes) if len(historical_closes) >= 14 else 50.0
                    macd, signal, hist = self.ti.calculate_macd(historical_closes) if len(historical_closes) >= 26 else (0, 0, 0)
                    
                    session_df = df[df['timestamp'].dt.date == row['timestamp'].date()]
                    session_df = session_df[session_df['timestamp'] <= row['timestamp']]
                    vwap = self.ti.calculate_vwap(session_df).iloc[-1] if len(session_df) > 0 else row['close']
                    
                    rows_to_insert.append((
                        symbol, row['timestamp'], row['open'], row['high'], 
                        row['low'], row['close'], row['volume'],
                        rsi, macd, signal, hist, vwap
                    ))
                
                if rows_to_insert:
                    cursor.executemany("""
                        INSERT INTO market_data_cache 
                        (symbol, timestamp, open, high, low, close, volume, 
                         rsi_14, macd, macd_signal, macd_hist, vwap)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        open=VALUES(open), high=VALUES(high), low=VALUES(low),
                        close=VALUES(close), volume=VALUES(volume),
                        rsi_14=VALUES(rsi_14), macd=VALUES(macd),
                        macd_signal=VALUES(macd_signal), macd_hist=VALUES(macd_hist),
                        vwap=VALUES(vwap)
                    """, rows_to_insert)
                    
        except Exception as e:
            logger.error(f"Error caching 1min data for {symbol}: {str(e)}")

    def get_cached_data(self, symbol: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Retrieve cached market data"""
        try:
            with db.get_cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT * FROM market_data_cache
                    WHERE symbol = %s
                    AND timestamp BETWEEN %s AND %s
                    ORDER BY timestamp
                """, (symbol, start_time, end_time))
                
                data = cursor.fetchall()
                if data:
                    df = pd.DataFrame(data)
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    return df
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error retrieving cached data: {e}")
            return pd.DataFrame()

    def calculate_average_volume(self, symbol: str, lookback_days: int = 20, reference_date: Optional[date] = None) -> float:
        """Calculate average volume over lookback period"""
        try:
             # Basic implementation using daily data
             end_date = datetime.combine(reference_date or datetime.now().date(), datetime.min.time())
             start_date = end_date - timedelta(days=lookback_days + 10)
             df = self.get_historical_data(symbol, start_date, end_date, interval="D")
             if not df.empty:
                 return df['volume'].tail(lookback_days).mean()
             return 0.0
        except Exception as e:
             logger.error(f"Error calculating avg volume: {e}")
             return 0.0
