from loguru import logger
import pandas as pd
from typing import Dict, Optional, List, Any
from src.data.yfinance_client import YahooFinanceData

class SectorGeneralAgent:
    _cache: Dict[str, Dict] = {}

    def __init__(self, data_client: Any):
        self.data_client = data_client
        # Mapping common stocks to their sectoral indices
        self.sector_map = {
            "TCS": "^CNXIT", "INFY": "^CNXIT", "WIPRO": "^CNXIT", "HCLTECH": "^CNXIT", "TECHM": "^CNXIT",
            "HDFCBANK": "^NSEBANK", "ICICIBANK": "^NSEBANK", "SBIN": "^NSEBANK", "KOTAKBANK": "^NSEBANK", "AXISBANK": "^NSEBANK",
            "MARUTI": "^CNXAUTO", "TATAMOTORS": "^CNXAUTO", "M&M": "^CNXAUTO", "ASHOKLEY": "^CNXAUTO", "BAJAJ-AUTO": "^CNXAUTO",
            "SUNPHARMA": "^CNXPHARMA", "DRREDDY": "^CNXPHARMA", "CIPLA": "^CNXPHARMA", "AUBANK": "^NSEBANK",
            "LT": "^CNXINFRA", "RELIANCE": "^CNXENERGY", "ONGC": "^CNXENERGY", "NTPC": "^CNXENERGY",
            "ITC": "^CNXFMCG", "HUL": "^CNXFMCG", "NESTLEIND": "^CNXFMCG", "BRITANNIA": "^CNXFMCG",
            "TATASTEEL": "^CNXMETAL", "HINDALCO": "^CNXMETAL", "JSWSTEEL": "^CNXMETAL", "COALINDIA": "^CNXMETAL",
            "DLF": "^CNXREALTY", "GODREJPROP": "^CNXREALTY", "OBEROIRLTY": "^CNXREALTY",
            "VEDL": "^CNXMETAL", "UNIONBANK": "^NSEBANK", "PNB": "^NSEBANK", "CANBK": "^NSEBANK"
        }
        logger.info("Sector General Agent Initialized with Caching")

    def get_sector_index(self, symbol: str) -> Optional[str]:
        """Returns the NSE Sectoral Index symbol for a given stock symbol."""
        # Remove suffix if any
        clean_symbol = symbol.replace(".NS", "").replace(".BO", "")
        return self.sector_map.get(clean_symbol)

    def is_sector_bullish(self, sector_index: str, trade_date: Optional[Any] = None) -> bool:
        """
        Checks if the sectoral index is bullish (Price > VWAP).
        Supports live (no trade_date) and historical (with trade_date).
        """
        from datetime import datetime, timedelta
        now = datetime.now()
        
        # Cache only for live usage (15 min TTL)
        if not trade_date and sector_index in self._cache:
            entry = self._cache[sector_index]
            if now - entry['timestamp'] < timedelta(minutes=15):
                return entry['status']

        try:
            if trade_date:
                # Historical: fetch data for that specific day
                start_dt = datetime.combine(trade_date, datetime.min.time())
                end_dt = start_dt + timedelta(days=1)
                data = self.data_client.fetch_realtime_data(sector_index, start=start_dt, end=end_dt, interval="5m")
            else:
                # Live: fetch recent data
                data = self.data_client.fetch_realtime_data(sector_index, period="1d", interval="5m")
                
            if data is None or data.empty:
                logger.warning(f"No data for sector index {sector_index} on {trade_date or 'Live'}")
                return True # Default to True
            
            # Use only opening range for historical to avoid lookahead
            if trade_date:
                # Check trend as of 9:45 AM
                cutoff = datetime.combine(trade_date, datetime.strptime("09:45", "%H:%M").time())
                data = data[data.index <= cutoff]
                if data.empty: return True

            # Calculate VWAP
            close = data['Close']
            volume = data['Volume']
            vwap = (close * volume).cumsum() / volume.cumsum()
            
            is_bullish = close.iloc[-1] > vwap.iloc[-1]
            
            # Update cache only for live
            if not trade_date:
                self._cache[sector_index] = {"status": is_bullish, "timestamp": now}
            return is_bullish
            
        except Exception as e:
            logger.error(f"Error checking sector {sector_index}: {e}")
            return True

    def validate_trend(self, symbol: str, trade_date: Optional[Any] = None) -> Dict:
        """
        Validates if the stock's sector is supportive.
        """
        sector_index = self.get_sector_index(symbol)
        if not sector_index:
            return {"status": "NEUTRAL", "reason": "No sector mapping found"}
        
        is_bullish = self.is_sector_bullish(sector_index, trade_date=trade_date)
        if is_bullish:
            return {"status": "BULLISH", "index": sector_index}
        else:
            return {"status": "BEARISH", "index": sector_index, "reason": f"Sector {sector_index} is below VWAP"}
