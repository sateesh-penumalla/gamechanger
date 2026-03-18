import os
import pandas as pd
from loguru import logger
from typing import List, Optional, Any
from dotenv import load_dotenv
from src.data.dhan_client import DhanDataClient

load_dotenv()

import os
import pandas as pd
import yfinance as yf
from loguru import logger
from typing import List, Optional, Any
from dotenv import load_dotenv
from src.data.dhan_client import DhanDataClient

load_dotenv()

class YahooFinanceData:
    """
    Yahoo Finance Data Client.
    Used for historical/weekly technical analysis where Dhan rate limits are too restrictive.
    """
    def __init__(self):
        logger.info("Initializing Authentic Yahoo Finance Data Layer...")
        # We keep Dhan client as fallback or for quote data if needed
        self.client_id = os.getenv("DHAN_CLIENT_ID")
        self.access_token = os.getenv("DHAN_ACCESS_TOKEN")
        
        if self.client_id and self.access_token:
            self.dhan = DhanDataClient(self.client_id, self.access_token)
        else:
            self.dhan = None

    def fetch_realtime_data(self, symbol: str, period: str = "1d", interval: str = "5m", start: Optional[Any] = None, end: Optional[Any] = None) -> Optional[pd.DataFrame]:
        """Fetch OHLCV data using yfinance."""
        # Clean symbol for Yahoo (.NS for NSE stocks)
        yahoo_symbol = symbol.replace(".NS", "")
        if not yahoo_symbol.endswith(".NS") and not yahoo_symbol.startswith("^"):
            yahoo_symbol = f"{yahoo_symbol}.NS"
            
        # mapping specific indices
        if symbol == "NIFTY" or symbol == "^NSEI": yahoo_symbol = "^NSEI"
        if symbol == "BANKNIFTY" or symbol == "^NSEBANK": yahoo_symbol = "^NSEBANK"
        
        try:
            # yfinance uses interval strings like '1m', '5m', '1h', '1d', '1wk', '1mo'
            # period like '1d', '5d', '1mo', '1y', 'max'
            
            ticker = yf.Ticker(yahoo_symbol)
            df = ticker.history(period=period, interval=interval, start=start, end=end)
            
            if df is None or df.empty:
                logger.warning(f"Yahoo Finance: No data for {yahoo_symbol}")
                return None
                
            # Rename columns to match Dhan standard (Capitalized)
            df = df.rename(columns={
                'Open': 'Open',
                'High': 'High',
                'Low': 'Low',
                'Close': 'Close',
                'Volume': 'Volume'
            })
            return df
            
        except Exception as e:
            logger.error(f"Yahoo Finance Error for {yahoo_symbol}: {e}")
            return None

    def get_quote_data(self, symbol: str) -> Optional[dict]:
        """Fetches latest quote. Prefers Dhan for real-time accuracy if available."""
        if self.dhan:
            clean_symbol = symbol.replace(".NS", "").replace("^", "")
            if symbol == "^NSEI": clean_symbol = "NIFTY"
            if symbol == "^NSEBANK": clean_symbol = "BANKNIFTY"
            return self.dhan.get_quote_data(clean_symbol)
        
        # Fallback to yfinance if Dhan not available
        try:
            yahoo_symbol = symbol if ".NS" in symbol or "^" in symbol else f"{symbol}.NS"
            ticker = yf.Ticker(yahoo_symbol)
            info = ticker.info
            return {
                "ltp": info.get("regularMarketPrice"),
                "change": info.get("regularMarketChangePercent"),
                "volume": info.get("regularMarketVolume")
            }
        except:
            return None

    def fetch_bulk_data(self, symbols: List[str]) -> dict:
        """Fetch data for multiple stocks using yfinance bulk download."""
        yahoo_symbols = []
        for s in symbols:
            ys = s.replace(".NS", "")
            if not ys.endswith(".NS") and not ys.startswith("^"):
                ys = f"{ys}.NS"
            if s == "^NSEI": ys = "^NSEI"
            if s == "^NSEBANK": ys = "^NSEBANK"
            yahoo_symbols.append(ys)
            
        try:
            data = yf.download(yahoo_symbols, period="1d", interval="5m", group_by='ticker', threads=True)
            results = {}
            for ys in yahoo_symbols:
                # Map back to original symbol
                orig = symbols[yahoo_symbols.index(ys)]
                if len(yahoo_symbols) > 1:
                    df = data[ys]
                else:
                    df = data
                results[orig] = df
            return results
        except Exception as e:
            logger.error(f"Yahoo Bulk Download Error: {e}")
            return {}
