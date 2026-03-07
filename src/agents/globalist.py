from loguru import logger
from src.data.yfinance_client import YahooFinanceData
from typing import Dict, Any, Optional

class GlobalistAgent:
    _cache: Dict[str, Dict] = {}

    def __init__(self, data_client: Any):
        self.data_client = data_client
        logger.info("Globalist Agent Initialized")

    def analyze_market_mood(self, trade_date: Optional[Any] = None) -> Dict:
        """
        Analyzes macro mood using Nifty and Bank Nifty vs Yesterday's Close.
        Caches results for 15 minutes.
        """
        from datetime import datetime, timedelta
        
        # Check Cache (only if live)
        cache_key = "market_mood"
        if not trade_date and cache_key in self._cache:
            entry = self._cache[cache_key]
            if datetime.now() - entry['timestamp'] < timedelta(minutes=15):
                return entry['data']

        logger.info(f"Analyzing Global and Index mood for {trade_date or 'Live'}...")
        
        # Fetch 2 days to compare vs Yesterday's Close
        indices = {"Nifty": "^NSEI", "BankNifty": "^NSEBANK"}
        index_scores = {}
        details = []

        for name, symbol in indices.items():
            if trade_date:
                # Historical: Need 2 days ending on trade_date
                start_dt = datetime.combine(trade_date - timedelta(days=3), datetime.min.time()) # Take 3 days to be safe w/ weekends
                end_dt = datetime.combine(trade_date, datetime.max.time())
                data = self.data_client.fetch_realtime_data(symbol, start=start_dt, end=end_dt, interval="5m")
            else:
                data = self.data_client.fetch_realtime_data(symbol, period="2d", interval="5m")

            if data is not None and not data.empty:
                # Group by date to find yesterday
                data['Date'] = data.index.date
                unique_dates = sorted(data['Date'].unique())
                
                # If target is trade_date, yesterday is the second-to-last unique date
                if len(unique_dates) >= 2:
                    yesterday_close = data[data['Date'] == unique_dates[-2]]['Close'].iloc[-1]
                    current_price = data.iloc[-1]['Close']
                    pct_change = ((current_price - yesterday_close) / yesterday_close) * 100
                    
                    # Store score for averaging
                    index_scores[name] = pct_change
                    details.append(f"{name}: {pct_change:+.2f}% vs Prev Close")

        # Composite Mood Calculation
        avg_pct = sum(index_scores.values()) / len(index_scores) if index_scores else 0
        
        mood = "Neutral"
        conviction = 50
        
        if avg_pct > 0.25:
            mood = "Bullish"
            conviction = 75 if avg_pct < 0.75 else 90
        elif avg_pct < -0.25:
            mood = "Bearish"
            conviction = 25 if avg_pct > -0.75 else 10

        res = {
            "mood": mood,
            "conviction": conviction,
            "details": details,
            "avg_pct": round(avg_pct, 2)
        }
        
        # Update Cache (only for live)
        if not trade_date:
            from datetime import datetime
            self._cache["market_mood"] = {
                "timestamp": datetime.now(),
                "data": res
            }
        return res
