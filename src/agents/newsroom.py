from loguru import logger
from typing import Dict, List, Optional, Any
import os

try:
    from google import genai
    HAS_GENAI = True
except ImportError:
    logger.warning("google-genai package not found or incompatible with Python version. Newsroom will use mock sentiment.")
    HAS_GENAI = False

class NewsroomAgent:
    # Class-level cache to persist across instances (symbols/sessions)
    _cache: Dict[str, Dict] = {}

    def __init__(self, gemini_client: Optional[Any] = None):
        logger.info("Newsroom Agent Initialized")
        self.client = gemini_client
        
        if not self.client and HAS_GENAI:
            api_key = os.getenv("GOOGLE_API_KEY")
            if api_key:
                try:
                    self.client = genai.Client(api_key=api_key)
                except Exception as e:
                    logger.error(f"Failed to initialize Gemini Client: {e}")
                    self.client = None
            else:
                logger.warning("GOOGLE_API_KEY not found. Newsroom will use mock sentiment.")

    def analyze_sentiment(self, symbol: str) -> Dict:
        """
        Scrapes news and uses Gemini to provide a 0-100 sentiment score.
        Caches results for 15 minutes to optimize API usage.
        """
        from datetime import datetime, timedelta
        
        # Check Cache (15 min TTL)
        if symbol in self._cache:
            entry = self._cache[symbol]
            if datetime.now() - entry['timestamp'] < timedelta(minutes=15):
                logger.info(f"Using cached news sentiment for {symbol} (Age: {(datetime.now() - entry['timestamp']).total_seconds()/60:.1f}m)")
                return entry['data']

        if not self.client:
            # Fallback mock logic for stability
            import random
            mock_score = random.randint(45, 65)
            logger.info(f"Gemini unavailable. Providing mock sentiment (50) for {symbol}")
            return {
                "symbol": symbol,
                "sentiment": "Neutral (Mock)",
                "score": 50,
                "reason": "Gemini API client not initialized or incompatible environment.",
                "highlights": ["Gemini AI is currently inactive. Using default neutral stance."]
            }

        logger.info(f"Analyzing real-time sentiment for {symbol}...")
        
        # 1. Fetch News
        news_data = self._fetch_news(symbol)
        
        if not news_data:
            return {
                "sentiment": "Neutral",
                "score": 50,
                "reason": "No news found",
                "highlights": ["No recent news found for this symbol."]
            }

        # 2. Get AI Sentiment
        res = self._get_gemini_sentiment(symbol, news_data)
        if "reason" not in res: res["reason"] = "AI Analysis"
        
        # Update Cache
        from datetime import datetime
        self._cache[symbol] = {
            "timestamp": datetime.now(),
            "data": res
        }
        
        return res

    def _fetch_news(self, symbol: str) -> List[str]:
        """News fetching disabled: Yahoo Finance removed per request."""
        # Dhan API does not provide a direct equivalent for news headlines.
        # This will be replaced with a proper news source (e.g. Google News RSS or similar) in the future.
        logger.warning(f"News fetching for {symbol} skipped: Yahoo Finance disabled.")
        return []

    def _get_gemini_sentiment(self, symbol: str, news_data: List[str]) -> Dict:
        """Uses Gemini to evaluate sentiment from news strings."""
        try:
            news_text = "\n---\n".join(news_data)
            prompt = f"""
            Analyze the market sentiment for the stock {symbol} based on these recent news headlines and summaries:
            
            {news_text}
            
            Respond strictly in the following JSON format:
            {{
                "sentiment": "Bullish" | "Bearish" | "Neutral",
                "score": int (0-100 where 0 is extremely negative, 100 is extremely positive, 50 is neutral),
                "highlights": ["point 1", "point 2"] (max 3 key takeaways)
            }}
            """
            
            response = self.client.models.generate_content(
                model='gemini-2.0-flash', 
                contents=prompt,
                config={'response_mime_type': 'application/json'}
            )
            
            import json
            return json.loads(response.text)
            
        except Exception as e:
            logger.error(f"AI Sentiment error for {symbol}: {e}")
            return {
                "sentiment": "Neutral",
                "score": 50,
                "highlights": ["Error during AI analysis."]
            }
