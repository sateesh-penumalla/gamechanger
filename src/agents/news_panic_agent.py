import re
from loguru import logger
from typing import List, Dict, Optional, Any
from google.genai import types

class NewsPanicAgent:
    """
    Specialized news agent for detecting panic-inducing catalysts.
    Uses keyword-driven weights and Gemini analysis to score fear intensity.
    """
    def __init__(self, gemini_client: Any = None):
        self.client = gemini_client
        self.sources = {
            "NSE_OFFICIAL": 2.0,
            "BSE_OFFICIAL": 2.0,
            "REUTERS": 1.5,
            "MONEYCONTROL": 1.2,
            "TWITTER": 0.4
        }
        
        self.panic_keywords = {
            r"fraud|scam|misappropriation|default": 10,
            r"raid|seizure|search|cbi|ed": 9,
            r"sebi|penalty|ban|show cause": 8,
            r"resigns|quits|step down|fired": 7,
            r"crash|plunge|tank|bloodbath": 5,
            r"downgrade|sell rating": 4
        }
        logger.info("News Panic Agent Initialized - Watching for Black Swans...")

    def calculate_headline_score(self, headline: str, source: str) -> float:
        """
        Calculates a raw panic score based on keywords and source credibility.
        """
        score = 0
        h = headline.lower()
        
        # 1. Keyword check
        for pattern, weight in self.panic_keywords.items():
            if re.search(pattern, h):
                score += (weight * 10)
        
        # 2. Source Multiplier
        multiplier = self.sources.get(source.upper(), 0.5)
        return min(score * multiplier, 100)

    def verify_panic_with_ai(self, symbol: str, headline: str) -> Dict:
        """
        Uses Gemini to confirm if the news is a secular bear trigger or noise.
        """
        if not self.client:
            return {"score": 50, "reason": "AI inactive, using keyword fallback"}

        prompt = f"""
        Analyze this news headline for {symbol} for INTENSE PANIC SELLING potential.
        Headline: "{headline}"
        
        Tasks:
        1. Categorize: FRAUD, REGULATORY, EARNINGS_MISS, or NOISE.
        2. Impact: Will it likely cause a >5% intraday drop? (Yes/No)
        3. Panic Score: 0 (Bullish) to 100 (Total Panic).
        
        Return ONLY a JSON:
        {{"category": "...", "impact": "...", "score": 85, "reason": "..."}}
        """
        
        try:
            response = self.client.models.generate_content(
                model="gemini-2.0-flash-exp",
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            return response.parsed
        except Exception as e:
            logger.error(f"AI Panic Verification failed: {e}")
            return {"score": 50, "reason": "Error during AI analysis"}

    def scan_for_panic_headlines(self, symbol: str) -> Dict:
        """
        Mocks fetching headlines and scoring them.
        In a real scenario, this would call a News API.
        """
        # Placeholder for real news fetching
        return {"panic_score": 0, "headline": "No major panic news found"}
