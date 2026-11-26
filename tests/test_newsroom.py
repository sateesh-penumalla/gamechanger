import os
import sys
from loguru import logger
from dotenv import load_dotenv

# Add current directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.agents.newsroom import NewsroomAgent

def test_newsroom():
    load_dotenv()
    
    agent = NewsroomAgent()
    
    # Test with a major stock
    symbol = "RELIANCE"
    print(f"\n--- Testing Newsroom for {symbol} ---")
    
    result = agent.analyze_sentiment(symbol)
    
    print(f"SYMBOL: {symbol}")
    print(f"SENTIMENT: {result['sentiment']}")
    print(f"SCORE: {result['score']}")
    print(f"HIGHLIGHTS: {result['highlights']}")
    
    if result['score'] != 50 or result['sentiment'] != "Neutral" or agent.model is not None:
         print("\nResult Analysis: The agent successfully performed a dynamic check!")
    else:
         print("\nResult Analysis: The agent returned default values (likely no API key or no news).")

if __name__ == "__main__":
    test_newsroom()
