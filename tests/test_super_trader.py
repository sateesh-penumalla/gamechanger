import os
import sys
import pandas as pd
from unittest.mock import MagicMock
from loguru import logger

# Add current directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.agents.scout import ScoutAgent
from src.agents.chartist import ChartistAgent
from src.agents.globalist import GlobalistAgent
from src.agents.newsroom import NewsroomAgent
from src.agents.librarian import LibrarianAgent
from src.core.orchestrator import BharatQuantOrchestrator

def test_strategy():
    logger.info("Verifying Super Trader Strategy...")

    # 1. Mock Data: First 15 mins (ORB)
    # Range: 99 - 101. High=101, Low=99.
    data = pd.DataFrame({
        "Open": [100.0, 100.5, 99.5, 101.5],
        "High": [101.0, 101.0, 100.0, 102.0],
        "Low": [99.0, 100.0, 99.5, 101.0],
        "Close": [100.5, 99.8, 100.0, 101.8],
        "Volume": [1000, 1100, 900, 3000]
    })
    
    # 2. Setup Mocks
    mock_data_client = MagicMock()
    mock_data_client.fetch_realtime_data.return_value = data
    
    mock_db = MagicMock()
    # Mock the SQLAlchemy query chain: query().filter().count()
    mock_db.query.return_value.filter.return_value.count.return_value = 0
    
    scout = ScoutAgent(mock_data_client)
    chartist = ChartistAgent()
    globalist = GlobalistAgent(mock_data_client)
    newsroom = NewsroomAgent()
    librarian = LibrarianAgent(mock_db)
    
    # Initialize Orchestrator
    orchestrator = BharatQuantOrchestrator(scout, chartist, globalist, newsroom, librarian)
    
    # Mock some agent outputs to force high confidence
    globalist.analyze_market_mood = MagicMock(return_value={"mood": "Bullish", "conviction": 80})
    newsroom.analyze_sentiment = MagicMock(return_value={"sentiment": "Bullish", "score": 75})
    chartist.analyze_technical_setup = MagicMock(return_value={"sentiment": "Bullish", "conviction": 80})
    
    # Run
    result = orchestrator.run(["MOCKSTOCK"])
    
    recs = result["recommendations"]
    print("\n" + "="*50)
    print("STRATEGY VERIFICATION RESULTS")
    print("="*50)
    
    if not recs:
        print("FAIL: No recommendations generated.")
    else:
        for r in recs:
            print(f"SYMBOL: {r['symbol']}")
            print(f"CONFIDENCE: {r['confidence']}%")
            print(f"ENTRY: {r['price']}")
            print(f"STOP LOSS: {r['sl']}")
            print(f"TARGET 1: {r['t1']}")
            print(f"TARGET 2: {r['t2']}")
            
            # Theoretical check
            # ORB Range High was 101. Entry is 101.1
            # ORB Range Low was 99. Risk was > 1.5%, so SL was capped at 1.5% below entry.
            assert r['price'] > 101
            assert r['sl'] < r['price']
            print("\nStrategy Levels Verified! Entry is above ORB High, SL is prudently managed.")

    print("="*50 + "\n")

if __name__ == "__main__":
    test_strategy()
