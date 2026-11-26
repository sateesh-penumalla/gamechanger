import os
import sys
import pandas as pd
from unittest.mock import MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add current directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.agents.oracle import OracleAgent
from src.db.schema import init_db, Ticker

def test_oracle_sync():
    print("Verifying Oracle Agent and Ticker Persistence...")

    # 1. Setup DB
    db_url = "mysql+pymysql://root@localhost/bharatquant_mas"
    engine = create_engine(db_url)
    init_db(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # 2. Mock Data
    # 30 weeks of data
    data = pd.DataFrame({
        "Close": [100.0 + i for i in range(30)],
        "Volume": [1000] * 30
    })
    
    # 3. Setup Mocks
    mock_data_client = MagicMock()
    # Oracle asks for period="2y", interval="1wk"
    mock_data_client.fetch_realtime_data.return_value = data
    
    oracle = OracleAgent(mock_data_client, session)
    
    # Run analysis on a test symbol
    print("Analyzing RELIANCE...")
    analysis = oracle.analyze_weekly_trend("RELIANCE")
    print(f"Analysis: {analysis}")
    
    # Sync to DB
    print("Syncing to database...")
    oracle.sync_ticker_db(["RELIANCE"])
    
    # Verify DB entry
    ticker = session.query(Ticker).filter(Ticker.symbol == "RELIANCE").first()
    if ticker:
        print(f"DB Entry Found: {ticker.symbol} | Status: {ticker.oracle_status} | RSI: {ticker.weekly_rsi}")
        assert ticker.symbol == "RELIANCE"
        assert ticker.oracle_status in ["SNIPER", "FILTERED"]
        print("SUCCESS: Oracle Persistence Verified!")
    else:
        print("FAIL: No DB entry found for RELIANCE")

if __name__ == "__main__":
    test_oracle_sync()
