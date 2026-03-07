import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from src.data.yfinance_client import YahooFinanceData
from src.agents.oracle import OracleAgent
from loguru import logger

# Load environment
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def run_adtv_backfill():
    if not DB_URL:
        logger.error("DATABASE_URL not found!")
        return

    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get all symbols from tickers table
        with engine.connect() as conn:
            result = conn.execute(text("SELECT symbol FROM tickers"))
            symbols = [r[0] for r in result]
        
        logger.info(f"Found {len(symbols)} symbols. Starting ADTV Backfill (2-Week Avg)...")
        
        data_client = YahooFinanceData()
        oracle = OracleAgent(data_client, session)
        
        # Run sync in batches to avoid overwhelming yfinance/rate limits
        batch_size = 50
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            logger.info(f"Processing batch {i} to {i+batch_size}...")
            oracle.sync_ticker_db(symbols=batch, max_workers=10)
            time.sleep(2) # Politeness delay
            
        logger.info("✅ ADTV Backfill Complete!")
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    run_adtv_backfill()
