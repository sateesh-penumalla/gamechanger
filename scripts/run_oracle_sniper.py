
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from loguru import logger

# Add project root to path
sys.path.append(os.getcwd())

from src.agents.oracle import OracleAgent
from src.data.yfinance_client import YahooFinanceData

load_dotenv()

def run_sniper():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not found in environment")
        return

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        logger.info("Starting Oracle Sniper Sync...")
        data_client = YahooFinanceData()
        agent = OracleAgent(data_client, session)
        
        # Run the sync process
        agent.sync_ticker_db()
        
        logger.info("Oracle Sniper Sync completed successfully.")
    except Exception as e:
        logger.error(f"Oracle Sniper Sync failed: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    run_sniper()
