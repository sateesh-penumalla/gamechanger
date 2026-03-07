import os
import sys
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

# Add project root to path
sys.path.append(os.getcwd())

from src.data.dhan_client import DhanDataClient
from src.agents.newsroom import NewsroomAgent
from src.agents.catalyst_hunter import CatalystHunterAgent
from src.db.schema import CatalystScan, init_db, get_ist_now

def run_catalyst_hunter():
    logger.info("Starting Catalyst Hunter Scan...")
    
    # 1. Setup Clients
    data_client = DhanDataClient()
    newsroom = NewsroomAgent()
    
    # 2. Setup DB
    db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root:root@127.0.0.1:3307/bharatquant_sniper")
    engine = create_engine(db_url)
    init_db(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    hunter = CatalystHunterAgent(data_client, newsroom, session)

    # 3. Fetch Symbols (F&O Universe preferred)
    try:
        import pandas as pd
        tickers_df = pd.read_sql("SELECT symbol FROM tickers", engine)
        symbols = tickers_df['symbol'].tolist()
    except Exception as e:
        logger.error(f"Failed to fetch tickers: {e}")
        symbols = ["ABB", "GODFRYPHLP", "TARIL", "VBL", "RELIANCE"]

    # 4. Run Scan
    # The agent internally loops and persists incrementally.
    try:
        hunter.find_breakout_candidates(symbols)
    except Exception as e:
        logger.error(f"Catalyst Hunter terminated due to critical error: {e}")
    
    session.close()
    logger.info("Catalyst Hunter Scan Completed.")
    
    session.close()
    logger.info("Catalyst Hunter Scan Completed.")

if __name__ == "__main__":
    run_catalyst_hunter()
