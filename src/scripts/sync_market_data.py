import os
import sys
import pandas as pd
import argparse
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.yfinance_client import YahooFinanceData
from src.data.dhan_client import DhanDataClient
from src.agents.oracle import OracleAgent
from src.agents.newsroom import NewsroomAgent
from src.db.schema import init_db

def sync_market_data():
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Bulk sync Ticker scores (Technicals & Sentiment).")
    parser.add_argument("--limit", type=int, help="Limit number of stocks to sync.")
    parser.add_argument("--no-sentiment", action="store_true", help="Skip sentiment sync (faster).")
    args = parser.parse_args()

    # 1. Setup Database
    db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root@localhost/bharatquant_sniper")
    engine = create_engine(db_url)
    init_db(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # 2. Setup Data Client
    source = os.getenv("DATA_SOURCE", "yahoo").lower()
    if source == "dhan":
        cid = os.getenv("DHAN_CLIENT_ID")
        token = os.getenv("DHAN_ACCESS_TOKEN")
        data_client = DhanDataClient(cid, token)
    else:
        data_client = YahooFinanceData()

    # 3. Setup Agents
    oracle = OracleAgent(data_client, session)
    news_agent = NewsroomAgent() if not args.no_sentiment else None

    # 4. Load Scrip List
    csv_path = "sec_list.csv"
    if not os.path.exists(csv_path):
        logger.error(f"Scrip list {csv_path} not found!")
        return

    symbols = pd.read_csv(csv_path)['Symbol'].tolist()
    if args.limit:
        symbols = symbols[:args.limit]
        
    logger.info(f"Starting Bulk Ticker Sync for {len(symbols)} stocks (Sentiment: {not args.no_sentiment})...")

    # 5. Run Sync
    oracle.sync_ticker_db(symbols, news_agent=news_agent)

    logger.info("Bulk Sync Complete. Ticker table is now prime for War Room.")

if __name__ == "__main__":
    sync_market_data()
