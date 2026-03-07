import os
import sys
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add root directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.db.schema import Ticker, init_db

def sync_tickers():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not found in .env")
        return

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 1. Load sec_list.csv
        df = pd.read_csv('sec_list.csv')
        symbols = df['Symbol'].astype(str).str.strip().tolist()
        logger.info(f"Read {len(symbols)} symbols from sec_list.csv")

        # 2. Get existing symbols in DB
        existing_symbols = {t.symbol for t in session.query(Ticker.symbol).all()}
        logger.info(f"Found {len(existing_symbols)} symbols already in DB.")

        # 3. Add missing symbols
        added_count = 0
        for symbol in symbols:
            if symbol not in existing_symbols:
                new_ticker = Ticker(symbol=symbol, sector="UNKNOWN")
                session.add(new_ticker)
                added_count += 1

        session.commit()
        logger.success(f"Successfully added {added_count} new symbols to 'tickers' table.")

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    sync_tickers()
