
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.db.schema import Ticker, Base
from dotenv import load_dotenv

load_dotenv()

def populate_snipers():
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define SNIPER stocks that have history data for 2026-02-06
    snipers = [
        {"symbol": "RELIANCE", "sector": "ENERGY"},
        {"symbol": "HDFCBANK", "sector": "BANKING"},
        {"symbol": "TCS", "sector": "IT"},
        {"symbol": "INFY", "sector": "IT"},
        {"symbol": "ICICIBANK", "sector": "BANKING"},
        {"symbol": "SBIN", "sector": "BANKING"},
        {"symbol": "BHARTIARTL", "sector": "TELECOM"}
    ]

    print("Populating 'tickers' table with SNIPER stocks...")

    for item in snipers:
        # Check if already exists
        exists = session.query(Ticker).filter_by(symbol=item['symbol']).first()
        if exists:
            print(f"Updating {item['symbol']} to SNIPER.")
            exists.oracle_status = 'SNIPER'
            exists.sector = item['sector']
        else:
            ticker = Ticker(
                symbol=item['symbol'],
                sector=item['sector'],
                oracle_status='SNIPER'
            )
            session.add(ticker)
            print(f"Added {item['symbol']} as SNIPER.")

    session.commit()
    print("Population complete.")
    session.close()

if __name__ == "__main__":
    populate_snipers()
