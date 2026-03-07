
import os
import pytz
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.db.schema import DailyFocus, Base
from dotenv import load_dotenv

load_dotenv()

def bootstrap_watchlist():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL not found in environment.")
        return

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    ist_tz = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist_tz).date()
    
    symbols = [
        {"symbol": "RELIANCE", "sector": "ENERGY", "orb_high": 2550.0, "orb_low": 2530.0},
        {"symbol": "HDFCBANK", "sector": "BANKING", "orb_high": 1650.0, "orb_low": 1630.0},
        {"symbol": "TCS", "sector": "IT", "orb_high": 4100.0, "orb_low": 4050.0}
    ]

    print(f"Bootstrapping watchlist for {today}...")

    for item in symbols:
        # Check if already exists
        exists = session.query(DailyFocus).filter_by(symbol=item['symbol'], date=today).first()
        if exists:
            print(f"Symbol {item['symbol']} already exists for today. Updating...")
            exists.oracle_status = 'SNIPER'
            exists.orb_high = item['orb_high']
            exists.orb_low = item['orb_low']
            exists.orb_high_clean = item['orb_high'] + 2
            exists.orb_low_clean = item['orb_low'] - 2
        else:
            focus = DailyFocus(
                symbol=item['symbol'],
                date=today,
                sector=item['sector'],
                oracle_status='SNIPER',
                orb_high=item['orb_high'],
                orb_low=item['orb_low'],
                orb_high_clean=item['orb_high'] + 2,
                orb_low_clean=item['orb_low'] - 2,
                orb_window=15
            )
            session.add(focus)
            print(f"Added {item['symbol']} as SNIPER.")

    try:
        session.commit()
        print("Bootstrap Complete. Watchlist is now live.")
    except Exception as e:
        print(f"Error during bootstrap: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    bootstrap_watchlist()
