import os
import sys
import time
from datetime import date
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

from src.db.schema import DailyFocus
from scripts.market_depth_simulator import MarketDepthSimulator

def get_focus_symbols():
    """Fetch symbols that were in focus today."""
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        today = date.today()
        # Query symbols explicitly tracked in daily_focus for today
        results = session.query(DailyFocus).filter(DailyFocus.date == today).all()
        return [r.symbol.replace(".NS", "").strip() for r in results]
    except Exception as e:
        logger.error(f"Error fetching focus symbols: {e}")
        return []
    finally:
        session.close()

def main():
    today_str = date.today().strftime("%Y-%m-%d")
    symbols = get_focus_symbols()
    
    if not symbols:
        logger.error("No focus symbols found for today. Nothing to replay.")
        return

    logger.info(f"Found {len(symbols)} symbols to replay for {today_str}")
    
    # We run at high speed (200x) since we just want to flush to DB/Redis 
    # and trigger signal generator
    speed = 200.0 
    
    for symbol in symbols:
        try:
            # Check if parquet file exists first to avoid simulator error
            p_path = f"data/historical_ticks/{symbol}/{today_str}.parquet"
            if not os.path.exists(p_path):
                logger.warning(f"Skipping {symbol} - No parquet file found for {today_str}")
                continue
                
            logger.info(f">>> Starting Replay for {symbol} ({speed}x) <<<")
            sim = MarketDepthSimulator(symbol=symbol, playback_date=today_str, speed=speed)
            sim.run()
            logger.success(f"Finished Replay for {symbol}")
            
            # Short sleep between symbols
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Error replaying {symbol}: {e}")

    logger.success("Batch replay completed.")

if __name__ == "__main__":
    main()
