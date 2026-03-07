import os
import sys
import pandas as pd
import yfinance as yf
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add root directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.db.schema import Ticker, init_db

def map_yfinance_to_sector(yf_info):
    """Maps yfinance sector/industry to BharatQuant standard sector names."""
    sector = yf_info.get('sector', '').strip()
    industry = yf_info.get('industry', '').strip()
    
    # Mapping Logic
    if "Bank" in industry or "Bank" in sector:
        return "NIFTY_BANK"
    if "Information Technology" in sector or "Software" in industry:
        return "NIFTY_IT"
    if "Auto" in sector or "Auto" in industry:
        return "NIFTY_AUTO"
    if "Pharmaceuticals" in industry or "Healthcare" in sector:
        return "NIFTY_PHARMA"
    if "Metals" in sector or "Mining" in industry or "Steel" in industry:
        return "NIFTY_METAL"
    if "FMCG" in sector or "Consumer Goods" in sector or "Beverages" in industry or "Food" in industry:
        return "NIFTY_FMCG"
    if "Energy" in sector or "Oil" in industry or "Power" in industry:
        return "NIFTY_ENERGY"
    if "Construction" in sector or "Infrastructure" in industry:
        return "NIFTY_INFRA"
    if "Real Estate" in sector:
        return "NIFTY_REALTY"
    if "Financial Services" in sector:
        return "NIFTY_FINANCIAL"
    
    # Fallback to a sanitized version of the sector
    if sector:
        return f"NIFTY_{sector.upper().replace(' ', '_')}"
    return "UNKNOWN"

def populate_sectors():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not found in .env")
        return

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 1. Fetch all tickers needing updates
        tickers = session.query(Ticker).filter(
            (Ticker.sector == "UNKNOWN") | (Ticker.sector == None) | (Ticker.sector == "")
        ).all()
        logger.info(f"Retrieved {len(tickers)} tickers needing sector updates.")

        updated_count = 0
        for ticker in tickers:
            symbol = ticker.symbol
            # Skip if already has a sector (optional, user might want to overwrite)
            # if ticker.sector and ticker.sector != "UNKNOWN":
            #     continue

            try:
                logger.info(f"Processing {symbol}...")
                yf_ticker = yf.Ticker(f"{symbol}.NS")
                info = yf_ticker.info
                
                if not info or 'sector' not in info:
                    # Try without .NS if it fails (indices or special cases)
                    yf_ticker = yf.Ticker(symbol)
                    info = yf_ticker.info

                if info and 'sector' in info:
                    sector_name = map_yfinance_to_sector(info)
                    ticker.sector = sector_name
                    updated_count += 1
                    logger.success(f"Updated {symbol} -> {sector_name}")
                else:
                    logger.warning(f"Could not find sector info for {symbol}")
                    ticker.sector = "UNKNOWN"

                # Small sleep to avoid rate limiting
                import time
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                ticker.sector = "ERROR"

        session.commit()
        logger.info(f"Successfully updated sectors for {updated_count} symbols.")

    except Exception as e:
        logger.error(f"Batch update failed: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    populate_sectors()
