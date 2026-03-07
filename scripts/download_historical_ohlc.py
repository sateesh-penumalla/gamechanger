import os
import sys
import argparse
import time
from datetime import datetime, timedelta
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import insert as mysql_insert
from dotenv import load_dotenv
import pandas as pd

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

from src.data.dhan_client import DhanDataClient
from src.db.schema import Ticker, HistoryDailyOHLC

def get_symbols_from_db():
    """Fetches all symbols from the tickers table."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set in environment.")
        return []
    
    try:
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        tickers = session.query(Ticker).all()
        # Clean symbols to match TrueData expected format (NSE symbols)
        symbols = [t.symbol.replace(".NS", "").replace("NSE:", "").strip() for t in tickers]
        session.close()
        return sorted(list(set(symbols)))
    except Exception as e:
        logger.error(f"Error fetching symbols from DB: {e}")
        return []

def upsert_daily_ohlc(session, symbol, df):
    """Upserts daily OHLC data into the history_daily_ohlc table."""
    if df is None or df.empty:
        return
    
    rows = []
    for ts, row in df.iterrows():
        # datetime index
        clean_ts = ts.replace(hour=0, minute=0, second=0, microsecond=0)
        rows.append({
            "symbol": symbol,
            "timestamp": clean_ts,
            "open": float(row['Open']),
            "high": float(row['High']),
            "low": float(row['Low']),
            "close": float(row['Close']),
            "volume": float(row['Volume']),
            "oi": float(row.get('OI', 0.0)),
            "source": "DHAN"
        })
    
    if not rows:
        return

    try:
        stmt = mysql_insert(HistoryDailyOHLC).values(rows)
        update_dict = {
            c.name: getattr(stmt.inserted, c.name)
            for c in HistoryDailyOHLC.__table__.columns
            if not c.primary_key and c.name != 'last_updated'
        }
        upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
        session.execute(upsert_stmt)
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Error upserting OHLC for {symbol}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Download historical daily OHLC data for symbols in DB.")
    parser.add_argument("--months", type=int, default=6, help="Number of months of history to download (default 6).")
    parser.add_argument("--days", type=int, help="Number of days of history to download (overrides --months).")
    parser.add_argument("--symbol", type=str, help="Specific symbol to download (optional).")
    
    args = parser.parse_args()
    
    # 1. Setup client and DB
    client = DhanDataClient()
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    # 2. Get symbols
    if args.symbol:
        symbols = [args.symbol]
    else:
        symbols = get_symbols_from_db()

    if not symbols:
        logger.error("No symbols found to download.")
        return

    # 3. Calculate range
    end_date = datetime.now()
    if args.days:
        start_date = end_date - timedelta(days=args.days)
    else:
        start_date = end_date - timedelta(days=args.months * 30)
    
    from_date_str = start_date.strftime("%Y-%m-%d")
    to_date_str = end_date.strftime("%Y-%m-%d")

    logger.info(f"Starting historical OHLC download for {len(symbols)} symbols from {from_date_str} to {to_date_str}...")

    for symbol in symbols:
        try:
            logger.info(f"Fetching daily OHLC for {symbol} from Dhan charts...")
            
            # Resolve security ID and segment
            sec_id = client.get_security_id(symbol)
            meta = client._meta_map.get(symbol.replace(".NS", "").strip())
            
            if not sec_id or not meta:
                logger.warning(f"Could not resolve Dhan ID for {symbol}")
                continue
                
            segment = meta['segment']
            instrument = meta['instr']

            # 1. Fetch Cash OHLC (Loop for deep history)
            current_start = start_date
            all_dfs = []
            
            while current_start < end_date:
                current_end = min(current_start + timedelta(days=90), end_date)
                
                f_str = current_start.strftime("%Y-%m-%d")
                t_str = current_end.strftime("%Y-%m-%d")
                
                batch_df = client.get_historical_ohlc(
                    security_id=str(sec_id),
                    exchange_segment=segment,
                    from_date=f_str,
                    to_date=t_str,
                    instrument=instrument,
                    oi=True
                )
                
                if batch_df is not None and not batch_df.empty:
                    all_dfs.append(batch_df)
                
                current_start = current_end + timedelta(days=1)
                
            if not all_dfs:
                logger.warning(f"No data returned for {symbol}")
                continue
                
            df = pd.concat(all_dfs).drop_duplicates()
            logger.info(f"Total bars fetched for {symbol}: {len(df)}")
            
            # 2. Fetch Futures OI (if FNO exists for this symbol)
            fut_id = client.get_active_futures_id(symbol)
            if fut_id and df is not None and not df.empty:
                logger.info(f"Fetching OI from futures contract for {symbol} in windows...")
                fut_segment = "NSE_FNO"
                fut_instrument = "FUTIDX" if instrument == "INDEX" else "FUTSTK"
                
                # Fetch Futures in windows same as cash
                curr_fut_start = start_date
                all_fut_dfs = []
                while curr_fut_start < end_date:
                    curr_fut_end = min(curr_fut_start + timedelta(days=90), end_date)
                    batch_fut_df = client.get_historical_ohlc(
                        security_id=str(fut_id),
                        exchange_segment=fut_segment,
                        from_date=curr_fut_start.strftime("%Y-%m-%d"),
                        to_date=curr_fut_end.strftime("%Y-%m-%d"),
                        instrument=fut_instrument,
                        oi=True
                    )
                    if batch_fut_df is not None and not batch_fut_df.empty:
                        all_fut_dfs.append(batch_fut_df)
                    curr_fut_start = curr_fut_end + timedelta(days=1)
                
                if all_fut_dfs:
                    df_fut = pd.concat(all_fut_dfs).drop_duplicates()
                    # Align OI to cash df using timestamp index
                    # DhanDataClient returns 'OI' as a column
                    if 'OI' in df_fut.columns:
                        df['OI'] = df_fut['OI']
                        df['OI'] = df['OI'].fillna(0.0)
            
            # Final cleansing
            df = df.fillna(0.0)
            
            if df is not None and not df.empty:
                upsert_daily_ohlc(session, symbol, df)
                logger.success(f"Saved {len(df)} daily bars for {symbol} (including OI)")
            else:
                logger.warning(f"No data found for {symbol}")
            
            # Rate limiting managed by DhanDataClient limiter
            
        except Exception as e:
            logger.error(f"Failed to process {symbol}: {e}")

    session.close()
    logger.success("Historical OHLC download completed.")

if __name__ == "__main__":
    main()
