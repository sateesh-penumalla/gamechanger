import os
import sys
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.yfinance_client import YahooFinanceData
from src.data.dhan_client import DhanDataClient
from src.agents.catalyst_hunter import CatalystHunterAgent
from src.agents.newsroom import NewsroomAgent
from src.agents.librarian import LibrarianAgent
from src.db.schema import init_db, CatalystScan, get_ist_now

def run_daily_catalyst_scan():
    load_dotenv()
    
    # 1. Setup Data Client
    source = os.getenv("DATA_SOURCE", "yahoo").lower()
    if source == "dhan":
        cid = os.getenv("DHAN_CLIENT_ID")
        token = os.getenv("DHAN_ACCESS_TOKEN")
        data_client = DhanDataClient(cid, token)
    else:
        data_client = YahooFinanceData()

    # 2. Setup Database & Agents
    db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root@localhost/bharatquant_sniper")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    newsroom = NewsroomAgent()
    hunter = CatalystHunterAgent(data_client, newsroom)
    librarian = LibrarianAgent(session)

    # 3. Load Scrip List
    csv_path = "sec_list.csv"
    if not os.path.exists(csv_path):
        logger.error(f"Scrip list {csv_path} not found!")
        return

    symbols = pd.read_csv(csv_path)['Symbol'].tolist()
    logger.info(f"Starting Breakout Catalyst Scan for {len(symbols)} stocks...")

    # 4. Run Scan
    candidates = hunter.find_breakout_candidates(symbols)

    # 5. Output Results & Save to DB
    print("\n" + "="*80)
    print(f" 🚀 BHARATQUANT BREAKOUT CATALYST SCANNER (Total Scanned: {len(symbols)})")
    print("="*80)
    
    if not candidates:
        print(" [NO BREAKOUTS DETECTED] No stocks met the multi-year explosive criteria today.")
    else:
        # Sort so hits come first
        candidates.sort(key=lambda x: x['type'], reverse=True)
        
        for c in candidates:
            status_emoji = "🔥 [HIT]" if c['type'] == "CATALYST_HIT" else "⚠️ [NEAR]"
            print(f"\n{status_emoji} STOCK: {c['symbol']} (Current Price: ₹{c['price']})")
            print(f"TRG 1: ₹{c['target_1']} | TRG 2: ₹{c['target_2']} | TRG 3: ₹{c['target_3']} (Wait: {c['hold_period_days']} days)")
            print(f"VOLUME SURGE: {c['multiplier']}x | SENTIMENT: {c['sentiment']} ({c['sentiment_score']}/100)")
            print(f"STOP LOSS: ₹{c['sl']}")
            if c['reason'] != "Default":
                print(f"CATALYST: {c['reason']}")
            
            # --- 1. NEW: SAVE TO CATALYST_SCANS TABLE (For analysis) ---
            scan_entry = CatalystScan(
                symbol=c["symbol"],
                scan_type=c["type"],
                current_price=c["price"],
                volume_multiplier=c["multiplier"],
                dist_to_high_pct=c.get("dist_to_high_pct", 0), # Corrected key name
                sentiment_score=c["sentiment_score"],
                target_1=c["target_1"],
                target_2=c["target_2"],
                target_3=c["target_3"],
                stop_loss=c["sl"]
            )
            session.add(scan_entry)
            session.commit()
            print(f" ✅ Saved to Catalyst History table.")

            # --- 2. EXISTING: RECORD RECOMMENDATION (Only for HITs) ---
            if c['type'] == "CATALYST_HIT":
                rec_data = {
                    "symbol": c["symbol"],
                    "signal_type": "BREAKUP",
                    "entry_price": c["price"],
                    "stop_loss": c["sl"],
                    "target_1": c["target_1"],
                    "target_2": c["target_2"],
                    "target_3": c["target_3"],
                    "hold_type": "SWING",
                    "confidence_score": c["sentiment_score"],
                    "agent_votes": {"catalyst_hunter": "HIT", "news_sentiment": c["sentiment"]}
                }
                librarian.record_recommendation(rec_data)
                print(" ✅ Recorded as SWING Alpha Recommendation.")
            else:
                print(" ℹ️ Watchlist Candidate (Volume Surge, no breakout price yet).")
            print("-" * 40)

    print("\n" + "="*80)

if __name__ == "__main__":
    run_daily_catalyst_scan()
