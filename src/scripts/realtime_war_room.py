import os
import sys
import time
import pandas as pd
from loguru import logger
from google import genai
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO")

# Add root directory to path
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.yfinance_client import YahooFinanceData
from src.data.dhan_client import DhanDataClient
from src.agents.scout import ScoutAgent
from src.agents.chartist import ChartistAgent
from src.agents.globalist import GlobalistAgent
from src.agents.newsroom import NewsroomAgent
from src.agents.librarian import LibrarianAgent
from src.agents.oracle import OracleAgent
from src.agents.sector_general import SectorGeneralAgent
from src.agents.sniper_monitor import SniperMonitorAgent
from src.agents.bear_hunter import BearHunterAgent # NEW
from src.agents.news_panic_agent import NewsPanicAgent # NEW
from src.agents.squeeze_hunter import SqueezeHunterAgent # NEW
from src.core.orchestrator import BharatQuantOrchestrator
from src.db.schema import init_db, Portfolio, Ticker, get_ist_now

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def render_dashboard(data: dict):
    clear_screen()
    print("=" * 80)
    print(" BHARATQUANT REAL-TIME WAR ROOM | SNIPER MODE ON ")
    print("=" * 80)
    print(f" TIME: {get_ist_now().strftime('%H:%M:%S')} | FREE CASH: ₹{data['free_cash']:,.2f} | DAILY PNL: ₹{data['realized_profit']:,.2f}")
    print(f" TRADES COMPLETED: {data['trade_count']} | ACTIVE SLOTS: {len(data['active_trades'])}/5")
    print("-" * 80)
    
    if not data['active_trades']:
        print(" [PORTFOLIO EMPTY] Waiting for high-conviction Snipes...")
    else:
        print(f"{'SYMBOL':<12} {'INVESTED':<12} {'PnL %':<10} {'STATUS'}")
        for t in data['active_trades']:
            color = "\033[92m" if t['pnl_pct'] > 0 else "\033[91m"
            reset = "\033[0m"
            print(f"{t['symbol']:<12} ₹{t['invested']:<12,.2f} {color}{t['pnl_pct']:>8.2f}%{reset}   {t['status']}")
            
    print("-" * 80)
    print(" [LOGS]")

def run_realtime_war_room():
    # 1. Setup Database
    db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root@localhost/bharatquant_sniper")
    engine = create_engine(db_url)
    init_db(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # 2. Initialize Data Client based on preference
    source = os.getenv("DATA_SOURCE", "yahoo").lower()
    if source == "dhan":
        cid = os.getenv("DHAN_CLIENT_ID")
        token = os.getenv("DHAN_ACCESS_TOKEN")
        if not cid or not token or "YOUR_" in cid:
            logger.error("Dhan credentials missing in .env! Falling back to Yahoo Finance.")
            data_client = YahooFinanceData()
        else:
            data_client = DhanDataClient(cid, token)
    else:
        data_client = YahooFinanceData()

    # Initialize Gemini Client Once
    api_key = os.getenv("GOOGLE_API_KEY")
    gemini_client = genai.Client(api_key=api_key) if api_key else None

    # 3. Initialize Agents
    scout = ScoutAgent(data_client, session)
    chartist = ChartistAgent()
    globalist = GlobalistAgent(data_client)
    newsroom = NewsroomAgent(gemini_client)
    librarian = LibrarianAgent(session)
    oracle = OracleAgent(data_client, session)
    sector_general = SectorGeneralAgent(data_client)
    bear_hunter = BearHunterAgent(data_client) # NEW
    news_panic = NewsPanicAgent(gemini_client) # NEW
    squeeze_hunter = SqueezeHunterAgent(data_client) # NEW
    monitor = SniperMonitorAgent(data_client, session)
    
    orchestrator = BharatQuantOrchestrator(
        scout, chartist, globalist, newsroom, librarian, 
        oracle, sector_general, bear_hunter, news_panic, squeeze_hunter # NEW
    )

    # 3. Load Elite List
    csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'sec_list.csv')
    symbols = pd.read_csv(csv_path)['Symbol'].tolist()
    
    logger.info("War Room Active. Starting 1-minute Scan Pulse...")
    
    # NEW: Warm-up Oracle for the first time
    orchestrator.warm_up(symbols)
    
    last_scan_time = 0
    scan_interval = 30 # High Frequency Pulse (30 Seconds)

    try:
        while True:
            current_time = time.time()
            
            # --- NODE 1: PORTFOLIO MONITOR (Every 20 Seconds for high precision) ---
            monitor_actions = monitor.monitor_portfolio()
            for action in monitor_actions:
                logger.info(f"Monitor Trigger: {action['action']} on {action['symbol']} at ₹{action['price']}")

            # --- NODE 2: SCAN PULSE (Every 1 Minute) ---
            if current_time - last_scan_time >= scan_interval:
                # Optimization: Only scan stocks that Oracle flagged as SNIPER
                snipers = session.query(Ticker).filter(Ticker.oracle_status.in_(['UP_SNIPER', 'DOWN_SNIPER'])).all()
                sniper_symbols = [t.symbol for t in snipers]
                
                logger.info(f"PULSE: Running technical scan on {len(sniper_symbols)} SNIPER candidates...")
                
                # Check how many slots we have
                active_count = session.query(Portfolio).count()
                if active_count < 5 and sniper_symbols:
                    result = orchestrator.run(sniper_symbols)
                    recs = result.get("recommendations", [])
                    
                    for rec in recs:
                        # Re-check count inside loop (recs could be many)
                        if session.query(Portfolio).count() >= 5:
                            break
                        
                        # TRIGGER LOGIC: Only enter if the market has actually hit the trigger price
                        # Fetch the absolute latest price to verify trigger
                        latest_data = data_client.fetch_realtime_data(rec['symbol'], period="1d", interval="1m")
                        if latest_data is None or latest_data.empty:
                            continue
                        
                        current_price = latest_data.iloc[-1]['Close']
                        high_price = latest_data.iloc[-1]['High']
                        low_price = latest_data.iloc[-1]['Low']
                        
                        is_triggered = False
                        sig = rec.get("signal_type", "BREAKUP")
                        
                        if sig == "BREAKUP":
                            if high_price >= rec["price"]:
                                is_triggered = True
                        elif sig == "PANIC_SELL": # NEW
                            if low_price <= rec["price"]:
                                is_triggered = True
                        elif sig == "REBOUND": # NEW: Long entry on capitulation
                            if high_price >= rec["price"]:
                                is_triggered = True
                        elif sig == "BREAKDOWN":
                            if low_price <= rec["price"]:
                                is_triggered = True
                        else:
                            is_triggered = True # Market entry for reversions
                            
                        if not is_triggered:
                            logger.info(f"⏳ PENDING: {rec['symbol']} setup found but trigger level ₹{rec['price']} not hit yet. (High: ₹{high_price})")
                            continue

                        # Simulate Auto-Entry for the "Cash Flow Sniper" logic
                        portfolio_data = {
                            "symbol": rec["symbol"],
                            "qty": rec["qty"],
                            "avg_price": rec["price"], 
                            "invested_amount": rec["allocation"],
                            "target_price": rec["price"] * 0.99 if rec["action"] == "SELL" else rec["price"] * 1.01,
                            "stop_loss": rec["sl"],
                            "hold_type": "INTRADAY"
                        }
                        librarian.add_to_portfolio(portfolio_data)
                        logger.info(f"🎯 TRIGGER HIT: Executed {rec['symbol']} at ₹{rec['price']}")
                        # Trigger macOS alert sound
                        os.system("afplay /System/Library/Sounds/Glass.aiff &")
                
                last_scan_time = current_time

            # --- NODE 3: RENDER DASHBOARD ---
            dash_data = monitor.get_dashboard_data()
            render_dashboard(dash_data)
            
            time.sleep(10) # Refresh Dashboard every 10s
    except KeyboardInterrupt:
        logger.info("War Room closed by user.")

if __name__ == "__main__":
    run_realtime_war_room()
