import os
import sys
from dotenv import load_dotenv
from loguru import logger
from google import genai
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add current directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data.yfinance_client import YahooFinanceData
from src.data.dhan_client import DhanDataClient
from src.agents.scout import ScoutAgent
from src.agents.chartist import ChartistAgent
from src.agents.globalist import GlobalistAgent
from src.agents.newsroom import NewsroomAgent
from src.agents.librarian import LibrarianAgent
from src.agents.curator import CuratorAgent
from src.agents.oracle import OracleAgent
from src.agents.sector_general import SectorGeneralAgent # NEW
from src.agents.bear_hunter import BearHunterAgent # NEW
from src.agents.news_panic_agent import NewsPanicAgent # NEW
from src.agents.squeeze_hunter import SqueezeHunterAgent # NEW
from src.core.orchestrator import BharatQuantOrchestrator
from src.db.schema import init_db

# Load environment variables
load_dotenv()

def main():
    logger.info("Initializing BharatQuant MAS...")
    
    # 1. Setup Database
    db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root@localhost/bharatquant_mas")
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

    oracle = OracleAgent(data_client, session)
    scout = ScoutAgent(data_client, session)
    chartist = ChartistAgent()
    globalist = GlobalistAgent(data_client)
    newsroom = NewsroomAgent(gemini_client)
    librarian = LibrarianAgent(session)
    sector_general = SectorGeneralAgent(data_client) # NEW
    bear_hunter = BearHunterAgent(data_client) # NEW
    news_panic = NewsPanicAgent(gemini_client) # NEW
    squeeze_hunter = SqueezeHunterAgent(data_client) # NEW
    
    # NEW: Curator for dynamic watchlist
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'watchlist.json')
    curator = CuratorAgent(config_path)

    # 3. Initialize Orchestrator
    orchestrator = BharatQuantOrchestrator(
        scout, chartist, globalist, newsroom, librarian, 
        oracle=oracle, sector_general=sector_general,
        bear_hunter=bear_hunter, news_panic=news_panic,
        squeeze_hunter=squeeze_hunter # NEW
    )
    
    # 4. Get Dynamic Watchlist
    symbols = curator.get_watchlist()
    logger.info(f"Starting analysis for dynamic watchlist: {symbols}")
    
    # 5. Warm up (Oracle Sync)
    orchestrator.warm_up(symbols)
    
    results = orchestrator.run(symbols)
    
    print("\n" + "!"*60)
    print("FINAL RECOMMENDATIONS FROM BHARATQUANT WAR ROOM")
    print("!"*60)
    
    if not results["recommendations"]:
        print("No high-conviction trades found at this time.")
    else:
        for rec in results["recommendations"]:
            print(f"SYMBOL: {rec['symbol']} | ACTION: {rec['action']} | CONFIDENCE: {rec['confidence']}%")
    
    print("!"*60 + "\n")

    logger.info("BharatQuant MAS session complete.")

if __name__ == "__main__":
    main()
