import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add current directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.orchestrator import BharatQuantOrchestrator
from src.agents.librarian import LibrarianAgent

# Mock Agents
class MockAgent:
    def __init__(self, score, conviction=50):
        self.score = score
        self.conviction = conviction
    def scan_for_setups(self, symbols):
        return [{"symbol": "MOCKSTOCK", "score": self.score, "price": 1000.0, "signals": ["Mock Signal"]}]
    def analyze_market_mood(self):
        return {"mood": "Bullish", "conviction": self.conviction, "details": ["Mock Mood"]}
    def analyze_sentiment(self, symbol):
        return {"sentiment": "Bullish", "score": 80, "highlights": ["Mock News"]}

def test_recording():
    db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root@localhost/bharatquant_mas")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    librarian = LibrarianAgent(session)
    # High score mock: Scout=90, Mood=80, News=80 -> Avg = 83.33
    orchestrator = BharatQuantOrchestrator(
        scout=MockAgent(90, 80), 
        chartist=None, 
        globalist=MockAgent(90, 80), 
        newsroom=MockAgent(90, 80), 
        librarian=librarian
    )
    
    print("Running orchestrator with MOCK high-confidence data...")
    results = orchestrator.run(["MOCKSTOCK"])
    
    if results["recommendations"]:
        print(f"SUCCESS: Found {len(results['recommendations'])} recommendations.")
        for rec in results["recommendations"]:
            print(f"Recorded: {rec['symbol']} with confidence {rec['confidence']}%")
    else:
        print("FAILURE: No recommendations found even with high scores.")

if __name__ == "__main__":
    test_recording()
