
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL')
engine = create_engine(db_url)

print(f"Connecting to: {db_url}")

with engine.connect() as conn:
    # 1. Check schema of history_testing
    print("\n--- Schema Check ---")
    res = conn.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'history_testing'"))
    for r in res:
        print(r)

    # 2. Check for AVANTIFEED records
    print("\n--- Recent Records for AVANTIFEED ---")
    query = text("SELECT trade_date, oracle_status FROM history_testing WHERE symbol = 'AVANTIFEED' ORDER BY trade_date DESC LIMIT 10")
    results = conn.execute(query).fetchall()
    for r in results:
        print(f"Date: {r[0]} | Status: {r[1]} | Type: {type(r[0])}")

    # 3. Test specific date match
    test_date = "2026-02-12"
    print(f"\n--- Test Match for {test_date} ---")
    query = text("SELECT symbol FROM history_testing WHERE symbol = 'AVANTIFEED' AND trade_date = :d")
    res = conn.execute(query, {"d": test_date}).fetchone()
    print(f"Match result (string): {res}")
    
    # 4. Try with cast or date object if needed
    from datetime import datetime
    dt_obj = datetime.strptime(test_date, "%Y-%m-%d").date()
    res = conn.execute(query, {"d": dt_obj}).fetchone()
    print(f"Match result (date object): {res}")
