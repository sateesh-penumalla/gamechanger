from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root@localhost/bharatquant_mas")
engine = create_engine(db_url)

with engine.connect() as conn:
    print("Dropping all tables for a clean migration...")
    # List of tables to drop
    tables = ["trade_recommendations", "tickers", "portfolio", "realized_pnl", "scan_logs", "performance_logs"]
    for table in tables:
        try:
            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
            print(f"Dropped {table}")
        except Exception as e:
            print(f"Error dropping {table}: {e}")
    conn.commit()
    print("Clean migration ready.")
