from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root@localhost/bharatquant_mas")
engine = create_engine(db_url)

with engine.connect() as conn:
    print("Dropping tables...")
    for table in ["tickers", "portfolio", "realized_pnl", "trade_recommendations", "scan_logs"]:
        conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
        print(f"Dropped {table}")
    conn.commit()
    print("DB reset complete.")
