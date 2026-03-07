import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv("DATABASE_URL")
if not db_url:
    print("DATABASE_URL not found in .env")
    exit(1)

engine = create_engine(db_url)

commands = [
    "ALTER TABLE intraday_ticks ADD COLUMN iceberg_timestamp DATETIME NULL AFTER iceberg_score;",
    "ALTER TABLE intraday_ticks ADD COLUMN iceberg_side VARCHAR(10) NULL AFTER iceberg_timestamp;"
]

with engine.connect() as conn:
    for cmd in commands:
        try:
            print(f"Executing: {cmd}")
            conn.execute(text(cmd))
            conn.commit()
            print("Success.")
        except Exception as e:
            if "Duplicate column name" in str(e):
                print("Column already exists. Skipping.")
            else:
                print(f"Error: {e}")

print("Migration complete.")
