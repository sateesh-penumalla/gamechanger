from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from src.db.schema import CatalystScan
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv()

db_url = os.getenv("DATABASE_URL")
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
session = Session()

try:
    today = date.today()
    scans = session.query(CatalystScan).filter(func.date(CatalystScan.timestamp) == today).all()
    
    print(f"Found {len(scans)} entries for today.")
    for s in scans:
        print(f"{s.symbol}|{s.scan_type}|{s.current_price}|{s.volume_multiplier}|{s.target_1}|{s.stop_loss}")
except Exception as e:
    print(e)
finally:
    session.close()
