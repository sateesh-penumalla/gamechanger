import os
import sys
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.dhan_client import DhanDataClient

def log_cub_pulse():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    data_client = DhanDataClient(cid, token)
    
    symbol = "CUB"
    data_1m = data_client.fetch_realtime_data(symbol, period="1d", interval="1m")
    
    if data_1m is not None:
        # data_1m index is likely UTC. 14:20 IST = 08:50 UTC.
        # Let's just look at the last 60 mins.
        print("\n--- CUB 1M PULSE (Last 30 mins) ---")
        tail = data_1m.tail(30).copy()
        tail['IST'] = tail.index + pd.Timedelta(hours=5, minutes=30)
        print(tail[['IST', 'Open', 'High', 'Low', 'Close', 'Volume']].to_string())

    # Sector check
    bn = data_client.fetch_realtime_data("BANKNIFTY", period="1d", interval="1m")
    if bn is not None:
        print("\n--- BANKNIFTY PULSE (Last 5 mins) ---")
        tail_bn = bn.tail(5).copy()
        tail_bn['IST'] = tail_bn.index + pd.Timedelta(hours=5, minutes=30)
        print(tail_bn[['IST', 'Open', 'High', 'Low', 'Close', 'Volume']].to_string())

if __name__ == "__main__":
    log_cub_pulse()
