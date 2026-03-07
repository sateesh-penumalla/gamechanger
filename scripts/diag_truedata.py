from truedata_ws.websocket.TD import TD
import os
import time
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("TRUEDATA_USER_ID")
pwd = os.getenv("TRUEDATA_PASSWORD")
port = int(os.getenv("TRUEDATA_REALTIME_PORT", 8086))

print(f"Testing TrueData Connection with Correct API...")
print(f"User: {user}, Port: {port}")

def my_handler(tick):
    print(f"Tick Received: {tick}")

try:
    td = TD(user, pwd, live_port=port)
    print("TD Instance Created. Setting callbacks...")
    
    td.trade_callback(my_handler)
    td.bidask_callback(my_handler)
    
    # Try subscribing to a known liquid symbol
    symbols = ["RELIANCE", "SBIN", "ACC"]
    print(f"Subscribing to {symbols}...")
    td.start_live_data(symbols)
    
    print("Waiting 15 seconds for data...")
    start_time = time.time()
    while time.time() - start_time < 15:
        time.sleep(1)
        
    print("Done waiting.")
    td.disconnect()
    
except Exception as e:
    print(f"Error during TrueData Test: {e}")
