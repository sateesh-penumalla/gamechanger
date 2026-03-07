import websocket
import threading
import time
import os
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("TRUEDATA_USER_ID")
pwd = os.getenv("TRUEDATA_PASSWORD")
port = int(os.getenv("TRUEDATA_REALTIME_PORT", 8086))
url = f"wss://push.truedata.in:{port}/?user={user}&password={pwd}"

print(f"Connecting to raw WebSocket: {url}")

def on_message(ws, message):
    print(f"RAW MSG: {message}")

def on_error(ws, error):
    print(f"RAW ERR: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"RAW CLOSE: {close_status_code} - {close_msg}")

def on_open(ws):
    print("RAW OPEN SUCCESS")
    # Try sending a heartbeat or subscription later
    
ws_app = websocket.WebSocketApp(url,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)

wst = threading.Thread(target=ws_app.run_forever)
wst.daemon = True
wst.start()

print("Waiting 30 seconds for messages...")
time.sleep(30)
ws_app.close()
print("Test Complete.")
