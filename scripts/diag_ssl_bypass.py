import websocket
import ssl
import threading
import time
import os
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("TRUEDATA_USER_ID")
pwd = os.getenv("TRUEDATA_PASSWORD")
port = 8086
url = f"wss://push.truedata.in:{port}/?user={user}&password={pwd}"

print(f"Connecting to raw WebSocket (SSL Bypass): {url}")

def on_message(ws, message):
    print(f"RAW MSG: {message}")

def on_error(ws, error):
    print(f"RAW ERR: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"RAW CLOSE: {close_status_code} - {close_msg}")

def on_open(ws):
    print("RAW OPEN SUCCESS")
    # Send subscription for a known symbol
    # The protocol usually expects a JSON subscription
    sub_msg = '{"method": "addsymbol", "symbols": ["RELIANCE", "SBIN"]}'
    ws.send(sub_msg)
    print(f"Sent: {sub_msg}")

ws_app = websocket.WebSocketApp(url,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)

# Run with SSL bypass
wst = threading.Thread(target=ws_app.run_forever, kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}})
wst.daemon = True
wst.start()

print("Waiting 20 seconds for messages...")
time.sleep(20)
ws_app.close()
print("Test Complete.")
