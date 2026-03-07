import websocket
import threading
import time
import os
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("TRUEDATA_USER_ID")
pwd = os.getenv("TRUEDATA_PASSWORD")

def test_url(url):
    print(f"\n--- Testing: {url} ---")
    results = {"success": False, "msg": None}
    
    def on_message(ws, message):
        print(f"  [MSG] {message}")
        results["success"] = True
        results["msg"] = message
        ws.close()

    def on_error(ws, error):
        print(f"  [ERR] {error}")

    def on_open(ws):
        print("  [OPEN] Success")
        
    ws_app = websocket.WebSocketApp(url,
                                  on_open=on_open,
                                  on_message=on_message,
                                  on_error=on_error)
    
    wst = threading.Thread(target=ws_app.run_forever)
    wst.daemon = True
    wst.start()
    
    time.sleep(5)
    ws_app.close()
    return results

# Test Matrix
ports = [8082, 8084, 8086]
protocols = ["wss", "ws"]

for port in ports:
    for proto in protocols:
        url = f"{proto}://push.truedata.in:{port}/?user={user}&password={pwd}"
        test_url(url)

print("\nDiagnostics Complete.")
