#!/usr/bin/env python3
"""
macOS Alert Bridge for GameChanger
Runs locally on your Mac to trigger native system alerts.
Bypasses browser autoplay restrictions.
"""

import json
import time
import subprocess
import urllib.request
from datetime import datetime

# --- CONFIGURATION ---
API_BASE_URL = "http://localhost:8000"
POLL_INTERVAL = 5  # Seconds
SOUND_NEW_SIGNAL = "Glass"
SOUND_NEAR_MISS = "Ping"

# State to track seen alerts
seen_signal_ids = set()
seen_near_miss_ids = set()

def send_macos_notification(title, message, sound="Glass"):
    """Trigger a native macOS banner with sound."""
    try:
        title = title.replace('"', '\\"')
        message = message.replace('"', '\\"')
        script = f'display notification "{message}" with title "{title}" sound name "{sound}"'
        subprocess.run(['osascript', '-e', script], check=True)
    except Exception as e:
        print(f"Error sending notification: {e}")

def poll_signals():
    global seen_signal_ids
    try:
        url = f"{API_BASE_URL}/dashboard/signals"
        with urllib.request.urlopen(url, timeout=5) as response:
            signals = json.loads(response.read().decode())
            
            # First run: seed the seen set so we don't alert for old signals
            if not seen_signal_ids and signals:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Initialized with {len(signals)} existing signals.")
                for s in signals:
                    seen_signal_ids.add(s['id'])
                return

            for sig in signals:
                if sig['id'] not in seen_signal_ids:
                    seen_signal_ids.add(sig['id'])
                    
                    # Logic for fresh signals only
                    emoji = "🟢" if sig.get('side') == "LONG" else "🔴"
                    title = f"{emoji} NEW SIGNAL: {sig['symbol']}"
                    message = f"{sig['side']} Entry @ {sig['entry_price']} | Tgt: {sig['tp']}"
                    
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ALERT: {title} - {message}")
                    send_macos_notification(title, message, sound=SOUND_NEW_SIGNAL)
                    
    except Exception as e:
        print(f"Signal Poll Error: {e}")

def poll_near_misses():
    global seen_near_miss_ids
    try:
        url = f"{API_BASE_URL}/dashboard/signals/nearmiss"
        with urllib.request.urlopen(url, timeout=5) as response:
            near_misses = json.loads(response.read().decode())
            
            if not seen_near_miss_ids and near_misses:
                for nm in near_misses:
                    seen_near_miss_ids.add(nm['id'])
                return

            for nm in near_misses:
                if nm['id'] not in seen_near_miss_ids:
                    seen_near_miss_ids.add(nm['id'])
                    
                    title = f"⚠️ Near Miss: {nm['symbol']}"
                    reasons = ", ".join(nm.get('fail_reasons', []))
                    message = f"Failed Filters: {reasons}"
                    
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] NEAR MISS: {title}")
                    send_macos_notification(title, message, sound=SOUND_NEAR_MISS)
                    
    except Exception as e:
        pass # Silently fail for near misses to avoid cluttering logs

def send_heartbeat():
    """Send a heartbeat to the backend."""
    try:
        url = f"{API_BASE_URL}/system/bridge/heartbeat"
        req = urllib.request.Request(url, method="POST")
        with urllib.request.urlopen(req, timeout=2) as response:
            pass
    except Exception:
        pass

def main():
    print("="*50)
    print("🚀 GameChanger macOS Alert Bridge Started")
    print(f"📡 Polling: {API_BASE_URL}")
    print(f"🕒 Interval: {POLL_INTERVAL}s")
    print("🔔 System alerts will now trigger for new signals.")
    print("="*50)
    
    while True:
        send_heartbeat()
        poll_signals()
        poll_near_misses()
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
