import time
import requests
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any, Callable
import threading
from loguru import logger
import os
import ssl
import websocket

# TrueData WebSocket Library
try:
    from truedata_ws.websocket.TD import TD
except ImportError:
    TD = None
    logger.warning("truedata-ws library not found. WebSocket features will be disabled. Run: pip install truedata_ws")

class TrueDataClient:
    """
    TrueData API Client for Real-Time and Historical Data.
    Authenticates via REST and connects via WebSocket.
    """
    
    BASE_URL_AUTH = "https://auth.truedata.in"
    BASE_URL_HISTORY = "https://history.truedata.in"
    BASE_URL_API = "https://api.truedata.in"
    
    def __init__(self, username: str = None, password: str = None):
        self.username = username or os.getenv("TRUEDATA_USER_ID")
        self.password = password or os.getenv("TRUEDATA_PASSWORD")
        self.access_token = None
        self.td_app = None # WebSocket Instance
        self.callbacks = [] # List of functions to call on tick
        
        if not self.username or not self.password:
            logger.warning("TrueData Credentials missing. Please set TRUEDATA_USER_ID and TRUEDATA_PASSWORD.")

        # Monkey Patch for SSL Certification bypass on macOS
        self._patch_ssl()

    def _patch_ssl(self):
        """Force websocket-client to bypass certificate verification."""
        try:
            if not hasattr(websocket.WebSocketApp, "_original_run_forever"):
                logger.info("Applying TrueData SSL Monkey Patch...")
                websocket.WebSocketApp._original_run_forever = websocket.WebSocketApp.run_forever
                
                def patched_run_forever(ws_self, *args, **kwargs):
                    if "sslopt" not in kwargs:
                        kwargs["sslopt"] = {"cert_reqs": ssl.CERT_NONE}
                    return ws_self._original_run_forever(*args, **kwargs)
                
                websocket.WebSocketApp.run_forever = patched_run_forever
        except Exception as e:
            logger.error(f"Failed to apply SSL patch: {e}")

    def login(self) -> bool:
        """Authenticate and get Bearer Token."""
        if not self.username or not self.password:
            return False
            
        try:
            url = f"{self.BASE_URL_AUTH}/token"
            payload = {
                "username": self.username,
                "password": self.password,
                "grant_type": "password"
            }
            # Headers as per Postman Collection
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            
            response = requests.post(url, data=payload, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get("access_token")
                logger.info(f"TrueData Login Successful. Token expires in {data.get('expires_in')}s")
                return True
            else:
                logger.error(f"TrueData Login Failed: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"TrueData Auth Exception: {e}")
            return False

    def start_websocket(self, symbols: List[str], on_tick: Callable[[Any], None] = None):
        """Start the TrueData WebSocket connection."""
        if not TD:
            logger.error("Cannot start WebSocket: library missing.")
            return

        if not self.username or not self.password:
            logger.error("Cannot start WebSocket: Credentials missing.")
            return
        
        try:
            # 1. Initialize
            port = int(os.getenv("TRUEDATA_REALTIME_PORT", 8082))
            logger.info(f"Connecting to TrueData on port {port}...")
            self.td_app = TD(self.username, self.password, live_port=port, full_feed=True)
            
            # 2. Add Callbacks
            if on_tick:
                # Use official callback setters
                self.td_app.trade_callback(on_tick)
                self.td_app.full_feed_trade_callback(on_tick)
                self.td_app.bidask_callback(on_tick)

            # 3. Start Connection & Subscribe
            logger.info(f"Subscribing to {len(symbols)} symbols...")
            # Official method to subscribe to symbols
            self.td_app.start_live_data(symbols)
            
            logger.info("TrueData WebSocket Subscribed.")
            return self.td_app

        except Exception as e:
            logger.error(f"TrueData WebSocket Error: {e}")
            return None
    def fetch_intraday_bars(self, symbol: str, start_time: datetime, end_time: datetime = None, interval: str = "1min") -> Optional[pd.DataFrame]:
        """
        Fetch intraday bars (OHLCV) via REST API.
        Endpoint: https://history.truedata.in/getbars
        """
        if not self.access_token:
            if not self.login():
                return None
                
        end_time = end_time or datetime.now()
        
        # TrueData format: YYMMDDTHH:MM:SS
        fmt = "%y%m%dT%H:%M:%S"
        
        params = {
            "symbol": symbol,
            "from": start_time.strftime(fmt),
            "to": end_time.strftime(fmt),
            "interval": interval,
            "response": "csv"
        }
        
        try:
            url = f"{self.BASE_URL_HISTORY}/getbars"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(url, params=params, headers=headers)
            
            if response.status_code != 200:
                logger.error(f"TrueData getBars failed: {response.status_code} - {response.text}")
                return None
                
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            # Standardize column names (TrueData sometimes returns lowercase)
            # Map 'timestamp' or 'Time' to 'Time'
            df.columns = ['Time' if c.lower() in ['time', 'timestamp'] else c.capitalize() for c in df.columns]
            
            if "Time" in df.columns:
                # TrueData format can be YYMMDDTHH:MM:SS or slightly different
                df['Datetime'] = pd.to_datetime(df['Time'])
                df.set_index('Datetime', inplace=True)
                df.drop(columns=['Time'], inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"TrueData getBars Exception: {e}")
            return None

    def fetch_latest_ticks(self, symbol: str, n_ticks: int = 1000, bidask: bool = True) -> Optional[pd.DataFrame]:
        """
        Fetch latest ticks with Best Bid/Ask.
        Endpoint: https://history.truedata.in/getlastnticks
        This can be used to reconstruct order flow historically.
        """
        if not self.access_token:
            if not self.login():
                return None
                
        params = {
            "symbol": symbol,
            "nticks": n_ticks,
            "bidask": 1 if bidask else 0,
            "response": "csv",
            "interval": "tick"
        }
        
        try:
            url = f"{self.BASE_URL_HISTORY}/getlastnticks"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(url, params=params, headers=headers)
            
            if response.status_code != 200:
                logger.error(f"TrueData getLastNTicks failed: {response.text}")
                return None
                
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            # Headers: Time, Last, Qty, Bid, BidQty, Ask, AskQty
            if "Time" in df.columns:
                df['Datetime'] = pd.to_datetime(df['Time'], format="%Y%m%dT%H:%M:%S.%f") # Ticks have ms
                df.set_index('Datetime', inplace=True)
                df.drop(columns=['Time'], inplace=True)
                
            return df
            
        except Exception as e:
            logger.error(f"TrueData getLastNTicks Exception: {e}")
            return None

    def fetch_ticks_range(self, symbol: str, start_time: datetime, end_time: datetime, bidask: bool = True) -> Optional[pd.DataFrame]:
        """
        Fetch ticks for a specific range via REST API.
        Endpoint: https://history.truedata.in/getticks
        """
        if not self.access_token:
            if not self.login():
                return None

        # TrueData format: YYMMDDTHH:MM:SS
        fmt = "%y%m%dT%H:%M:%S"
        
        params = {
            "symbol": symbol,
            "from": start_time.strftime(fmt),
            "to": end_time.strftime(fmt),
            "bidask": 1 if bidask else 0,
            "response": "csv",
            "comp": "false"
        }
        
        try:
            url = f"{self.BASE_URL_HISTORY}/getticks"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(url, params=params, headers=headers)
            
            if response.status_code != 200:
                error_msg = response.text
                if "quota exceeded" in error_msg.lower():
                    logger.critical("TrueData API Limit Reached: Quota Exceeded!")
                logger.error(f"TrueData getTicks failed: {error_msg}")
                return None
                
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            # Cleaning columns
            if "Time" in df.columns:
                df['Datetime'] = pd.to_datetime(df['Time'], format="%Y%m%dT%H:%M:%S.%f")
                df.set_index('Datetime', inplace=True)
                df.drop(columns=['Time'], inplace=True)
                
            return df
            
        except Exception as e:
            logger.error(f"TrueData getTicks Exception: {e}")
            return None

    def get_market_status(self):
        """Check if API is reachable."""
        return self.login()
