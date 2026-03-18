
import asyncio
import websockets
import json
import struct
import os
from loguru import logger
from datetime import datetime
from typing import Dict, List, Callable, Optional

from collections import defaultdict

class DhanFeedClient:
    """
    Dhan Market Depth Binary WebSocket Client.
    Supports:
    - Standard Feed: wss://api-feed.dhan.co (LTP, Volume, 5-Level Depth)
    - Deep Feed: wss://depth-api-feed.dhan.co/twentydepth (20-Level Depth)
    """
    
    def __init__(self, client_id: str, access_token: str, enable_deep: bool = False):
        self.client_id = client_id
        self.access_token = access_token
        self.enable_deep = enable_deep
        
        self.std_url = "wss://api-feed.dhan.co"
        self.deep_url = "wss://depth-api-feed.dhan.co/twentydepth"
        
        self.on_tick_callback: Optional[Callable] = None
        self.running = False
        
        # Internal state to merge Bid/Ask packets and LTP
        # {security_id: {'bid': [], 'ask': [], 'ltp': ...}}
        self._depth_state = defaultdict(lambda: {"bid": [], "ask": [], "ltp": 0.0, "volume": 0, "v_buy": 0, "v_sell": 0})
        # Mapping for security_id -> symbol
        self._id_to_symbol = {}

    def set_id_meta_mapping(self, mapping: Dict[int, Dict[str, str]]):
        """Inject mapping from SecurityID (int) to Meta Dict {'symbol': str, 'segment': str}."""
        self._id_meta = mapping
        # Backward compatibility for existing logic
        self._id_to_symbol = {k: v['symbol'] for k, v in mapping.items()}

    async def connect(self, std_ids: List[int], deep_ids: List[int], on_tick: Callable):
        self.on_tick_callback = on_tick
        self.running = True
        
        # Start Standard Feed in a separate thread (using websocket-client as per Dhan team suggestion)
        import threading
        self.std_thread = threading.Thread(target=self._run_standard_feed_sync, args=(std_ids,), daemon=True)
        self.std_thread.start()
        
        # Start Deep Feed (async) if enabled
        if self.enable_deep:
            await self._feed_loop("DEEP", self.deep_url, deep_ids)
            
        # Keep main loop alive for deep feed
        while self.running:
            await asyncio.sleep(1)

    def _run_standard_feed_sync(self, security_ids: List[int]):
        """Synchronous WebSocketApp for Standard Feed to avoid 429 errors."""
        import websocket
        import ssl
        
        def on_message(ws, message):
            # logger.info(f"Dhan RAW Msg: type={type(message)}")
            if isinstance(message, bytes):
                self._parse_standard_binary(message)
            else:
                logger.info(f"Dhan STANDARD Text Msg: {message}")

        def on_error(ws, error):
            logger.error(f"Dhan STANDARD Error: {error}")

        def on_close(ws, close_status_code, close_msg):
            logger.warning(f"Dhan STANDARD Closed: {close_status_code} - {close_msg}")
            
        def on_open(ws):
            logger.info("Dhan STANDARD Connection Opened.")
            # Subscribe
            # Subscribe
            req_code = 21 
            chunk_size = 50
            
            # Group by segment
            by_segment = defaultdict(list)
            for sid in security_ids:
                meta = self._id_meta.get(sid, {})
                segment = meta.get('segment', 'NSE_EQ')
                by_segment[segment].append(int(sid))
            
            for segment, ids in by_segment.items():
                # Standard Equity = 21, Indices = 15
                seg_req_code = 15 if segment == 'IDX_I' else 21
                
                for i in range(0, len(ids), chunk_size):
                    chunk = ids[i:i + chunk_size]
                    subscription_msg = {
                        "RequestCode": seg_req_code,
                        "InstrumentCount": len(chunk),
                        "InstrumentList": [
                            {"ExchangeSegment": segment, "SecurityId": str(sid)} 
                            for sid in chunk
                        ]
                    }
                    ws.send(json.dumps(subscription_msg))
                    logger.info(f"Subscribed: {segment} batch {i//chunk_size + 1} ({len(chunk)} instruments) with Code {seg_req_code}")
                    time.sleep(0.5)

        import time
        import random
        
        backoff_delay = 1
        max_delay = 60
        
        while self.running:
            try:
                # Direct URL construction as per Dhan team
                socket_url = f"{self.std_url}?version=2&token={self.access_token}&clientId={self.client_id}&authType=2"
                logger.info(f"Connecting to Dhan STANDARD Sync Feed...")
                
                ws = websocket.WebSocketApp(socket_url,
                                          on_open=on_open,
                                          on_message=on_message,
                                          on_error=on_error,
                                          on_close=on_close)
                
                ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=25, ping_timeout=10)
                
                # If we are here, connection closed. Reset backoff if it was open for > 1 min? 
                # For now, simple exponential backoff on disconnect
                if self.running:
                    jitter = random.uniform(0, 1)
                    wait_time = min(backoff_delay + jitter, max_delay)
                    logger.warning(f"Dhan STANDARD Reconnecting in {wait_time:.2f}s...")
                    time.sleep(wait_time)
                    backoff_delay = min(backoff_delay * 2, max_delay)
                    
            except Exception as e:
                logger.error(f"Dhan STANDARD Sync Loop Crash: {e}")
                time.sleep(5)

    async def _feed_loop(self, name: str, url: str, security_ids: List[int]):
        while self.running:
            try:
                logger.info(f"Connecting to Dhan {name} Feed...")
                import ssl
                ssl_context = ssl._create_unverified_context()
                
                # Deep feed URL is simple
                current_url = f"{url}?token={self.access_token}&clientId={self.client_id}&authType=2"
                
                async with websockets.connect(current_url, ssl=ssl_context) as ws:
                    # Subscribe (Deep: 23)
                    req_code = 23
                    chunk_size = 50
                    
                    # Group by segment
                    by_segment = defaultdict(list)
                    for sid in security_ids:
                        meta = self._id_meta.get(sid, {})
                        segment = meta.get('segment', 'NSE_EQ')
                        by_segment[segment].append(int(sid))
                    
                    for segment, ids in by_segment.items():
                        for i in range(0, len(ids), chunk_size):
                            chunk = ids[i:i + chunk_size]
                            subscription_msg = {
                                "RequestCode": req_code,
                                "InstrumentCount": len(chunk),
                                "InstrumentList": [
                                    {"ExchangeSegment": segment, "SecurityId": str(sid)} 
                                    for sid in chunk
                                ]
                            }
                            await ws.send(json.dumps(subscription_msg))
                            logger.info(f"Subscribed Deep: {segment} batch {i//chunk_size + 1} ({len(chunk)} instruments)")
                            await asyncio.sleep(0.5)
                    
                    # Receive loop
                    while self.running:
                        try:
                            message = await ws.recv()
                            if isinstance(message, bytes):
                                self._parse_deep_binary(message)
                            else:
                                logger.debug(f"Dhan {name} Text Msg: {message}")
                        except websockets.ConnectionClosed:
                            logger.warning(f"Dhan {name} WS Closed. Reconnecting...")
                            break
                        except Exception as e:
                            logger.error(f"Error in Dhan {name} loop: {e}")
            except Exception as e:
                logger.error(f"Failed to connect to Dhan {name}: {e}. Retrying in 5s...")
                await asyncio.sleep(5)

    def _parse_standard_binary(self, data: bytes):
        """Standard Feed: LTP, Volume, 5-Level Depth."""
        try:
            if len(data) < 8: return
            feed_code = data[0]
            
            if feed_code in [2, 4, 6, 8, 15, 16, 17]:
                sec_id = struct.unpack('<I', data[4:8])[0]
                ltp = struct.unpack('<f', data[8:12])[0]
                
                state = self._depth_state[sec_id]
                if sec_id == 13:
                    logger.debug(f"DHAN NIFTY TICK: Code={feed_code} | Price={ltp}")
                state['ltp'] = ltp
                
                # For indices, volume is usually 0 but we can check if it exists in code 16
                # However, for now, just emitting the LTP change is enough for ORB
                
                if feed_code == 8:
                    # Full Packet has fields at specific offsets:
                    # Volume (22), Total Sell Qty (26), Total Buy Qty (30)
                    volume = struct.unpack('<I', data[22:26])[0]
                    t_sell = struct.unpack('<I', data[26:30])[0]
                    t_buy = struct.unpack('<I', data[30:34])[0]

                    state['volume'] = volume
                    state['total_buy_qty'] = t_buy
                    state['total_sell_qty'] = t_sell
                    
                    # 🚀 Critical: Extract Open Interest (OI)
                    # Offset 54: Open Interest (4-byte Unsigned Int)
                    # Offset 58: OI Change (4-byte Signed Int)
                    if len(data) >= 62:
                        oi = struct.unpack('<I', data[54:58])[0]
                        oi_change = struct.unpack('<i', data[58:62])[0]
                        state['oi'] = oi
                        state['oi_change'] = oi_change
                    
                    # Always parse 5-level depth for standard feed to ensure baseline L1 data
                    depth_start = 62 
                    bids, asks = [], []
                    for i in range(5):
                        base = depth_start + (i * 20)
                        if len(data[base:]) < 20: break
                        bq, aq, bo, ao, bp, ap = struct.unpack('<IIHHff', data[base:base+20])
                        bids.append({"price": bp, "qty": bq, "orders": bo})
                        asks.append({"price": ap, "qty": aq, "orders": ao})
                    
                    # Prefer deep data if already present. If current bid array is > 5 items, it came from deep feed.
                    if not self.enable_deep or len(state.get('bid', [])) <= 5:
                        state['bid'], state['ask'] = bids, asks

                self._emit_tick(sec_id)
        except Exception as e:
            logger.error(f"Error parsing Standard Dhan packet: {e}")

    def _parse_deep_binary(self, data: bytes):
        """20-Level Depth Feed (Codes 41/51)."""
        try:
            offset = 0
            while offset < len(data):
                if len(data[offset:]) < 12: break
                msg_len, resp_code, segment, sec_id, seq = struct.unpack('<HBBII', data[offset:offset+12])
                
                payload_len = msg_len - 12
                row_start = offset + 12
                rows = []
                for r in range(0, payload_len, 16):
                    row_data = data[row_start+r : row_start+r+16]
                    if len(row_data) < 16: break
                    price, qty, orders = struct.unpack('<dII', row_data)
                    rows.append({"price": price, "qty": qty, "orders": orders})
                
                state = self._depth_state[sec_id]
                if resp_code == 41: state["bid"] = rows
                elif resp_code == 51: state["ask"] = rows
                
                self._emit_tick(sec_id)
                offset += msg_len
        except Exception as e:
            logger.error(f"Error parsing Deep Dhan packet: {e}")

    def _emit_tick(self, sec_id: int):
        symbol = self._id_to_symbol.get(sec_id)
        if symbol and self.on_tick_callback:
            state = self._depth_state[sec_id]
            meta = self._id_meta.get(sec_id, {})
            
            tick = {
                "symbol": symbol,
                "security_id": sec_id,
                "is_derivative": meta.get('is_derivative', False),
                "ltp": state["ltp"],
                "bid": state["bid"],
                "ask": state["ask"],
                "volume": state["volume"],
                "total_buy_qty": state.get("total_buy_qty", 0),
                "total_sell_qty": state.get("total_sell_qty", 0),
                "oi": state.get("oi", 0),
                "oi_change": state.get("oi_change", 0),
                "timestamp": datetime.now().isoformat(),
                "source": "DHAN_DEEP" if self.enable_deep else "DHAN_STD"
            }
            self.on_tick_callback(tick)

    def stop(self):
        self.running = False

async def main_test():
    # Simple test logic
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    client = DhanFeedClient(cid, token)
    
    # Mock SBIN (ID: 3045)
    client.set_symbol_mapping({3045: "SBIN"})
    
    def handle_tick(tick):
        print(f"Tick: {tick['symbol']} | Bids: {len(tick['bid'])} | Asks: {len(tick['ask'])}")

    await client.connect([3045], handle_tick)

if __name__ == "__main__":
    asyncio.run(main_test())
