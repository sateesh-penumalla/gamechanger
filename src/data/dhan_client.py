import os
import requests
import json
import pandas as pd
import ssl
import time
import threading
from loguru import logger
from typing import List, Optional, Dict, Any
from dhanhq import dhanhq
from datetime import datetime, timedelta


class DhanRateLimiter:
    """Global rate limiter for Dhan API across threads."""
    def __init__(self, seconds_per_req: float = 1.3):
        self.lock = threading.Lock()
        self.interval = seconds_per_req
        self.last_call = 0

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.interval:
                wait_time = self.interval - elapsed
                time.sleep(wait_time)
            self.last_call = time.time()

# Global instance for all DhanDataClient instances
_limiter = DhanRateLimiter(seconds_per_req=1.3) # Slightly aggressive 1.3s

class DhanDataClient:
    def __init__(self, client_id: Optional[str] = None, access_token: Optional[str] = None):
        cid = client_id or os.getenv("DHAN_CLIENT_ID")
        token = access_token or os.getenv("DHAN_ACCESS_TOKEN")
        
        if not cid or not token:
            logger.error("DhanDataClient: Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")
            
        self.client_id = cid
        self.access_token = token
        self.dhan = dhanhq(cid, token)
        self.master_csv_path = "data/dhan_scrip_master.csv"
        self._master_df = None
        self._id_map = {}
        self._meta_map = {} # Store instrument type and segment
        self._fno_map = {} # Map base symbol -> list of FNO contracts
        
        # Ensure config dir exists
        os.makedirs("config", exist_ok=True)
        
        logger.info("Initializing DhanHQ Data Client...")
        self._load_master_data()

    def _load_master_data(self):
        """Downloads or loads the Dhan instrument master for mapping symbols to IDs."""
        try:
            import urllib.request
            
            # Check if cache is fresh (older than 24h)
            should_download = True
            if os.path.exists(self.master_csv_path):
                mtime = os.path.getmtime(self.master_csv_path)
                if (datetime.now().timestamp() - mtime) < 86400:
                    should_download = False
            
            if should_download:
                logger.info("Downloading Dhan Instrument Master CSV...")
                url = "https://images.dhan.co/api-data/api-scrip-master.csv"
                
                # Handling Mac SSL Certificate issue via urllib
                context = ssl._create_unverified_context()
                with urllib.request.urlopen(url, context=context) as response, open(self.master_csv_path, 'wb') as out_file:
                    out_file.write(response.read())
                
                logger.info("Dhan Master CSV downloaded successfully.")

            self._master_df = pd.read_csv(self.master_csv_path, low_memory=False)
            
            # Filter for NSE Equity and Indices
            nse_data = self._master_df[
                (self._master_df['SEM_EXM_EXCH_ID'] == 'NSE') & 
                (self._master_df['SEM_INSTRUMENT_NAME'].isin(['EQUITY', 'INDEX']))
            ]
            
            for _, row in nse_data.iterrows():
                symbol = str(row['SEM_TRADING_SYMBOL']).strip()
                sec_id = int(row['SEM_SMST_SECURITY_ID'])
                instr = str(row['SEM_INSTRUMENT_NAME']).strip().upper()
                
                self._id_map[symbol] = sec_id
                
                # Derive market segment
                segment = "NSE_EQ"
                if instr == "INDEX":
                    segment = "IDX_I"
                
                self._meta_map[symbol] = {
                    "id": sec_id,
                    "instr": instr,
                    "segment": segment
                }

            # Filter for NSE Futures
            fno_data = self._master_df[
                (self._master_df['SEM_EXM_EXCH_ID'] == 'NSE') & 
                (self._master_df['SEM_INSTRUMENT_NAME'].isin(['FUTSTK', 'FUTIDX']))
            ]
            
            for _, row in fno_data.iterrows():
                trading_symbol = str(row['SEM_TRADING_SYMBOL']).strip()
                # Parse base symbol (e.g., RELIANCE from RELIANCE-Feb2026-FUT)
                base_symbol = trading_symbol.split('-')[0].strip()
                
                if base_symbol not in self._fno_map:
                    self._fno_map[base_symbol] = []
                
                self._fno_map[base_symbol].append({
                    "id": int(row['SEM_SMST_SECURITY_ID']),
                    "symbol": trading_symbol,
                    "expiry": str(row['SEM_EXPIRY_DATE']),
                    "instr": str(row['SEM_INSTRUMENT_NAME']).strip(),
                    "segment": "NSE_FNO"
                })
            
            # Common Yahoo-to-Dhan Index Mappings
            yahoo_indices = {
                "NSEI": "NIFTY",
                "NSEBANK": "BANKNIFTY",
                "CNXENERGY": "NIFTY ENERGY",
                "CNXINFRA": "NIFTY INFRA",
                "CNXAUTO": "NIFTY AUTO",
                "CNXMETAL": "NIFTY METAL",
                "CNXIT": "NIFTY IT",
                "CNXPHARMA": "NIFTY PHARMA",
                "CNXPSUBANK": "NIFTY PSU BANK",
                "CNXREALTY": "NIFTY REALTY",
                "CNXFMCG": "NIFTY FMCG",
                "NIFTY 50": "NIFTY",
                "NIFTY BANK": "BANKNIFTY"
            }
            for y_sym, d_sym in yahoo_indices.items():
                if d_sym in self._meta_map:
                    self._id_map[y_sym] = self._id_map[d_sym]
                    self._meta_map[y_sym] = self._meta_map[d_sym]
                elif y_sym not in self._id_map:
                    # Fallback for common IDs if mapping fails
                    if y_sym == "NSEI": 
                        self._id_map[y_sym] = 13
                        self._meta_map[y_sym] = {"id": 13, "instr": "INDEX", "segment": "IDX_I"}
                    if y_sym == "NSEBANK": 
                        self._id_map[y_sym] = 25
                        self._meta_map[y_sym] = {"id": 25, "instr": "INDEX", "segment": "IDX_I"}

            logger.info(f"Dhan Master List Loaded. {len(self._id_map)} symbols (Equity + Indices) mapped.")
        except Exception as e:
            logger.error(f"Error loading Dhan master data: {e}")

    def get_active_futures_id(self, symbol: str) -> Optional[int]:
        """
        Finds the security ID for the current month future contract of a given base symbol.
        """
        clean_symbol = str(symbol).replace(".NS", "").replace("^", "").strip()
        if clean_symbol == "NSEI": clean_symbol = "NIFTY"
        if clean_symbol == "NSEBANK": clean_symbol = "BANKNIFTY"
        
        contracts = self._fno_map.get(clean_symbol)
        if not contracts:
            return None
        
        # Sort by expiry to get the nearest one (current month)
        # Expiry format: 2026-02-26 15:30:00
        # Filtering to ensure we only get NSE scrips (already done in loader but for safety)
        sorted_contracts = sorted(contracts, key=lambda x: x['expiry'])
        return sorted_contracts[0]['id']

    def get_active_expiry(self, symbol: str) -> Optional[str]:
        """
        Finds the nearest expiry date for a symbol.
        """
        clean_symbol = str(symbol).replace(".NS", "").replace("^", "").strip()
        contracts = self._fno_map.get(clean_symbol)
        if not contracts:
            return None
        
        # Filter for current or future expiries only to avoid expired master data
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        valid_contracts = [c for c in contracts if c['expiry'] >= now_str]
        
        if not valid_contracts:
            # Fallback to the latest one if all are technically 'expired' in master but still in segment
            valid_contracts = contracts
            
        sorted_contracts = sorted(valid_contracts, key=lambda x: x['expiry'])
        # Return only the date part YYYY-MM-DD
        return sorted_contracts[0]['expiry'].split(' ')[0]

    def get_security_id(self, symbol: str) -> Optional[int]:
        """Resolves a symbol name (RELIANCE) to a Dhan Security ID."""
        clean_symbol = str(symbol).replace(".NS", "").replace("^", "").strip()
        if clean_symbol == "NSEI": clean_symbol = "NIFTY"
        if clean_symbol == "NSEBANK": clean_symbol = "BANKNIFTY"
        return self._id_map.get(clean_symbol)

    def get_selective_indices(self, index_names: List[str] = None) -> Dict[str, int]:
        """
        Returns security IDs for Core indices + specific indices requested.
        Core: NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY.
        """
        core_symbols = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
        target_symbols = set(core_symbols)
        
        if index_names:
            for n in index_names:
                if n: target_symbols.add(n)

        results = {}
        for sym in target_symbols:
            # Try direct mapping
            sec_id = self._id_map.get(sym)
            if not sec_id:
                # Try without spaces (NIFTY IT -> NIFTYIT)
                no_space = sym.replace(" ", "")
                sec_id = self._id_map.get(no_space)
            
            if sec_id:
                results[sym] = sec_id
                
        return results

    def get_bulk_quotes(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Fetches real-time quotes for multiple symbols in batches to reduce rate limit hits.
        Returns a mapping of {symbol: quote_data}.
        """
        results = {}
        # Group by segment
        segment_map = {"NSE_EQ": [], "IDX_I": []}
        symbol_to_id = {}

        for sym in symbols:
            clean_sym = str(sym).replace(".NS", "").replace("^", "").strip()
            meta = self._meta_map.get(clean_sym)
            if not meta: 
                # Try raw symbol if clean fails
                meta = self._meta_map.get(sym)
                if not meta: continue
            
            sec_id = meta["id"]
            segment = meta["segment"]
            
            symbol_to_id[sec_id] = sym
            segment_map[segment].append(int(sec_id))

        # Batch calls (Dhan allows up to 50 IDs per request for quote_data)
        for segment, ids in segment_map.items():
            if not ids: continue
            
            for i in range(0, len(ids), 50):
                batch_ids = ids[i:i+50]
                try:
                    logger.debug(f"Dhan Bulk Batch: {segment} -> {batch_ids}")
                    response = self.dhan.quote_data({segment: batch_ids})
                    
                    if response and response.get('status') == 'success':
                        # Recursive search for the security-keyed data block
                        data_block = {}
                        temp = response.get('data', {})
                        
                        # Depth search up to 3 levels
                        for _ in range(3):
                            if not isinstance(temp, dict): break
                            if any(str(k).isdigit() for k in temp.keys()):
                                data_block = temp
                                break
                            if segment in temp:
                                temp = temp[segment]
                            elif 'data' in temp:
                                temp = temp['data']
                            else:
                                break
                        
                        logger.debug(f"Dhan Bulk Quote Found {len(data_block)} security datasets.")
                        
                        for sid, sdata in data_block.items():
                            if not isinstance(sdata, dict): continue
                            
                            try:
                                sid_int = int(sid)
                                orig_sym = symbol_to_id.get(sid_int)
                                if orig_sym:
                                    # Normalize keys
                                    if 'lastPrice' in sdata:
                                        sdata['last_price'] = sdata['lastPrice']
                                    elif 'last_price' in sdata:
                                        sdata['lastPrice'] = sdata['last_price']
                                    
                                    # Calculate percentage change if missing
                                    if 'net_change_percentage' not in sdata and 'net_change' in sdata and sdata.get('last_price'):
                                        lp = sdata['last_price']
                                        nc = sdata['net_change']
                                        prev_close = lp - nc
                                        if prev_close > 0:
                                            sdata['net_change_percentage'] = (nc / prev_close) * 100
                                            
                                    results[orig_sym] = sdata
                                    logger.debug(f"Mapped {orig_sym} -> Change: {sdata.get('net_change_percentage', 0):.2f}%")
                            except ValueError:
                                pass 
                    else:
                        logger.warning(f"Dhan Bulk Quote FAILED for segment {segment}: {response.get('remarks') if response else 'No response'}")
                    time.sleep(0.2)
                except Exception as e:
                    logger.error(f"Error in bulk quote batch for {segment}: {e}")

        return results

    def get_quote_data(self, symbol: str) -> Optional[Dict]:
        """Fetches the latest real-time quote (LTP, OHLC, etc.) from Dhan."""
        sec_id = self.get_security_id(symbol)
        if not sec_id: return None
        
        try:
            # Determine Segment
            segment = "NSE_EQ"
            # Indices check
            if symbol.upper() in ["NIFTY", "BANKNIFTY", "NSEI", "NSEBANK", "NIFTY 50", "NIFTY BANK"] or (sec_id and sec_id < 1000):
                segment = "IDX_I" 

            # Dhan quote_data returns data keyed by security ID
            response = self.dhan.quote_data({
                segment: [int(sec_id)]
            })
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                # Extract the actual data for the specific security ID
                sec_data = data.get(str(sec_id))
                
                # Double check for integer key as well
                if not sec_data:
                    sec_data = data.get(int(sec_id))

                if not sec_data and data:
                    # Fallback: find the first dictionary value if ID-based lookup fails
                    for val in data.values():
                        if isinstance(val, dict):
                            sec_data = val
                            break
                
                if not sec_data:
                    logger.warning(f"Dhan Quote for {symbol} (ID: {sec_id}) yielded no security data. Response keys: {list(data.keys())}")
                else:
                    # Rename keys to a standard format if they are slightly different (e.g. CamelCase)
                    if 'lastPrice' in sec_data and 'last_price' not in sec_data:
                        sec_data['last_price'] = sec_data['lastPrice']
                
                return sec_data
            else:
                logger.warning(f"Dhan Quote for {symbol} failed: {response.get('remarks') if response else 'No response'}")
                
            return None
        except Exception as e:
            logger.error(f"Error fetching Dhan quote for {symbol}: {e}")
            return None

    def fetch_bulk_intraday(self, symbols: List[str], interval: str = "1m", max_workers: int = 5, from_dates: Dict[str, str] = None) -> Dict[str, pd.DataFrame]:
        """
        Fetches intraday data for multiple symbols in parallel.
        Returns a mapping of {symbol: DataFrame}.
        `from_dates`: Optional dictionary mapping symbol to ISO date string (YYYY-MM-DD).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = {}
        logger.info(f"Starting bulk intraday fetch for {len(symbols)} symbols...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {}
            for sym in symbols:
                fd = from_dates.get(sym) if from_dates else None
                future_to_symbol[executor.submit(self.fetch_realtime_data, sym, period="1d", interval=interval, from_date_str=fd)] = sym
            
            for future in as_completed(future_to_symbol):
                sym = future_to_symbol[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        results[sym] = df
                except Exception as e:
                    logger.error(f"Bulk fetch failed for {sym}: {e}")
        
        logger.info(f"Bulk intraday fetch complete. Success: {len(results)}/{len(symbols)}")
        return results

    def fetch_realtime_data(self, symbol: str, period: str = "5d", interval: str = "5m", from_date_str: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetches intraday/historical data from DhanHQ and returns a standard OHLCV DataFrame.
        `from_date_str`: Optional ISO format 'YYYY-MM-DD'. If provided, overrides 'period'.
        """
        clean_symbol = str(symbol).replace(".NS", "").replace("^", "").strip()
        if clean_symbol == "NSEI": clean_symbol = "NIFTY"
        if clean_symbol == "NSEBANK": clean_symbol = "BANKNIFTY"
        
        meta = self._meta_map.get(clean_symbol)
        if not meta:
            logger.warning(f"Could not resolve metadata (ID/Segment) for {symbol}")
            return None
        
        sec_id = meta["id"]
        segment = meta["segment"]
        instr = meta["instr"]

        _limiter.wait()

        try:
            # Map intervals to Dhan-compatible versions or mark for local resampling
            requested_interval = str(interval)
            dhan_interval = None
            
            if requested_interval in ["1wk", "1mo", "1d"]:
                dhan_interval = "1d"
            else:
                try:
                    dhan_interval = int(requested_interval.replace("m", ""))
                except ValueError:
                    dhan_interval = 1
            
            if from_date_str:
                from_date = from_date_str
            else:
                from_date = (datetime.now() - timedelta(days=7 if requested_interval not in ["1wk", "1mo", "1d"] else 800)).strftime("%Y-%m-%d")
            
            to_date = datetime.now().strftime("%Y-%m-%d")
            
            df = None
            # Fetch Data
            if dhan_interval == "1d":
                df = self.get_historical_ohlc(
                    security_id=str(sec_id),
                    exchange_segment=segment,
                    from_date=from_date,
                    to_date=to_date,
                    instrument=instr,
                    expiry_code=0
                )
            else:
                response = self.dhan.intraday_minute_data(
                    str(sec_id),
                    segment,
                    instr,
                    from_date,
                    to_date,
                    interval=dhan_interval
                )
                if response and response.get('status') == 'success' and response.get('data'):
                    df = pd.DataFrame(response['data'])
                    df = df.rename(columns={
                        'open': 'Open',
                        'high': 'High',
                        'low': 'Low',
                        'close': 'Close',
                        'volume': 'Volume',
                        'start_Time': 'Datetime',
                        'timestamp': 'Datetime'
                    })
                    
                    if 'Datetime' in df.columns:
                        df['Datetime'] = pd.to_datetime(df['Datetime'], unit='s')
                        df['Datetime'] = df['Datetime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                        df.set_index('Datetime', inplace=True)
                else:
                    logger.warning(f"Dhan API returned no data for {symbol}: {response.get('remarks') if response else 'No response'}")
                    return None

            if df is None or df.empty:
                return None
            
            # Common Filtering and Conversion
            df.index = pd.to_datetime(df.index)
            
            if "d" in period:
                days = int(period.replace("d", ""))
                cutoff = datetime.now() - timedelta(days=days)
                df = df[df.index >= cutoff]

            # Local Resampling for Weekly/Monthly
            if requested_interval == "1wk":
                df = df.resample('W-MON').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
            elif requested_interval == "1mo":
                df = df.resample('MS').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()

            return df

        except Exception as e:
            logger.error(f"Error fetching Dhan data for {symbol}: {e}")
            return None

        except Exception as e:
            logger.error(f"Error fetching Dhan data for {symbol}: {e}")
            return None
    # --- ORDER MANAGEMENT ---

    def place_order(self, symbol: str, transaction_type: str, qty: int, order_type: str = "MARKET", 
                    price: float = 0.0, trigger_price: float = 0.0, product_type: str = "INTRADAY", 
                    validity: str = "DAY") -> Optional[Dict]:
        """
        Places an order on Dhan.
        transaction_type: BUY, SELL
        order_type: MARKET, LIMIT, STOP_LOSS, STOP_LOSS_MARKET
        product_type: CNC, INTRADAY, MARGIN, MTF, CO, BO
        """
        sec_id = self.get_security_id(symbol)
        if not sec_id:
            logger.error(f"Dhan Order: Could not resolve security ID for {symbol}")
            return None
        
        meta = self._meta_map.get(str(symbol).replace(".NS", "").replace("^", "").strip())
        segment = meta["segment"] if meta else "NSE_EQ"

        try:
            response = self.dhan.place_order(
                security_id=str(sec_id),
                exchange_segment=segment,
                transaction_type=transaction_type,
                quantity=qty,
                order_type=order_type,
                product_type=product_type,
                price=price,
                trigger_price=trigger_price,
                validity=validity,
                after_market_order=False,
                amo_time='OPEN'
            )

            if response and response.get('status') == 'success':
                logger.info(f"Dhan Order Placed: {symbol} {transaction_type} {qty} @ {order_type}. OrderID: {response.get('data', {}).get('orderId')}")
                return response.get('data')
            else:
                logger.error(f"Dhan Order FAILED for {symbol}: {response.get('remarks') if response else 'No response'}")
                return None
        except Exception as e:
            logger.error(f"Exception during Dhan order placement for {symbol}: {e}")
            return None

    def place_super_order(self, symbol: str, side: str, entry_price: float, target_price: float, 
                          sl_price: float, qty: int, product_type: str = "INTRADAY",
                          order_type: str = "LIMIT") -> Optional[Dict]:
        """
        Places a Dhan Super Order (entry + target + SL in one API call).
        
        Args:
            symbol: Stock symbol (e.g., 'RELIANCE')
            side: 'LONG' or 'SHORT'
            entry_price: Entry price (limit order)
            target_price: Target price (take profit)
            sl_price: Stop-loss price
            qty: Quantity to trade
            product_type: 'INTRADAY' or 'CNC' (default: INTRADAY)
            order_type: 'LIMIT' or 'MARKET' (default: LIMIT)
        
        Returns:
            dict: {
                'entry_order_id': str,
                'target_order_id': str,
                'sl_order_id': str,
                'status': 'success' or 'failed'
            }
        """
        sec_id = self.get_security_id(symbol)
        if not sec_id:
            logger.error(f"Dhan Super Order: Could not resolve security ID for {symbol}")
            return None
        
        meta = self._meta_map.get(str(symbol).replace(".NS", "").replace("^", "").strip())
        segment = meta["segment"] if meta else "NSE_EQ"
        
        # Map side to transaction type
        transaction_type = "BUY" if side == "LONG" else "SELL"
        
        try:
            # Dhan Super Order API (Direct REST call as SDK 2.0.2 lacks it)
            url = f"{self.dhan.base_url}/super/orders"
            
            # Extract webhookUrl from Token if available (fallback to .env)
            webhook_url = os.getenv("DHAN_WEBHOOK_URL", "https://api.marginos.app/dhan/postback")
            
            payload = {
                "dhanClientId": self.dhan.client_id,
                "transactionType": transaction_type,
                "exchangeSegment": segment,
                "productType": product_type,
                "orderType": order_type,
                "validity": "DAY",
                "securityId": str(sec_id),
                "quantity": int(qty),
                "price": float(entry_price) if order_type == "LIMIT" else 0.0,
                "targetPrice": float(target_price),
                "stopLossPrice": float(sl_price),
                "webhookUrl": webhook_url
            }
            
            logger.info(f"Placing Dhan Super Order with Webhook: {webhook_url}")
            response = requests.post(
                url, 
                json=payload, 
                headers=self.dhan.header, 
                timeout=60
            )
            
            # Use SDK's internal parser for consistency
            parsed_res = self.dhan._parse_response(response)
            
            if parsed_res and parsed_res.get('status') == 'success':
                data = parsed_res.get('data', {})
                # Extract IDs with fallbacks
                entry_id = data.get('orderId')
                target_id = data.get('targetOrderId') or data.get('boTargetOrderId')
                sl_id = data.get('stopLossOrderId') or data.get('boStopLossOrderId')
                
                logger.info(f"✅ Dhan Super Order Placed: {symbol} {side} {qty} @ ₹{entry_price:.2f}")
                logger.info(f"   Entry Order ID: {entry_id}")
                logger.info(f"   Target Order ID: {target_id}")
                logger.info(f"   SL Order ID: {sl_id}")
                logger.info(f"   Raw Keys: {list(data.keys())}")
                
                return {
                    'entry_order_id': entry_id,
                    'target_order_id': target_id,
                    'sl_order_id': sl_id,
                    'status': 'success',
                    'raw_response': data
                }
            else:
                remarks = parsed_res.get('remarks') if parsed_res else 'No response'
                logger.error(f"❌ Dhan Super Order FAILED for {symbol}: {remarks}")
                return {
                    'status': 'failed',
                    'error': str(remarks)
                }
        except Exception as e:
            logger.error(f"❌ Exception during Dhan super order placement for {symbol}: {e}")
            return {
                'status': 'failed',
                'error': str(e)
            }

    def get_order_list(self) -> List[Dict]:
        """Fetches all orders for the current day."""
        try:
            response = self.dhan.get_order_list()
            if response and response.get('status') == 'success':
                return response.get('data', [])
            return []
        except Exception as e:
            logger.error(f"Error fetching Dhan order list: {e}")
            return []

    def get_order_status(self, order_id: str) -> Optional[Dict]:
        """Fetches status for a specific order ID."""
        try:
            response = self.dhan.get_order_by_id(order_id)
            if response and response.get('status') == 'success':
                return response.get('data')
            return None
        except Exception as e:
            logger.error(f"Error fetching Dhan order status for {order_id}: {e}")
            return None

    def get_positions(self) -> Optional[List[Dict]]:
        """
        Fetches currently open positions from Dhan.
        Returns None if the API call fails (e.g. expired token), returns [] if no positions.
        """
        try:
            response = self.dhan.get_positions()
            if response and response.get('status') == 'success':
                return response.get('data', [])
            
            # Log specific error if status is not success
            err = response.get('remarks') or response.get('status')
            logger.error(f"Dhan API Error (get_positions): {err}")
            return None
        except Exception as e:
            logger.error(f"Exception fetching Dhan positions: {e}")
            return None

    def get_trade_book(self) -> List[Dict]:
        """Fetches all trades for the current day."""
        try:
            response = self.dhan.get_trade_book()
            if response and response.get('status') == 'success':
                return response.get('data', [])
            return []
        except Exception as e:
            logger.error(f"Error fetching Dhan trade book: {e}")
            return []
    def get_fund_limits(self) -> Optional[float]:
        """Fetches available cash margin from Dhan."""
        try:
            response = self.dhan.get_fund_limits()
            if response and response.get('status') == 'success':
                data = response.get('data')
                # 'availabelBalance' is the key in Dhan API for free cash
                # Fallback to 'availableMargin' if balance is 0 or missing
                return float(data.get('availabelBalance', data.get('availableMargin', 0.0)))
            return 0.0
        except Exception as e:
            logger.error(f"Error fetching fund limits: {e}")
            return 0.0

    def get_ohlc_bulk(self, instruments: Dict[str, List[int]]) -> Optional[Dict]:
        """
        Fetch current OHLC data for specified instruments using Dhan v2 MarketFeed API.
        
        Example input: {"NSE_EQ": [11536], "NSE_FNO": [49081, 49082]}
        """
        url = "https://api.dhan.co/v2/marketfeed/ohlc"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'access-token': self.access_token,
            'client-id': self.client_id
        }
        
        try:
            _limiter.wait()
            response = requests.post(url, headers=headers, data=json.dumps(instruments))
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching bulk OHLC data: {e}")
            return None

    def get_option_chain(self, underlying_sid: int, underlying_seg: str, expiry: str):
        logger.info(f"Fetching Option Chain for {underlying_sid} | Seg: {underlying_seg} | Expiry: {expiry}")
        """
        Fetches the full option chain for an underlying.
        Returns a list of strikes with OI, Greeks, and LTP.
        Rate limit: 1 unique request every 3 seconds.
        """
        url = "https://api.dhan.co/v2/optionchain"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'access-token': self.access_token,
            'client-id': self.client_id
        }
        payload = {
            "UnderlyingScrip": int(underlying_sid),
            "UnderlyingSeg": underlying_seg,
            "Expiry": expiry
        }
        
        try:
            # Option Chain has a stricter rate limit than standard OHLC
            time.sleep(3) 
            _limiter.wait()
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            data = response.json()
            if data.get('status') == 'success':
                return data.get('data', [])
            else:
                logger.warning(f"Dhan Option Chain Failed for SID {underlying_sid} ({expiry}): {data.get('remarks')} | Status: {data.get('status')}")
                return []
        except Exception as e:
            logger.error(f"Error fetching Option Chain: {e}")
            return []

    def get_historical_option_chain(self, underlying_sid: int, underlying_seg: str, expiry: str, from_date: str, to_date: str):
        """
        Fetches the historical option chain (including expired contracts) for a specific expiry within a max 30-day window.
        Returns a dictionary or list of historical daily/minute data for the strikes.
        Rate limit: 1 unique request every 3 seconds. Max 30 days per request.
        """
        # Dhan doesn't have a single "give me the whole historical chain" endpoint.
        # But we can get the historical Option Chain data by polling specific strikes 
        # using the historical charts API or the specific Option Data API if available.
        # Wait, the Dhan documentation mentions an API to get data for expired contracts using ATM +/-.
        url = "https://api.dhan.co/v2/charts/historical" # Using historical charts with FNO segment
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'access-token': self.access_token,
            'client-id': self.client_id
        }
        
        # NOTE: A true "historical option chain snapshot" requires looping through strikes 
        # or relying on Dhan's daily historical snapshots if they offer a bulk endpoint.
        # Since Dhan requires specific Security IDs or an ATM offset for historical, 
        # we will fetch the live Option Chain structure first to get the IDs, or use a workaround.
        # For our Swing Strategy, we only need the aggregate OI and ATM IV.
        # We will implement the exact API call for historical options here.
        # *Placeholder for the actual Dhan Historical Options API payload*
        logger.warning("get_historical_option_chain: Fetching full historical chains is highly rate-limited. Ensure 30-day bounds.")
        pass

    def get_historical_ohlc(self, security_id: str, exchange_segment: str, 
                            from_date: str, to_date: str, interval: str = "D", 
                            instrument: str = "EQUITY", expiry_code: int = 0, 
                            oi: bool = False) -> Optional[pd.DataFrame]:
        """
        Fetch historical daily OHLC data from Dhan v2 Charts API with automatic batching for large ranges.
        """
        url = "https://api.dhan.co/v2/charts/historical"
        headers = {
            'Content-Type': 'application/json',
            'access-token': self.access_token,
            'client-id': self.client_id
        }
        
        from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        to_dt = datetime.strptime(to_date, "%Y-%m-%d")
        
        all_dfs = []
        current_start = from_dt
        
        while current_start < to_dt:
            current_end = min(current_start + timedelta(days=90), to_dt)
            
            payload = {
                "securityId": str(security_id),
                "exchangeSegment": exchange_segment,
                "instrument": instrument,
                "interval": interval,
                "expiryCode": expiry_code,
                "fromDate": current_start.strftime("%Y-%m-%d"),
                "toDate": current_end.strftime("%Y-%m-%d")
            }
            
            try:
                _limiter.wait()
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                
                if response.status_code != 200:
                    error_data = response.json() if "json" in response.headers.get("Content-Type", "") else response.text
                    logger.error(f"Dhan Historical API Error {response.status_code} for {security_id}: {error_data} | Payload: {payload}")
                    
                    if response.status_code in [401, 429]:
                        raise Exception(f"Stop: Dhan API Critical Error {response.status_code}")
                        
                    current_start = current_end + timedelta(days=1)
                    continue
                
                data = response.json()
                if not data or 'open' not in data:
                    current_start = current_end + timedelta(days=1)
                    continue
                    
                open_data = data.get('open', [])
                high_data = data.get('high', [])
                low_data = data.get('low', [])
                close_data = data.get('close', [])
                vol_data = data.get('volume', [])
                oi_data = data.get('oi', []) or data.get('open_interest', [])
                
                if not oi_data and open_data:
                    oi_data = [0.0] * len(open_data)
                elif oi_data and len(oi_data) < len(open_data):
                    oi_data.extend([0.0] * (len(open_data) - len(oi_data)))

                batch_df = pd.DataFrame({
                    'Open': open_data,
                    'High': high_data,
                    'Low': low_data,
                    'Close': close_data,
                    'Volume': vol_data,
                    'OI': oi_data
                })
                
                timestamps = data.get('timestamp', [])
                if timestamps:
                    batch_df['Datetime'] = pd.to_datetime(timestamps, unit='s')
                    batch_df['Datetime'] = batch_df['Datetime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                    batch_df.set_index('Datetime', inplace=True)
                else:
                    dates = [(current_start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(len(batch_df))]
                    batch_df.index = pd.to_datetime(dates)
                
                all_dfs.append(batch_df)
                
            except Exception as e:
                if "Stop:" in str(e): raise e
                logger.error(f"Network/Parse Error fetching historical for {security_id}: {e}")
            
            current_start = current_end + timedelta(days=1)

        if not all_dfs:
            return None
            
        return pd.concat(all_dfs).drop_duplicates().sort_index()
