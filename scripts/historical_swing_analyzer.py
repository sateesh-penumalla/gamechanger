import argparse
import pandas as pd
import numpy as np
import os
import sys
import time
import json
from datetime import datetime, timedelta
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.data.dhan_client import DhanDataClient
from src.db.schema import HistoryDailyOHLC, AlphaSignal

class HistoricalSwingAnalyzer:
    def __init__(self):
        load_dotenv()
        db_url = os.getenv("DATABASE_URL")
        
        # DEBUG
        print(f"DEBUG: Loaded DATABASE_URL from env: {db_url}")
        
        # Ensure we use PyMySQL driver and correct local port/creds if needed
        if not db_url or "localhost" in db_url or "127.0.0.1" in db_url:
            # Verified working string from terminal tests
            db_url = "mysql+pymysql://root:root@127.0.0.1:3307/bharatquant_sniper"
            print(f"DEBUG: Using fallback/forced URL: {db_url}")
        elif db_url.startswith("mysql://"):
            db_url = db_url.replace("mysql://", "mysql+pymysql://")
            
        self.engine = create_engine(db_url, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Initialize Dhan Client for Deep Recon (Live Data)
        self.dhan = None
        
    def _init_dhan(self):
        """Lazy initialization of Dhan Client."""
        if self.dhan is None:
            try:
                self.dhan = DhanDataClient()
                logger.info("Initialized DhanDataClient for Deep Recon.")
            except Exception as e:
                logger.error(f"Failed to initialize DhanDataClient: {e}")
                return False
        return True
        
    def fetch_local_history(self, symbol: str, lookback_days: int = 180) -> pd.DataFrame:
        """Fetches historical daily OHLCV and OI straight from local MySQL DB using pymysql."""
        import pymysql
        
        try:
            # Verified credentials from debug_db.py
            conn = pymysql.connect(
                host='127.0.0.1',
                port=3307,
                user='root',
                password='root',
                database='bharatquant_sniper',
                cursorclass=pymysql.cursors.DictCursor
            )
            
            cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S")
            
            query = f"""
                SELECT timestamp, open, high, low, close, volume, oi, pcr_oi, pcr_vol, max_pain, atm_iv 
                FROM history_daily_ohlc 
                WHERE symbol = '{symbol}' AND timestamp >= '{cutoff_date}'
                ORDER BY timestamp ASC
            """
            
            with conn.cursor() as cursor:
                cursor.execute(query)
                records = cursor.fetchall()
                
            conn.close()
            
            if not records:
                logger.warning(f"No records found in database for {symbol}")
                return pd.DataFrame()
                
            # Convert to DataFrame
            data = []
            for r in records:
                data.append({
                    "Date": r['timestamp'],
                    "Open": r['open'],
                    "High": r['high'],
                    "Low": r['low'],
                    "Close": r['close'],
                    "Volume": r['volume'],
                    "OI": r['oi'] if r['oi'] else 0.0,
                    "PCR_OI": r['pcr_oi'] if r['pcr_oi'] else 0.0,
                    "PCR_VOL": r['pcr_vol'] if r['pcr_vol'] else 0.0,
                    "Max_Pain": r['max_pain'] if r['max_pain'] else 0.0,
                    "ATM_IV": r['atm_iv'] if r['atm_iv'] else 0.0
                })
                
            df = pd.DataFrame(data)
            df.set_index("Date", inplace=True)
            return df
            
        except Exception as e:
            logger.error(f"Error fetching DB history for {symbol} via pymysql: {e}")
            return pd.DataFrame()

    def calculate_vcp_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates Mark Minervini style Volatility Contraction Pattern (VCP) metrics.
        - Tightening price action (ATR descending)
        - Volume dying down during base building (Dry up)
        - Moving Averages alignment
        """
        # 1. Moving Averages (Trend Filter)
        df['SMA_50'] = df['Close'].rolling(window=50, min_periods=1).mean()
        df['SMA_150'] = df['Close'].rolling(window=150, min_periods=1).mean()
        df['SMA_200'] = df['Close'].rolling(window=200, min_periods=1).mean()
        
        # 2. Average True Range (Volatility)
        high_low = df['High'] - df['Low']
        high_prev_close = np.abs(df['High'] - df['Close'].shift(1))
        low_prev_close = np.abs(df['Low'] - df['Close'].shift(1))
        tr = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
        df['ATR_14'] = tr.rolling(window=14, min_periods=1).mean()
        df['ATR_3'] = tr.rolling(window=3, min_periods=1).mean() # Micro contraction
        
        # 3. Volume Metrics (Dry up & Footprints)
        df['Vol_SMA_50'] = df['Volume'].rolling(window=50, min_periods=1).mean()
        df['Vol_Ratio'] = df['Volume'] / df['Vol_SMA_50']
        # Magnitude Score: How much is today's volume relative to the median of last 20 days?
        df['Vol_Magnitude'] = df['Volume'] / df['Volume'].rolling(window=20).median()
        
        # 4. Range Contraction
        df['Daily_Range_Pct'] = (df['High'] - df['Low']) / df['Close'] * 100
        df['Range_SMA_10'] = df['Daily_Range_Pct'].rolling(window=10, min_periods=1).mean()
        
        # 5. Relative Strength (Alpha)
        # Fetch NIFTYBEES for comparison
        nifty_df = self.fetch_local_history("NIFTYBEES", lookback_days=300)
        if not nifty_df.empty:
            df['RS_Index'] = (df['Close'] / df['Close'].shift(20)) / (nifty_df['Close'] / nifty_df['Close'].shift(20))
        else:
            df['RS_Index'] = 1.0
            
        # 6. Momentum Velocity (Acceleration)
        # 3-day return vs 10-day return ratio
        ret_3 = df['Close'].pct_change(3)
        ret_10 = df['Close'].pct_change(10)
        df['Momentum_Velocity'] = ret_3 / ret_10.abs().replace(0, 0.001)
        
        # 7. Smart Money Accumulation (Rising OI + Price Base)
        df['OI_Change_5d'] = df['OI'].pct_change(periods=5) * 100
        df['Price_Change_5d'] = df['Close'].pct_change(periods=5) * 100
        
        return df


    def detect_vcp_breakout_setup(self, df: pd.DataFrame, current_idx: int = -1) -> dict:
        """
        Evaluates a specific day (default: latest) for a high-probability VCP setup.
        Returns the signal and risk parameters.
        Includes Minervini Stage 2 Trend + Aggregated Options Sentiment.
        """
        if len(df) < 100:
            return {"signal": "INSUFFICIENT_DATA"}
            
        row = df.iloc[current_idx]
        
        # 1. TREND FILTER (Momentum Trend)
        close = row['Close']
        sma50 = row.get('SMA_50')
        sma150 = row.get('SMA_150')
        
        if pd.isna(sma50):
            return {"signal": "HOLD", "reason": "Insufficient Data for SMA"}
            
        # Relaxed Trend Template: Price above 50, and 50 above 150 if available
        is_trending = close > sma50
        if not pd.isna(sma150):
            is_trending = is_trending and (close > sma150)
        
        # 2. RELATIVE STRENGTH / NEAR HIGHS
        # Price should be within 30% of its 52-week high (approx 250 bars)
        lookback_max = df['High'].iloc[max(0, current_idx-250):current_idx+1].max()
        is_near_high = close > (lookback_max * 0.70)
        
        if not is_trending:
            return {"signal": "HOLD", "reason": "Below 50-day SMA (Neutral/Bearish Trend)"}
            
        if not is_near_high:
            return {"signal": "HOLD", "reason": "Not near high probability base (Far from 52w High)"}
            
        # 3. VOLATILITY CONTRACTION (VCP)
        atr_14 = row.get('ATR_14', 1.0)
        atr_3 = row.get('ATR_3', 1.0)
        contraction_ratio = atr_3 / atr_14 if atr_14 > 0 else 1.0
        
        is_contracted = contraction_ratio < 0.90 # More sensitive contraction
        
        # 4. VOLUME DRY UP
        vol_ratio = row['Vol_Ratio']
        is_volume_dry = vol_ratio < 1.1 
        
        # 5. SMART MONEY ACCUMULATION (OI)
        oi_change = row.get('OI_Change_5d', 0)
        is_accumulating = (oi_change > 2.0)
        
        # 6. OPTIONS SENTIMENT (Confirmation)
        pcr = row.get('PCR_OI', 0)
        
        # THE PIVOT POINT (The Trigger)
        recent_10d_high = df['High'].iloc[max(0, current_idx-10):current_idx].max()
        is_breakout = close > recent_10d_high
        is_volume_spike = vol_ratio > 1.2
        body_size_pct = ((close - row['Open']) / row['Open']) * 100
        is_strong_candle = body_size_pct > 1.5
        
        # SCORE CARD (0-10)
        score = 0
        if is_trending: score += 2
        if is_near_high: score += 1
        if is_contracted: score += 2
        if is_volume_dry: score += 1
        if is_accumulating: score += 2
        if pcr > 1.0: score += 1
        if is_breakout: score += 1
        
        if is_breakout and is_volume_spike and is_strong_candle:
            recent_low = df['Low'].iloc[max(0, current_idx-5):current_idx+1].min()
            stop_loss = round(recent_low * 0.98, 2)
            target = round(close * 1.20, 2)
            risk_pct = ((close - stop_loss) / close) * 100
            
            if 0.5 < risk_pct < 10.0 and score >= 5:
                return {
                    "signal": "BUY",
                    "reason": "Explosive VCP Breakout",
                    "entry": close,
                    "stop_loss": stop_loss,
                    "target": target,
                    "risk_pct": risk_pct,
                    "score": score,
                    "pcr": pcr,
                    "oi_acc": oi_change
                }
                 
        return {"signal": "HOLD", "reason": "No Breakout Pattern", "score": score}

    def detect_alpha_momentum_setup(self, df: pd.DataFrame, current_idx: int = -1) -> dict:
        """
        Aggressive "Big Fish" detector.
        Prioritizes:
        1. Extreme Relative Strength (RS)
        2. Institutional Volume Footprints (Magnitudes > 300%)
        3. Momentum Velocity (Vertical moves)
        """
        if len(df) < 30:
            return {"signal": "INSUFFICIENT_DATA"}
            
        row = df.iloc[current_idx]
        close = row['Close']
        
        # 1. High Velocity Filter
        rs_score = row.get('RS_Index', 1.0)
        vol_mag = row.get('Vol_Magnitude', 1.0)
        velocity = row.get('Momentum_Velocity', 0.0)
        
        # 2. ALPHA SCORE CARD (0-100)
        score = 0
        if rs_score > 1.2: score += 30 # Outperforming Nifty by 20%
        if rs_score > 1.5: score += 10 # Extreme RS
        
        if vol_mag > 3.0: score += 30 # 3x Median Volume
        if vol_mag > 5.0: score += 10 # "Sweep" Volume
        
        if velocity > 1.2: score += 20 # Accelerating
        
        # 3. BASE TIGHTNESS (Pause before blast off)
        # 3-day ATR / 14-day ATR < 0.8
        atr_14 = row.get('ATR_14', 1.0)
        atr_3 = row.get('ATR_3', 1.0)
        if (atr_3 / atr_14 if atr_14 > 0 else 1) < 0.85:
            score += 10 # Volatility compression detected
            
        # 4. TRIGGER
        is_rocket = score >= 50
        
        if is_rocket:
            # Targets for Alpha are higher
            # Stop Loss is tighter (Momentum stops)
            low_3d = df['Low'].iloc[max(0, current_idx-3):current_idx+1].min()
            stop_loss = round(low_3d * 0.97, 2)
            target = round(close * 1.30, 2) # Targeting 30% for these rocket stocks
            
            risk_pct = ((close - stop_loss) / close) * 100
            
            return {
                "signal": "ALPHA_BUY",
                "reason": f"Rocket Momentum (Alpha Score: {score})",
                "entry": close,
                "stop_loss": stop_loss,
                "target": target,
                "risk_pct": risk_pct,
                "score": score,
                "rs": rs_score,
                "vol_mag": vol_mag
            }
            
        return {"signal": "HOLD", "reason": "Cooling Down", "score": score}

    def analyze_all_stocks(self):
        """
        Scans all symbols in the DB for the best "Alpha" and "VCP" setups today.
        """
        logger.info("Scanning all stocks for Alpha Momentum (Big Fish)...")
        
        # Get symbols from history_daily_ohlc
        try:
            query = "SELECT DISTINCT symbol FROM history_daily_ohlc"
            symbols = pd.read_sql(query, self.engine)['symbol'].tolist()
        except Exception as e:
            logger.error(f"Error fetching symbols: {e}")
            return

        results = []
        processed = 0
        for symbol in symbols:
            if symbol == "NIFTYBEES": continue # Skip the benchmark
            
            print(f"Scanning {symbol}...", end="\r", flush=True)
            df = self.fetch_local_history(symbol, lookback_days=300)
            if df.empty or len(df) < 50:
                continue
                
            processed += 1
            df = self.calculate_vcp_metrics(df)
            
            # Primary: Alpha Momentum
            alpha_setup = self.detect_alpha_momentum_setup(df)
            
            # Secondary: Classic VCP (Backup)
            vcp_setup = self.detect_vcp_breakout_setup(df)
            
            # Choose the better signal
            if alpha_setup['signal'] == "ALPHA_BUY":
                results.append({
                    "symbol": symbol,
                    "type": "🚀 ALPHA",
                    "score": alpha_setup['score'],
                    "rs": alpha_setup['rs'],
                    "vol_mag": alpha_setup['vol_mag'],
                    "signal": "BUY",
                    "reason": alpha_setup['reason'],
                    "target": alpha_setup['target']
                })
            elif vcp_setup['signal'] == "BUY":
                results.append({
                    "symbol": symbol,
                    "type": "📦 VCP",
                    "score": vcp_setup['score'] * 10, # Normalize classic to 100
                    "rs": df['RS_Index'].iloc[-1],
                    "vol_mag": df['Vol_Magnitude'].iloc[-1],
                    "signal": "BUY",
                    "reason": vcp_setup['reason'],
                    "target": vcp_setup['target']
                })
            else:
                # Still include in rankings for visibility
                results.append({
                    "symbol": symbol,
                    "type": "HOLD",
                    "score": max(alpha_setup['score'], vcp_setup.get('score', 0) * 10),
                    "rs": df['RS_Index'].iloc[-1],
                    "vol_mag": df['Vol_Magnitude'].iloc[-1],
                    "signal": "HOLD",
                    "reason": alpha_setup['reason'] if alpha_setup['score'] >= vcp_setup.get('score', 0) * 10 else vcp_setup['reason'],
                    "target": 0
                })
        
        print("\n")
        logger.info(f"Analyzed {processed}/{len(symbols)} symbols. Found {len([r for r in results if r['signal'] == 'BUY'])} active setups.")
        
        # Print Summary Table
        res_df = pd.DataFrame(results)
        if res_df.empty:
            logger.info("No valid setups found today.")
            return

        # Sort by Alpha Score
        res_df = res_df.sort_values(by="score", ascending=False)
        
        print("\n" + "="*100)
        print(f"� BIG FISH DISCOVERY: {datetime.now().strftime('%Y-%m-%d')}")
        print("="*100)
        # Format columns for readability
        print(res_df.head(50).to_string(index=False, formatters={
            'score': '{:,.0f}'.format,
            'rs': '{:,.2f}'.format,
            'vol_mag': '{:,.2f}'.format,
            'target': '{:,.2f}'.format
        }))
        print("="*100 + "\n")
        return results

    def deep_recon(self, symbols: list):
        """
        Performs real-time deeper dive for a shortlist of stocks.
        """
        if not self._init_dhan():
            logger.error("Deep Recon aborted: Dhan Client not available.")
            return

        print("\n" + "🔍" + "="*98)
        print(f"🕵️ DEEP RECON: LIVE INSTITUTIONAL FOOTPRINT ({datetime.now().strftime('%H:%M:%S')})")
        print("="*100)
        
        recon_data = []
        for symbol in symbols:
            print(f"Analyzing {symbol}...", end="\r", flush=True)
            
            # 1. Get Live Quote
            quote = self.dhan.get_bulk_quotes([symbol])
            if not quote or symbol not in quote:
                continue
            
            ltp = quote[symbol].get('last_price') or quote[symbol].get('lastPrice') or 0.0
            v_change = quote[symbol].get('volume') or quote[symbol].get('volume_traded') or 0
            
            # 2. Check for Options Support and OI Chain
            expiry = self.dhan.get_active_expiry(symbol)
            sid = self.dhan.get_security_id(symbol)
            
            # Resolve segment from meta
            meta = self.dhan._meta_map.get(symbol, {})
            segment = meta.get('segment', 'NSE_EQ')
            
            fno_sentiment = "N/A (CASH ONLY)"
            pcr = 0.0
            
            if expiry and sid:
                # Fetch Option Chain
                chain = self.dhan.get_option_chain(sid, segment, expiry)
                if chain and isinstance(chain, dict):
                    # Handle Dhan v2 nested structure: {"last_price": ..., "oc": {...}}
                    oc_data = chain.get('oc') if 'oc' in chain else chain
                    
                    if isinstance(oc_data, dict):
                        total_call_oi = 0
                        total_put_oi = 0
                        
                        for strike, data in oc_data.items():
                            if isinstance(data, dict):
                                total_call_oi += data.get('ce', {}).get('oi', 0)
                                total_put_oi += data.get('pe', {}).get('oi', 0)
                        
                        if total_call_oi > 0:
                            pcr = total_put_oi / total_call_oi
                            
                            # Sentiment logic
                            if pcr > 1.2: fno_sentiment = "BULLISH (Put Writing)"
                            elif pcr < 0.7: fno_sentiment = "BEARISH (Call Writing)"
                            else: fno_sentiment = "NEUTRAL"
                        else:
                            fno_sentiment = "NO_OI_DATA"
                    elif isinstance(oc_data, list):
                        # Fallback for list format
                        total_call_oi = sum(s.get('occeOI') or s.get('ce_oi') or 0 for s in oc_data if isinstance(s, dict))
                        total_put_oi = sum(s.get('ocpeOI') or s.get('pe_oi') or 0 for s in oc_data if isinstance(s, dict))
                        if total_call_oi > 0: pcr = total_put_oi / total_call_oi
                        if pcr > 1.2: fno_sentiment = "BULLISH"
                else:
                    fno_sentiment = "NO_CHAIN_DATA"
            
            # 3. Conviction Score
            # High Volume + High RS + Bullish PCR = 🔥 Sureshot
            conviction = "MEDIUM"
            if ltp > quote[symbol].get('ohlc', {}).get('close', 0) * 1.02:
                conviction = "🔥 HIGH"
            if fno_sentiment == "BULLISH (Put Writing)":
                conviction = "🚀 SURESHOT"

            recon_data.append({
                "symbol": symbol,
                "ltp": ltp,
                "fno_sentiment": fno_sentiment,
                "pcr": round(pcr, 2),
                "vol_magnitude": "HIGH" if v_change > 1000000 else "NORMAL",
                "conviction": conviction,
                "action": "BUY ON DIP" if ltp < quote[symbol].get('ohlc', {}).get('close', 0) else "EXPLOSIVE BUY"
            })

            # Persistence
            try:
                session = self.SessionLocal()
                # Check for existing signal for today
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                existing = session.query(AlphaSignal).filter(
                    AlphaSignal.symbol == symbol,
                    AlphaSignal.signal_date >= today
                ).first()
                
                if not existing:
                    alpha_sig = AlphaSignal(
                        symbol=symbol,
                        signal_date=datetime.now(),
                        price=ltp,
                        pcr=pcr,
                        sentiment=fno_sentiment,
                        status="ACTIVE"
                    )
                    session.add(alpha_sig)
                    session.commit()
                session.close()
            except Exception as db_e:
                logger.error(f"Failed to persist Alpha signal for {symbol}: {db_e}")
            
        res_df = pd.DataFrame(recon_data)
        if not res_df.empty:
            print(res_df.to_string(index=False))
        else:
            print("No live data found for the shortlist.")
        print("="*100 + "\n")

    def run_backtest(self, symbol: str, df: pd.DataFrame):
        """
        Simulates trading the VCP strategy over the historical dataset.
        """
        logger.info(f"Running Backtest for {symbol} ({len(df)} days)...")
        
        trades = []
        in_trade = False
        entry_price = 0
        target = 0
        stop_loss = 0
        entry_date = None
        
        for i in range(50, len(df)):
            row = df.iloc[i]
            current_date = df.index[i]
            
            # 1. Check open trade conditions
            if in_trade:
                high = row['High']
                low = row['Low']
                
                # Check for Target Hit
                if high >= target:
                    days_held = (current_date - entry_date).days
                    trades.append({
                        "entry_date": entry_date,
                        "exit_date": current_date,
                        "outcome": "WIN",
                        "pnl_pct": 20.0,
                        "days_held": days_held
                    })
                    in_trade = False
                    continue
                    
                # Check for Stop Loss Hit
                if low <= stop_loss:
                    days_held = (current_date - entry_date).days
                    loss_pct = ((entry_price - stop_loss) / entry_price) * 100
                    trades.append({
                        "entry_date": entry_date,
                        "exit_date": current_date,
                        "outcome": "LOSS",
                        "pnl_pct": -loss_pct,
                        "days_held": days_held
                    })
                    in_trade = False
                    continue
                
                # Time Stop (Close after 20 trading days if neither hit)
                if (current_date - entry_date).days > 28: # ~20 trading days
                    days_held = (current_date - entry_date).days
                    pnl_pct = ((row['Close'] - entry_price) / entry_price) * 100
                    outcome = "WIN" if pnl_pct > 0 else "LOSS"
                    trades.append({
                        "entry_date": entry_date,
                        "exit_date": current_date,
                        "outcome": f"TIME_{outcome}",
                        "pnl_pct": pnl_pct,
                        "days_held": days_held
                    })
                    in_trade = False
                    continue
                    
            # 2. Look for new entries if not in trade
        if not in_trade:
            # Check for Alpha Momentum first (Big Fish)
            alpha_setup = self.detect_alpha_momentum_setup(df, current_idx=i)
            vcp_setup = self.detect_vcp_breakout_setup(df, current_idx=i)
            
            setup = alpha_setup if alpha_setup['signal'] == 'ALPHA_BUY' else vcp_setup
            
            if setup['signal'] in ['BUY', 'ALPHA_BUY']:
                in_trade = True
                entry_price = setup['entry']
                target = setup['target']
                stop_loss = setup['stop_loss']
                entry_date = current_date
        
        # Finalize open trades if any
        if in_trade:
            last_row = df.iloc[-1]
            pnl_pct = ((last_row['Close'] - entry_price) / entry_price) * 100
            trades.append({
                "entry_date": entry_date,
                "exit_date": df.index[-1],
                "outcome": "OPEN",
                "pnl_pct": pnl_pct,
                "days_held": (df.index[-1] - entry_date).days
            })
        
        # Process Results
        if not trades:
            logger.warning(f"No trades triggered for {symbol} in the dataset.")
            return
            
        wins = [t for t in trades if t['pnl_pct'] > 0]
        losses = [t for t in trades if t['pnl_pct'] <= 0]
        
        win_rate = (len(wins) / len(trades)) * 100
        avg_win = sum([t['pnl_pct'] for t in wins]) / len(wins) if wins else 0
        avg_loss = sum([t['pnl_pct'] for t in losses]) / len(losses) if losses else 0
        avg_days = sum([t['days_held'] for t in trades]) / len(trades)
        
        print("\n" + "="*50)
        print(f"📊 BACKTEST RESULTS: {symbol}")
        print("="*50)
        print(f"Total Trades:      {len(trades)}")
        print(f"Win Rate:          {win_rate:.1f}%")
        print(f"Avg Return/Trade:  {sum([t['pnl_pct'] for t in trades]) / len(trades):.2f}%")
        print(f"Avg Winning Trade: +{avg_win:.2f}%")
        print(f"Avg Losing Trade:  {avg_loss:.2f}%")
        print(f"Avg Days Held:     {avg_days:.1f} days")
        print("="*50)
        for t in trades:
            res = "🟢 WIN" if t['pnl_pct'] > 0 else "🔴 LOSS"
            if t['outcome'] == 'OPEN': res = "🔵 OPEN"
            print(f"[{t['exit_date'].strftime('%Y-%m-%d')}] {res} : {t['pnl_pct']:.2f}% (Held: {t['days_held']}d)")
        print("="*50 + "\n")

    def run_analysis(self, symbol: str):
        """Analyzes the latest day for a live trading recommendation."""
        df = self.fetch_local_history(symbol)
        if df.empty:
            logger.error(f"No local database history found for {symbol}.")
            return
            
        df = self.calculate_vcp_metrics(df)
        
        # Analyze the most recent completed day
        setup = self.detect_vcp_breakout_setup(df, current_idx=-1)
        
        print("\n" + "="*50)
        print(f"🎯 SWING TRADING ANALYZER: {symbol}")
        print("="*50)
        
        if setup['signal'] == 'BUY':
            print(f"✅ ACTION: STRONG BUY")
            print(f"Rationale: {setup['reason']}")
            print(f"Current Price: ₹{setup['entry']}")
            print(f"Stop Loss:     ₹{setup['stop_loss']} (Risk: {setup['risk_pct']:.1f}%)")
            print(f"Target (20%):  ₹{setup['target']}")
            if setup.get('oi_accumulation', 0) > 0:
                print(f"Smart Money:  OI increased {setup['oi_accumulation']:.1f}% recently.")
        else:
            print(f"⏸️ ACTION: {setup['signal']}")
            print(f"Rationale: {setup.get('reason', 'N/A')}")
            print(f"Current Price: ₹{df['Close'].iloc[-1]}")
            
        print("="*50 + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, help="Stock symbol to analyze")
    parser.add_argument("--backtest", action="store_true", help="Run 6-month historical backtest")
    
    parser.add_argument("--analyze", action="store_true", help="Scan all stocks for current opportunities.")
    parser.add_argument("--recon", action="store_true", help="Perform real-time deep dive for top-ranked symbols.")
    
    args = parser.parse_args()
    
    analyzer = HistoricalSwingAnalyzer()
    
    if args.analyze:
        results = analyzer.analyze_all_stocks()
        # Automatically trigger Recon for Top 5 Alpha candidates if they are active buys
        top_candidates = [r['symbol'] for r in results if r['signal'] == 'BUY'][:5]
        if top_candidates:
            print(f"\n🚀 TOP {len(top_candidates)} ALPHA CANDIDATES IDENTIFIED. TRIGGERING DEEP RECON...")
            analyzer.deep_recon(top_candidates)
            
    elif args.recon:
        # Manual recon for a specific symbol if provided, or from a default list
        target = args.symbol if args.symbol != "ALL" else ["GODFRYPHLP", "ABB", "TARIL"]
        analyzer.deep_recon([target] if isinstance(target, str) else target)
    elif args.backtest:
        df = analyzer.fetch_local_history(args.symbol, lookback_days=365)
        if not df.empty:
            df = analyzer.calculate_vcp_metrics(df)
            analyzer.run_backtest(args.symbol, df)
        else:
            logger.error(f"Could not run backtest for {args.symbol}. Data fetch failed.")
    else:
        # Default single analysis
        df = analyzer.fetch_local_history(args.symbol, lookback_days=300)
        if not df.empty:
            df = analyzer.calculate_vcp_metrics(df)
            setup = analyzer.detect_vcp_breakout_setup(df)
            print(f"\nAnalysis for {args.symbol}:")
            print(f"Signal: {setup['signal']}")
            print(f"Reason: {setup['reason']}")
            if setup['signal'] == "BUY":
                print(f"Target: {setup['target']} (+20%)")
                print(f"Stop:   {setup['stop_loss']} (-{setup['risk_pct']:.2f}%)")
        else:
            logger.error(f"Could not analyze {args.symbol}. Data fetch failed.")
