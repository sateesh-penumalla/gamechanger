import os
import pandas as pd
import numpy as np
import json
import pytz
from datetime import datetime, time, timedelta
from dotenv import load_dotenv
from src.data.dhan_client import DhanDataClient
from src.data.yfinance_client import YahooFinanceData
from src.agents.globalist import GlobalistAgent
from src.agents.sector_general import SectorGeneralAgent
from src.agents.newsroom import NewsroomAgent

# Load environment variables for database access
load_dotenv()

def migrate_db(engine):
    """Ensures ORB_window column exists in tickers table."""
    from sqlalchemy import text
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE tickers ADD COLUMN ORB_window INTEGER"))
            conn.commit()
        except Exception:
            pass # Column likely exists
    
    # Ensure new tables (like IntradayTick) are created
    from src.db.schema import Base
    Base.metadata.create_all(engine)

def run_mimic_session(preset_name, risk_params, blacklist=None, whitelist=None, mode='REPLAY', trade_date=None, require_sniper=False, htf_params=None, orb_duration=15, ignore_htf=False, range_filter_mode='STANDARD', execution_boundary='STANDARD', use_api=False):
    """
    Executes a high-fidelity mimic session.
    Returns: (df_executed, df_skipped, df_orb_summary)
    """
    # Dynamic ORB calculation
    orb_start_time = "09:15"
    orb_end_minutes = 15 + orb_duration # Starts at 09:15, so 15m duration hits 09:30
    
    # Calculate end time string
    base_time = datetime.strptime("09:15", "%H:%M")
    end_time_dt = base_time + timedelta(minutes=orb_duration)
    orb_end_time = end_time_dt.strftime("%H:%M")
    session_start_time = (end_time_dt + timedelta(minutes=1)).strftime("%H:%M")
    if blacklist is None: blacklist = []
    
    # 0. Initialize Macro Agents
    yf_client = YahooFinanceData()
    globalist = GlobalistAgent(yf_client)
    sector_agent = SectorGeneralAgent(yf_client)
    news_agent = NewsroomAgent()
    
    # Fetch Market Mood early (only if toggled)
    market_mood = None
    if risk_params.get('use_market_filter'):
        market_mood = globalist.analyze_market_mood(trade_date=trade_date)
    
    # 1. Load Preset
    with open("src/config/strategy_presets.json", "r") as f:
        presets = json.load(f)
    p = presets[preset_name]
    
    # 2. Timing Bounds
    strat_start = p['start'][:5] # HH:MM
    strat_end = p['end'][:5]     # HH:MM
    
    # Final detection window: max(ORB_End+1, Strat_Start) to min(15:20, Strat_End)
    effective_start = max(session_start_time, strat_start)
    effective_end = min('15:25', strat_end)
        
    # --- EXECUTION SIMULATION ---
    cache_dir = "data/sniper_cache"
    executed_trades = []
    skipped_signals = []
    processed_symbols = set()
    orb_summary = []
    sniper_info = {}
    sniper_symbols = None

    # --- MODE: REPLAY (Historical Date Session) ---
    if mode == 'REPLAY':
        from sqlalchemy import create_engine, text
        date_str = trade_date.strftime('%Y-%m-%d') if hasattr(trade_date, 'strftime') else str(trade_date)
        history_dir = f"data/history/{date_str}"
        
        if not os.path.exists(history_dir):
            return pd.DataFrame(), pd.DataFrame([{"reason": f"No history for {date_str}"}]), pd.DataFrame()

        # Oracle & HTF Data Fetching
        htf_data = {}
        sniper_symbols = None
        db_url = os.getenv("DATABASE_URL")
        
        if db_url:
            try:
                engine = create_engine(db_url)
                with engine.connect() as conn:
                    # Fetch oracle status for symbols
                    res = conn.execute(text("SELECT symbol, oracle_status, avg_daily_turnover FROM history_testing WHERE trade_date = :d"), {"d": date_str})
                    results = res.fetchall()
                    sniper_info = {r[0]: r[1] for r in results}
                    adtv_cache = {r[0]: r[2] for r in results}
                    
                    if require_sniper:
                        sniper_symbols = [s for s, status in sniper_info.items() if status in ['UP_SNIPER', 'DOWN_SNIPER']]
                    
                    # Fetch Weekly RSI and SMA for all symbols in the session
                    res_htf = conn.execute(text("SELECT symbol, weekly_rsi, weekly_sma FROM history_testing WHERE trade_date = :d"), {"d": date_str})
                    for r in res_htf:
                        htf_data[r[0]] = {
                            "weekly_rsi": r[1], 
                            "weekly_sma": r[2],
                            "adtv_cr": adtv_cache.get(r[0], 0)
                        }
            except Exception as e:
                print(f"HTF Sync Error: {e}")
        else:
            print("HTF Sync Warning: DATABASE_URL not set. Skipping institutional filters.")

        files = [f for f in os.listdir(history_dir) if f.endswith('.csv')]
        symbols = [f.split('.')[0] for f in files]
        
        if whitelist:
            symbols = [s for s in symbols if s in whitelist]
        if sniper_symbols is not None:
            symbols = [s for s in symbols if s in sniper_symbols]

        for symbol in symbols:
            if symbol in blacklist: continue
            if risk_params.get('single_trade') and symbol in processed_symbols: continue

            filepath = os.path.join(history_dir, f"{symbol}.csv")
            df = pd.read_csv(filepath)
            
            # Standardize index
            ts_col = 'Datetime' if 'Datetime' in df.columns else 'timestamp'
            if ts_col in df.columns:
                df.set_index(ts_col, inplace=True)
            df.index = pd.to_datetime(df.index)
            if df.empty: continue
            if df.index.tz is None:
                df.index = df.index.tz_localize('Asia/Kolkata', ambiguous='infer')
            else:
                df.index = df.index.tz_convert('Asia/Kolkata')

            df = calculate_indicators(df)
            early_data = df.between_time(orb_start_time, orb_end_time)
            if early_data.empty: continue
            
            orb_h_std, orb_l_std = early_data['High'].max(), early_data['Low'].min()
            bh, bl = early_data[['Open', 'Close']].max(axis=1), early_data[['Open', 'Close']].min(axis=1)
            orb_h_cln, orb_l_cln = bh.max(), bl.min()

            # Decoupled Boundary Selection
            # 1. Selection for Entry/SL levels (Execution)
            if execution_boundary == 'CLEAN':
                orb_h, orb_l = orb_h_cln, orb_l_cln
            else:
                orb_h, orb_l = orb_h_std, orb_l_std

            rd_pct = (early_data.iloc[-1]['Close'] - early_data.iloc[0]['Open']) / early_data.iloc[0]['Open']
            range_dir = "BULLISH" if rd_pct > 0.0005 else ("BEARISH" if rd_pct < -0.0005 else "NEUTRAL")

            r_pct = round(((orb_h_std - orb_l_std) / orb_l_std) * 100, 2)
            r_pct_cln = round(((orb_h_cln - orb_l_cln) / orb_l_cln) * 100, 2)

            orb_summary.append({
                "symbol": symbol, "orb_h_std": orb_h_std, "orb_l_std": orb_l_std,
                "orb_h_cln": orb_h_cln, "orb_l_cln": orb_l_cln,
                "range_pct": r_pct,
                "range_pct_clean": r_pct_cln,
                "direction": range_dir,
                "oracle_status": sniper_info.get(symbol, 'N/A')
            })

            h_metrics = htf_data.get(symbol, {})

            # 3. Define Detection Window
            session_data = df.between_time(effective_start, effective_end)
            in_bo, in_bd = False, False
            for ts, row in session_data.iterrows():
                if row['Close'] < (orb_h * 0.998): in_bo = False
                if row['Close'] > (orb_l * 1.002): in_bd = False

                if row['High'] > orb_h and not in_bo:
                    is_valid, reasons, macro = meets_filters(row, 'LONG', p, orb_h_std, orb_l_std, range_dir, audit=True, risk_params=risk_params, htf_metrics=h_metrics, ignore_htf=ignore_htf, orb_h_cln=orb_h_cln, orb_l_cln=orb_l_cln, range_filter_mode=range_filter_mode, globalist=globalist, sector_agent=sector_agent, news_agent=news_agent, symbol=symbol, trade_date=trade_date, market_mood=market_mood, oracle_status=sniper_info.get(symbol, 'N/A'))
                    if is_valid:
                        candle_range = row['High'] - row['Low']
                        vqs = (row['Close'] - row['Low']) / candle_range if candle_range > 0 else 1.0
                        entry_metrics = {
                            "rsi": round(row['RSI'], 1), "macd": round(row['MACD'], 3),
                            "vol_surge": round(row['Vol_Surge'], 2), "slope": round(row['Slope'], 4),
                            "vqs": round(vqs, 2),
                            "weekly_rsi": h_metrics.get('weekly_rsi'),
                            "weekly_sma": h_metrics.get('weekly_sma'),
                            "range_pct": r_pct,
                            "range_pct_clean": r_pct_cln,
                            "boundary": p['boundary'], "orb_h_std": orb_h_std, "orb_l_std": orb_l_std,
                            "orb_h_cln": orb_h_cln, "orb_l_cln": orb_l_cln,
                            **macro
                        }
                        trades = simulate_trade_management(symbol, 'LONG', df, ts, row['Close'], orb_h, orb_l, range_dir, risk_params, entry_metrics=entry_metrics, globalist=globalist, sector_agent=sector_agent, trade_date=trade_date, oracle_status=sniper_info.get(symbol, 'N/A'))
                        if trades:
                            executed_trades.extend(trades)
                            processed_symbols.add(symbol); in_bo = True; break
                    else:
                        skipped_signals.append({"symbol": symbol, "time": ts.strftime('%H:%M'), "side": 'LONG', "oracle_status": sniper_info.get(symbol, 'N/A'), "reason": ", ".join(reasons)})

                if symbol not in processed_symbols and row['Low'] < orb_l and not in_bd:
                    is_valid, reasons, macro = meets_filters(row, 'SHORT', p, orb_h_std, orb_l_std, range_dir, audit=True, risk_params=risk_params, htf_metrics=h_metrics, ignore_htf=ignore_htf, orb_h_cln=orb_h_cln, orb_l_cln=orb_l_cln, range_filter_mode=range_filter_mode, globalist=globalist, sector_agent=sector_agent, news_agent=news_agent, symbol=symbol, trade_date=trade_date, market_mood=market_mood, oracle_status=sniper_info.get(symbol, 'N/A'))
                    if is_valid:
                        candle_range = row['High'] - row['Low']
                        vqs = (row['High'] - row['Close']) / candle_range if candle_range > 0 else 1.0
                        entry_metrics = {
                            "rsi": round(row['RSI'], 1), "macd": round(row['MACD'], 3),
                            "vol_surge": round(row['Vol_Surge'], 2), "slope": round(row['Slope'], 4),
                            "vqs": round(vqs, 2),
                            "weekly_rsi": h_metrics.get('weekly_rsi'),
                            "weekly_sma": h_metrics.get('weekly_sma'),
                            "range_pct": r_pct,
                            "range_pct_clean": r_pct_cln,
                            "boundary": p['boundary'], "orb_h_std": orb_h_std, "orb_l_std": orb_l_std,
                            "orb_h_cln": orb_h_cln, "orb_l_cln": orb_l_cln,
                            **macro
                        }
                        trades = simulate_trade_management(symbol, 'SHORT', df, ts, row['Close'], orb_h, orb_l, range_dir, risk_params, entry_metrics=entry_metrics, globalist=globalist, sector_agent=sector_agent, trade_date=trade_date, oracle_status=sniper_info.get(symbol, 'N/A'))
                        if trades:
                            executed_trades.extend(trades)
                            processed_symbols.add(symbol); in_bd = True; break
                    else:
                        skipped_signals.append({"symbol": symbol, "time": ts.strftime('%H:%M'), "side": 'SHORT', "oracle_status": sniper_info.get(symbol, 'N/A'), "reason": ", ".join(reasons)})

        return pd.DataFrame(executed_trades), pd.DataFrame(skipped_signals), pd.DataFrame(orb_summary)

    # --- MODE: LIVE (Real-time Mirror) ---
    else: 
        from sqlalchemy import create_engine, text
        from src.db.schema import Ticker, IntradayTick, Base
        from sqlalchemy.orm import sessionmaker
        
        db_url = os.getenv("DATABASE_URL")
        engine = create_engine(db_url)
        migrate_db(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            if require_sniper:
                res = session.query(Ticker.symbol, Ticker.oracle_status).filter(Ticker.oracle_status.in_(['UP_SNIPER', 'DOWN_SNIPER'])).all()
            else:
                res = session.query(Ticker.symbol, Ticker.oracle_status).all()
            
            sniper_info = {r[0]: r[1] for r in res}
            symbols = list(sniper_info.keys())
        except Exception as e:
            print(f"DB Fetch Error in LIVE mode: {e}")
            symbols = [f.split('_')[0] for f in os.listdir(cache_dir) if f.endswith('.csv')]
            
        if whitelist:
            symbols = [s for s in whitelist if s in symbols]

        # --- PRE-FILTER SYMBOLS BY PERSISTENT ORB RANGE ---
        # Load Strategy Preset for Range Filtering
        presets = {}
        if os.path.exists("src/config/strategy_presets.json"):
            with open("src/config/strategy_presets.json", "r") as f:
                presets = json.load(f)
        
        p_preset = presets.get(preset_name, {})
        r_min, r_max = p_preset.get('range_pct', [0, 100])
        
        symbols_to_fetch = symbols
        if use_api:
            filtered_symbols = []
            print(f"Applying Range Filtering ({r_min}% - {r_max}%) before bulk fetch...")
            for sym in symbols:
                ticker_obj = session.query(Ticker).filter(Ticker.symbol == sym).first()
                if ticker_obj and ticker_obj.ORB_range_pct is not None:
                    # Check if updated today
                    ist_tz = pytz.timezone('Asia/Kolkata')
                    ist_now_dt = datetime.now(ist_tz)
                    if ticker_obj.last_updated and ticker_obj.last_updated.date() == ist_now_dt.date():
                        if ticker_obj.ORB_window == orb_duration:
                            if not (r_min <= ticker_obj.ORB_range_pct <= r_max):
                                # Skip fetching intraday bars for this stock
                                continue
                filtered_symbols.append(sym)
            
            symbols_to_fetch = filtered_symbols
            print(f"Filtered watchlist from {len(symbols)} to {len(symbols_to_fetch)} symbols based on persistent ORB range.")

        # --- BULK DATA FETCH ---
        all_data = {}
        if use_api:
            try:
                cid = os.getenv("DHAN_CLIENT_ID")
                token = os.getenv("DHAN_ACCESS_TOKEN")
                if cid and token:
                    data_client = DhanDataClient(cid, token)
                    # Find last ticks for all symbols in ONE bulk query
                    print(f"Calculating deltas for {len(symbols_to_fetch)} symbols (Bulk Indexing)...")
                    from_dates = {}
                    ist_tz = pytz.timezone('Asia/Kolkata')
                    today_ist = datetime.now(ist_tz).date()

                    from sqlalchemy import func
                    last_ticks_query = session.query(
                        IntradayTick.symbol, 
                        func.max(IntradayTick.timestamp).label('last_timestamp')
                    ).filter(IntradayTick.symbol.in_(symbols_to_fetch)).group_by(IntradayTick.symbol).all()
                    
                    last_ticks_map = {row.symbol: row.last_timestamp for row in last_ticks_query}
                    
                    for sym in symbols_to_fetch:
                        last_ts = last_ticks_map.get(sym)
                        if last_ts:
                            if last_ts.date() == today_ist:
                                from_dates[sym] = last_ts.strftime("%Y-%m-%d")
                            else:
                                from_dates[sym] = today_ist.strftime("%Y-%m-%d")
                        else:
                            sev_days_ago = (datetime.now(ist_tz) - timedelta(days=7)).date()
                            from_dates[sym] = sev_days_ago.strftime("%Y-%m-%d")

                    # --- LAZY FETCH OPTIMIZATION ---
                    # Filter out symbols that were already updated in the last 60 seconds (likely by background service)
                    now_ist = datetime.now(ist_tz)
                    lazy_fetch_list = []
                    for sym in symbols_to_fetch:
                        last_ts = last_ticks_map.get(sym)
                        # Add timezone awareness to last_ts for comparison if it's naive
                        if last_ts and last_ts.tzinfo is None:
                            last_ts = ist_tz.localize(last_ts)
                        
                        if last_ts and (now_ist - last_ts).total_seconds() < 60:
                            # Skip API call, DB is already fresh enough
                            continue
                        lazy_fetch_list.append(sym)
                    
                    if len(lazy_fetch_list) < len(symbols_to_fetch):
                        print(f"Lazy Fetch: Skipping {len(symbols_to_fetch) - len(lazy_fetch_list)} symbols already fresh in DB.")
                    
                    if lazy_fetch_list:
                        print(f"Fetching real-time data for {len(lazy_fetch_list)} symbols in parallel (Optimized Payload)...")
                        api_results = data_client.fetch_bulk_intraday(lazy_fetch_list, interval="1m", from_dates=from_dates)
                    else:
                        api_results = {}
                        print("Lazy Fetch: All symbols are fresh. Skipping API fetch cluster.")
                    
                    # Store ticks in DB - Optimized Bulk Sync
                    print(f"Syncing {len(api_results)} symbols to database...")
                    all_tick_objects = []
                    for sym, df_ticks in api_results.items():
                        for ts, row in df_ticks.iterrows():
                            all_tick_objects.append({
                                "symbol": sym,
                                "timestamp": ts,
                                "open": float(row['Open']),
                                "high": float(row['High']),
                                "low": float(row['Low']),
                                "close": float(row['Close']),
                                "volume": int(row['Volume'])
                            })
                    
                    if all_tick_objects:
                        try:
                            # Use bulk insert with "ON DUPLICATE KEY UPDATE" for MySQL performance
                            from sqlalchemy.dialects.mysql import insert
                            stmt = insert(IntradayTick).values(all_tick_objects)
                            upsert_stmt = stmt.on_duplicate_key_update(
                                open=stmt.inserted.open,
                                high=stmt.inserted.high,
                                low=stmt.inserted.low,
                                close=stmt.inserted.close,
                                volume=stmt.inserted.volume
                            )
                            session.execute(upsert_stmt)
                            session.commit()
                            print(f"Database sync complete. Total ticks synced: {len(all_tick_objects)}")
                        except Exception as e:
                            print(f"DB Bulk Sync Error: {e}")
                            session.rollback()
                else:
                    print(f"Dhan Credentials missing. Initializing fallback.")
            except Exception as e:
                print(f"Bulk Fetch Error: {e}")
        
        for symbol in symbols:
            h_metrics = {} 
            if symbol in blacklist: continue
            if risk_params.get('single_trade') and symbol in processed_symbols: continue

            # Fetch Live HTF Data (ADTV)
            h_metrics = {}
            with engine.connect() as conn:
                res = conn.execute(text("SELECT weekly_rsi, weekly_sma, avg_daily_turnover FROM tickers WHERE symbol = :s"), {"s": symbol}).fetchone()
                if res:
                    h_metrics = {
                        "weekly_rsi": res[0],
                        "weekly_sma": res[1],
                        "adtv_cr": res[2]
                    }

            # --- PERSISTENT ORB CHECK ---
            ticker_obj = session.query(Ticker).filter(Ticker.symbol == symbol).first()
            reuse_orb = False
            if ticker_obj and ticker_obj.ORB_high is not None and ticker_obj.ORB_window == orb_duration:
                # Check if updated today (IST)
                ist_tz = pytz.timezone('Asia/Kolkata')
                ist_now_dt = datetime.now(ist_tz)
                last_upd = ticker_obj.last_updated
                
                if last_upd and last_upd.date() == ist_now_dt.date():
                    # --- NEW: FINAL ORB REFRESH LOGIC ---
                    # If current time is past 9:31 AM and the ORB was saved before 9:31 AM, 
                    # it means it was a "partial" ORB. We force a final recalculation.
                    cutoff_time = time(9, 31)
                    last_upd_time = last_upd.time()
                    curr_time = ist_now_dt.time()
                    
                    if curr_time >= cutoff_time and last_upd_time < cutoff_time:
                        print(f"Force-refreshing ORB for {symbol} (Partial snapshot detected from {last_upd_time})")
                        reuse_orb = False
                    else:
                        reuse_orb = True
                        orb_h_std = ticker_obj.ORB_high
                        orb_l_std = ticker_obj.ORB_low
                        orb_h_cln = ticker_obj.ORB_high_clean
                        orb_l_cln = ticker_obj.ORB_low_clean
                        range_dir = ticker_obj.ORB_direction
                        r_pct = ticker_obj.ORB_range_pct
                        r_pct_cln = round(((orb_h_cln - orb_l_cln) / orb_l_cln) * 100, 2) if orb_l_cln else 0
                        
                        # Aggregated summary for UI even if we don't fetch full intraday data
                        orb_summary.append({
                            "symbol": symbol,
                            "orb_h_std": orb_h_std, "orb_l_std": orb_l_std,
                            "orb_h_cln": orb_h_cln, "orb_l_cln": orb_l_cln,
                            "range_pct": r_pct,
                            "range_pct_clean": r_pct_cln,
                            "direction": range_dir,
                            "oracle_status": sniper_info.get(symbol, 'N/A')
                        })
                        print(f"Reusing stored ORB for {symbol} ({orb_duration}m)")

            # --- LOAD DATA (DB-Centric) ---
            df = None
            if use_api:
                ist_tz = pytz.timezone('Asia/Kolkata')
                today_ist = datetime.now(ist_tz).date()
                
                # Query IntradayTick for context (last 500 bars)
                db_ticks = session.query(IntradayTick).filter(
                    IntradayTick.symbol == symbol
                ).order_by(IntradayTick.timestamp.desc()).limit(500).all()
                db_ticks.reverse() # Chronological order
                
                if db_ticks:
                    df = pd.DataFrame([{
                        'Datetime': t.timestamp,
                        'Open': t.open, 'High': t.high, 'Low': t.low, 'Close': t.close, 'Volume': t.volume
                    } for t in db_ticks])
                    if not df.empty:
                        df.set_index('Datetime', inplace=True)
                        if df.index.tz is None:
                            df.index = df.index.tz_localize('Asia/Kolkata', ambiguous='infer')
                        else:
                            df.index = df.index.tz_convert('Asia/Kolkata')

            if df is None or df.empty:
                filepath = os.path.join(cache_dir, f"{symbol}_1d_1m.csv")
                if os.path.exists(filepath):
                    df = pd.read_csv(filepath, index_col=0, parse_dates=True)
                    if df is not None and not df.empty and df.index.tz is None:
                        df.index = df.index.tz_localize('Asia/Kolkata', ambiguous='infer')

            if df is None or df.empty: continue
            
            # 1. Prepare Indicators (on full 500-bar context)
            df = calculate_indicators(df)
            
            # STRICT TODAY FILTER for results/ORB:
            ist_tz = pytz.timezone('Asia/Kolkata')
            today_ist = datetime.now(ist_tz).date()
            df_today = df[df.index.date == today_ist].copy()
            
            # If no data for today yet, we can't do ORB/Signals
            if df_today.empty:
                print(f"Skipping {symbol}: No data for today {today_ist} in DB yet.")
                continue

            # Always define early_data for metrics/signals even if reusing ORB
            early_data = df_today.between_time(orb_start_time, orb_end_time)
            
            if not reuse_orb:
                # 2. Define Opening Range (Same as calculate_orb.py)
                if early_data.empty: continue
                
                # Standard (Wick-based)
                orb_h_std, orb_l_std = early_data['High'].max(), early_data['Low'].min()
                
                # Clean (Body-based)
                bh, bl = early_data[['Open', 'Close']].max(axis=1), early_data[['Open', 'Close']].min(axis=1)
                orb_h_cln, orb_l_cln = bh.max(), bl.min()
                
                rd_pct = (early_data.iloc[-1]['Close'] - early_data.iloc[0]['Open']) / early_data.iloc[0]['Open']
                range_dir = "BULLISH" if rd_pct > 0.0005 else ("BEARISH" if rd_pct < -0.0005 else "NEUTRAL")

                r_pct = round(((orb_h_std - orb_l_std) / orb_l_std) * 100, 2)
                r_pct_cln = round(((orb_h_cln - orb_l_cln) / orb_l_cln) * 100, 2)

                # SAVE TO DB
                if ticker_obj:
                    ticker_obj.ORB_high = float(orb_h_std)
                    ticker_obj.ORB_low = float(orb_l_std)
                    ticker_obj.ORB_high_clean = float(orb_h_cln)
                    ticker_obj.ORB_low_clean = float(orb_l_cln)
                    ticker_obj.ORB_direction = range_dir
                    ticker_obj.ORB_range_pct = float(r_pct)
                    ticker_obj.ORB_window = orb_duration
                    ticker_obj.last_updated = datetime.now()
                    session.commit()
                    print(f"Saved new ORB for {symbol} ({orb_duration}m)")

            # Active ORB for filters
            # Sidebar Override for Execution takes precedence
            if execution_boundary == 'CLEAN':
                orb_h, orb_l = orb_h_cln, orb_l_cln
            else:
                orb_h, orb_l = orb_h_std, orb_l_std
            
            # Re-calculate or reuse range metrics
            if not reuse_orb:
                if not early_data.empty:
                    rd_pct = (early_data.iloc[-1]['Close'] - early_data.iloc[0]['Open']) / early_data.iloc[0]['Open']
                    range_dir = "BULLISH" if rd_pct > 0.0005 else ("BEARISH" if rd_pct < -0.0005 else "NEUTRAL")
                r_pct = round(((orb_h_std - orb_l_std) / orb_l_std) * 100, 2)
                r_pct_cln = round(((orb_h_cln - orb_l_cln) / orb_l_cln) * 100, 2)

            # Aggregate ORB Summary (Only if not already added via reuse_orb)
            if not any(d['symbol'] == symbol for d in orb_summary):
                orb_summary.append({
                    "symbol": symbol,
                    "orb_h_std": orb_h_std, "orb_l_std": orb_l_std,
                    "orb_h_cln": orb_h_cln, "orb_l_cln": orb_l_cln,
                    "range_pct": r_pct,
                    "range_pct_clean": r_pct_cln,
                    "direction": range_dir,
                    "oracle_status": sniper_info.get(symbol, 'N/A')
                })
            # 3. Define Detection Window
            if df_today.empty:
                print(f"Skipping {symbol}: No data for today {today_ist}")
                continue
                
            session_data = df_today.between_time(effective_start, effective_end)
            in_bo, in_bd = False, False
            
            for ts, row in session_data.iterrows():
                # Update Latches (Pullback reset)
                if row['Close'] < (orb_h * 0.998): in_bo = False
                if row['Close'] > (orb_l * 1.002): in_bd = False

                # INDEPENDENT DETECTION (Mirror calculate_orb.py)
                # LONG BREAKOUT
                if row['High'] > orb_h and not in_bo:
                    is_valid, reasons, macro = meets_filters(row, 'LONG', p, orb_h_std, orb_l_std, range_dir, audit=True, risk_params=risk_params, ignore_htf=ignore_htf, orb_h_cln=orb_h_cln, orb_l_cln=orb_l_cln, range_filter_mode=range_filter_mode, globalist=globalist, sector_agent=sector_agent, news_agent=news_agent, symbol=symbol, market_mood=market_mood, oracle_status=sniper_info.get(symbol, 'N/A'))
                    if is_valid:
                        # Capture Metrics
                        candle_range = row['High'] - row['Low']
                        vqs = (row['Close'] - row['Low']) / candle_range if candle_range > 0 else 1.0
                        entry_metrics = {
                            "rsi": round(row['RSI'], 1),
                            "macd": round(row['MACD'], 3),
                            "vol_surge": round(row['Vol_Surge'], 2),
                            "slope": round(row['Slope'], 4),
                            "vqs": round(vqs, 2),
                            "weekly_rsi": h_metrics.get('weekly_rsi') if h_metrics else None,
                            "weekly_sma": h_metrics.get('weekly_sma') if h_metrics else None,
                            "adtv_cr": h_metrics.get('adtv_cr') if h_metrics else None,
                            "range_pct": r_pct,
                            "range_pct_clean": r_pct_cln,
                            "boundary": p['boundary'],
                            "orb_h_std": orb_h_std, "orb_l_std": orb_l_std,
                            "orb_h_cln": orb_h_cln, "orb_l_cln": orb_l_cln,
                            **macro
                        }
                        # Pass full df for trailing stops, but it will be filtered inside
                        trades = simulate_trade_management(symbol, 'LONG', df_today, ts, row['Close'], orb_h, orb_l, range_dir, risk_params, entry_metrics=entry_metrics, globalist=globalist, sector_agent=sector_agent, oracle_status=sniper_info.get(symbol, 'N/A'))
                        if trades:
                            executed_trades.extend(trades)
                            processed_symbols.add(symbol)
                            in_bo = True
                            break # Symbol processed
                    else:
                        skipped_signals.append({"symbol": symbol, "time": ts.strftime('%H:%M'), "side": 'LONG', "oracle_status": sniper_info.get(symbol, 'N/A'), "reason": ", ".join(reasons)})

                # SHORT BREAKDOWN
                if symbol not in processed_symbols and row['Low'] < orb_l and not in_bd:
                    is_valid, reasons, macro = meets_filters(row, 'SHORT', p, orb_h_std, orb_l_std, range_dir, audit=True, risk_params=risk_params, ignore_htf=ignore_htf, orb_h_cln=orb_h_cln, orb_l_cln=orb_l_cln, range_filter_mode=range_filter_mode, globalist=globalist, sector_agent=sector_agent, news_agent=news_agent, symbol=symbol, market_mood=market_mood, oracle_status=sniper_info.get(symbol, 'N/A'))
                    if is_valid:
                        # Capture Metrics
                        candle_range = row['High'] - row['Low']
                        vqs = (row['High'] - row['Close']) / candle_range if candle_range > 0 else 1.0
                        entry_metrics = {
                            "rsi": round(row['RSI'], 1),
                            "macd": round(row['MACD'], 3),
                            "vol_surge": round(row['Vol_Surge'], 2),
                            "slope": round(row['Slope'], 4),
                            "vqs": round(vqs, 2),
                            "weekly_rsi": h_metrics.get('weekly_rsi') if h_metrics else None,
                            "weekly_sma": h_metrics.get('weekly_sma') if h_metrics else None,
                            "adtv_cr": h_metrics.get('adtv_cr') if h_metrics else None,
                            "range_pct": r_pct,
                            "range_pct_clean": r_pct_cln,
                            "boundary": p['boundary'],
                            "orb_h_std": orb_h_std, "orb_l_std": orb_l_std,
                            "orb_h_cln": orb_h_cln, "orb_l_cln": orb_l_cln,
                            **macro
                        }
                        trades = simulate_trade_management(symbol, 'SHORT', df_today, ts, row['Close'], orb_h, orb_l, range_dir, risk_params, entry_metrics=entry_metrics, globalist=globalist, sector_agent=sector_agent, oracle_status=sniper_info.get(symbol, 'N/A'))
                        if trades:
                            executed_trades.extend(trades)
                            processed_symbols.add(symbol)
                            in_bd = True
                            break # Symbol processed
                    else:
                        skipped_signals.append({"symbol": symbol, "time": ts.strftime('%H:%M'), "side": 'SHORT', "oracle_status": sniper_info.get(symbol, 'N/A'), "reason": ", ".join(reasons)})

        return pd.DataFrame(executed_trades), pd.DataFrame(skipped_signals), pd.DataFrame(orb_summary)

def simulate_trade_management(symbol, side, df, entry_ts, entry_price, orb_h, orb_l, range_dir, risk_params, entry_metrics=None, globalist=None, sector_agent=None, trade_date=None, oracle_status='N/A'):
    """Encapsulated management logic for minute-by-minute simulation."""
    if entry_metrics is None: entry_metrics = {}
    
    # Start Trace from Entry Minute
    post_entry = df.loc[entry_ts:]
    
    # SL and TP Initialization
    if risk_params.get('sl_type') == 'ORB_BOUNDARY':
        sl_price = orb_l if side == 'LONG' else orb_h
    else:
        sl_price = entry_price * (1 - risk_params['sl_pct']/100) if side == 'LONG' else entry_price * (1 + risk_params['sl_pct']/100)
            
    active_trade = {
        "symbol": symbol,
        "side": side,
        "oracle_status": oracle_status,
        "entry_ts": entry_ts,
        "entry_price": entry_price,
        "sl": float(sl_price),
        "tp": entry_price * (1 + risk_params['tp_pct']/100) if side == 'LONG' else entry_price * (1 - risk_params['tp_pct']/100),
        "max_profit": 0.0,
        "direction": range_dir,
        "rider_active": False,
        "mkt_live": entry_metrics.get('mkt_entry', 'N/A'),
        "sec_live": entry_metrics.get('sec_entry', 'N/A'),
        **entry_metrics
    }
    
    # SIMULATION LOOP (Management Phase)
    for ts, row in post_entry.iterrows():
        current_time = ts.time()
        
        # Update Live Macro every 5 mins (only if toggled)
        if ts.minute % 5 == 0:
            if globalist and risk_params.get('use_market_filter'):
                mkt = globalist.analyze_market_mood(trade_date=trade_date)
                active_trade['mkt_live'] = f"{mkt['mood']} ({mkt['conviction']})"
            if sector_agent and risk_params.get('use_sector_filter'):
                sec = sector_agent.validate_trend(symbol, trade_date=trade_date)
                active_trade['sec_live'] = f"{sec['status']}"

        # Current Profit %
        cur_profit = ((row['High'] - active_trade['entry_price']) / active_trade['entry_price'] * 100) if active_trade['side'] == 'LONG' else \
                     ((active_trade['entry_price'] - row['Low']) / active_trade['entry_price'] * 100)
        
        if cur_profit > active_trade['max_profit']:
            active_trade['max_profit'] = cur_profit
        
        exit_reason = None
        exit_pnl = 0
        
        # 1. Phase Shift (Hold -> Rider)
        if not active_trade['rider_active']:
            if cur_profit >= risk_params['tp_pct']:
                if risk_params.get('exit_method') == 'FIXED_TARGET':
                    exit_pnl = risk_params['tp_pct']
                    exit_price = active_trade['tp']
                    exit_reason = "TARGET"
                elif risk_params.get('exit_method') == 'TREND_RIDER':
                    active_trade['rider_active'] = True
        
        # 2. TSL logic (Active only after milestone)
        if not exit_reason and active_trade['rider_active'] and risk_params['enable_tsl']:
            if active_trade['side'] == 'LONG':
                potential_sl = active_trade['entry_price'] * (1 + (active_trade['max_profit'] - risk_params['tsl_step'])/100)
                if potential_sl > active_trade['sl']: active_trade['sl'] = potential_sl
            else:
                potential_sl = active_trade['entry_price'] * (1 - (active_trade['max_profit'] - risk_params['tsl_step'])/100)
                if potential_sl < active_trade['sl']: active_trade['sl'] = potential_sl

        # 3. Exit Conditions (Price Breach) - Checked using current minute's Low/High
        if not exit_reason:
            if active_trade['side'] == 'LONG':
                if row['Low'] <= active_trade['sl']: 
                    exit_pnl = ((active_trade['sl'] - active_trade['entry_price']) / active_trade['entry_price'] * 100)
                    exit_price = active_trade['sl']
                    exit_reason = "SL/TSL HIT"
            else: # SHORT
                if row['High'] >= active_trade['sl']:
                    exit_pnl = ((active_trade['entry_price'] - active_trade['sl']) / active_trade['entry_price'] * 100)
                    exit_price = active_trade['sl']
                    exit_reason = "SL/TSL HIT"
        
        # 4. EOD Check (3:25 PM)
        if not exit_reason and current_time >= time(15, 25):
            exit_pnl = ((row['Close'] - active_trade['entry_price']) / active_trade['entry_price'] * 100) if active_trade['side'] == 'LONG' else \
                       ((active_trade['entry_price'] - row['Close']) / active_trade['entry_price'] * 100)
            exit_price = row['Close']
            exit_reason = "EOD SQUAREOFF"

        if exit_reason:
            pts = round(exit_price - active_trade['entry_price'], 2) if active_trade['side'] == 'LONG' else round(active_trade['entry_price'] - exit_price, 2)
            active_trade.update({
                "exit_ts": ts, 
                "exit_price": round(exit_price, 2), 
                "points": pts, 
                "pnl": round(exit_pnl, 2), 
                "reason": exit_reason
            })
            # Continue loop to keep current_price updated to the latest
            for final_ts, final_row in post_entry.loc[ts:].iterrows():
                active_trade["current_price"] = round(final_row['Close'], 2)
            return [active_trade]
            
    # Handle trades that never exited (Live vs Session End)
    # Determine label based on session type and current time
    ist_tz = pytz.timezone('Asia/Kolkata')
    ist_now_dt = datetime.now(ist_tz)
    
    is_today = False
    if trade_date:
        # trade_date might be a date object or datetime
        td = trade_date.date() if hasattr(trade_date, 'date') else trade_date
        if td == ist_now_dt.date():
            is_today = True
    else:
        # Default to Live/Today if no trade_date provided
        is_today = True

    if is_today and ist_now_dt.time() < time(15, 30):
        label = "OPEN"
    else:
        label = "SESSION END"
    
    exit_pnl = ((row['Close'] - active_trade['entry_price']) / active_trade['entry_price'] * 100) if active_trade['side'] == 'LONG' else \
                ((active_trade['entry_price'] - row['Close']) / active_trade['entry_price'] * 100)
    
    exit_price = row['Close']
    pts = round(exit_price - active_trade['entry_price'], 2) if active_trade['side'] == 'LONG' else round(active_trade['entry_price'] - exit_price, 2)
    
    active_trade.update({
        "exit_ts": ts, 
        "exit_price": round(exit_price, 2), 
        "current_price": round(row['Close'], 2),
        "points": pts, 
        "pnl": round(exit_pnl, 2), 
        "reason": label
    })
    return [active_trade]

def calculate_indicators(df):
    # Volume Surge (20-min Moving Average - EXCLUDING current candle for Parity)
    df['Vol_Avg'] = df['Volume'].shift(1).rolling(window=20).mean()
    df['Vol_Surge'] = df['Volume'] / df['Vol_Avg']
    
    # RSI (SMA Method for 100% Research Parity)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # MACD Histogram (MACD - Signal)
    exp12 = df['Close'].ewm(span=12, adjust=False).mean()
    exp26 = df['Close'].ewm(span=26, adjust=False).mean()
    macd_line = exp12 - exp26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    df['MACD'] = macd_line - signal_line
    
    # EMA Slope (Absolute Difference - Research Standard)
    df['MA20'] = df['Close'].rolling(window=20, min_periods=1).mean()
    df['Slope'] = df['MA20'].diff()
    return df

def meets_filters(row, side, p, orb_h_std, orb_l_std, range_dir, audit=False, risk_params=None, htf_metrics=None, ignore_htf=False, orb_h_cln=None, orb_l_cln=None, range_filter_mode='STANDARD', globalist=None, sector_agent=None, news_agent=None, symbol=None, trade_date=None, market_mood=None, oracle_status=None):
    """
    Validates if the current price/time meets all strategy filters.
    Returns: (is_valid, reasons, macro_info)
    """
    # VQS Calculation
    candle_range = row['High'] - row['Low']
    if candle_range > 0:
        vqs = (row['Close'] - row['Low']) / candle_range if side == 'LONG' else (row['High'] - row['Close']) / candle_range
    else: vqs = 1.0
    
    reasons = []

    # --- 0. Granular Oracle Bias Filter ---
    if risk_params:
        if side == 'LONG':
            allowance = risk_params.get('long_sniper_allowance', [])
            if oracle_status and oracle_status != 'N/A' and oracle_status not in allowance:
                reasons.append(f"Sniper Bias Veto: {oracle_status} not in LONG allowance {allowance}")
        else:
            allowance = risk_params.get('short_sniper_allowance', [])
            if oracle_status and oracle_status != 'N/A' and oracle_status not in allowance:
                reasons.append(f"Sniper Bias Veto: {oracle_status} not in SHORT allowance {allowance}")
    
    # --- 1. Institutional HTF Filters ---
    if not ignore_htf and htf_metrics and risk_params:
        htf = risk_params.get('htf', {})
        w_rsi = htf_metrics.get('weekly_rsi')
        w_sma = htf_metrics.get('weekly_sma')
        adtv_cr = htf_metrics.get('adtv_cr', 0)
        
        # Liquidity Guard: Check if stock meets minimum turnover threshold
        min_adtv = risk_params.get('min_adtv', 0)
        if adtv_cr < min_adtv:
            reasons.append(f"Liquidity Veto: ADTV {adtv_cr:.1f} Cr < Threshold {min_adtv} Cr")

        if w_rsi is not None and htf:
            if side == 'LONG':
                if not (htf['rsi_long'][0] <= w_rsi <= htf['rsi_long'][1]):
                    reasons.append(f"HTF: Weekly RSI {w_rsi:.1f} not in LONG range {htf['rsi_long']}")
            else:
                if not (htf['rsi_short'][0] <= w_rsi <= htf['rsi_short'][1]):
                    reasons.append(f"HTF: Weekly RSI {w_rsi:.1f} not in SHORT range {htf['rsi_short']}")
        
        if w_sma is not None:
            # current close vs weekly sma alignment
            if side == 'LONG' and htf.get('sma_align_l') and row['Close'] <= w_sma:
                reasons.append(f"HTF: Price {row['Close']:.2f} <= Weekly SMA {w_sma:.2f}")
            if side == 'SHORT' and htf.get('sma_align_s') and row['Close'] >= w_sma:
                reasons.append(f"HTF: Price {row['Close']:.2f} >= Weekly SMA {w_sma:.2f}")

    # 1. Range Bias Check (Parity with Enforce Bias toggle)
    if p.get('enforce_bias'):
        if side == 'LONG' and range_dir != 'BULLISH':
            reasons.append(f"Bias Mismatch: Long in {range_dir} range")
        if side == 'SHORT' and range_dir != 'BEARISH':
            reasons.append(f"Bias Mismatch: Short in {range_dir} range")

    # Range Pct Check
    if range_filter_mode == 'CLEAN' and orb_h_cln is not None:
        h, l = orb_h_cln, orb_l_cln
        mode_label = "Clean"
    else:
        h, l = orb_h_std, orb_l_std
        mode_label = "Standard"
        
    range_pct = ((h - l) / l) * 100
    if range_pct < p['range_pct'][0] or range_pct > p['range_pct'][1]: 
        reasons.append(f"Range ({mode_label}) {range_pct:.2f}% out of bounds")
    
    # Indicator Logic
    if side == 'LONG':
        if not (row['RSI'] >= p['rsi_l_min'] and row['RSI'] <= p['rsi_l_max']): 
            reasons.append(f"Long RSI {row['RSI']:.1f} out of bounds")
        if not (row['MACD'] >= p['macd_l_min']): 
            reasons.append(f"Long MACD {row['MACD']:.3f} < {p['macd_l_min']}")
        if not (row['Slope'] >= p['trend_l_min']): 
            reasons.append(f"Long Slope {row['Slope']:.4f} < {p['trend_l_min']}")
    else:
        if not (row['RSI'] >= p['rsi_s_min'] and row['RSI'] <= p['rsi_s_max']): 
            reasons.append(f"Short RSI {row['RSI']:.1f} out of bounds")
        if not (row['MACD'] <= p['macd_s_max']): 
            reasons.append(f"Short MACD {row['MACD']:.3f} > {p['macd_s_max']}")
        if not (row['Slope'] <= p['trend_s_max']): 
            reasons.append(f"Short Slope {row['Slope']:.4f} > {p['trend_s_max']}")

    # --- 2. Macro Indicators (Efficiency Guard: Only if Technicals pass) ---
    macro_info = {}
    if len(reasons) == 0 and risk_params:
        # Market Mood
        if market_mood:
            macro_info['mkt_entry'] = f"{market_mood['mood']} ({market_mood['conviction']})"
            if risk_params.get('use_market_filter'):
                if (side == 'LONG' and market_mood['mood'] == 'Bearish') or (side == 'SHORT' and market_mood['mood'] == 'Bullish'):
                    reasons.append(f"Macro: Market Mood Veto ({market_mood['mood']})")

        # Sector Trend (only if toggled)
        if sector_agent and symbol and risk_params.get('use_sector_filter'):
            sec = sector_agent.validate_trend(symbol, trade_date=trade_date)
            macro_info['sec_entry'] = sec['status']
            if (side == 'LONG' and sec['status'] == 'BEARISH') or (side == 'SHORT' and sec['status'] == 'BULLISH'):
                reasons.append(f"Macro: Sector Veto ({sec['status']})")

        # News Catalyst (only if toggled)
        if news_agent and symbol and risk_params.get('use_news_filter'):
            res = news_agent.analyze_sentiment(symbol)
            macro_info['news_score'] = res.get('score', 50)
            macro_info['news_summary'] = ", ".join(res.get('highlights', []))
            if (side == 'LONG' and res.get('score', 50) < 40) or (side == 'SHORT' and res.get('score', 50) > 60):
                reasons.append(f"Macro: News Veto (Score: {res.get('score')})")

    return (len(reasons) == 0, reasons, macro_info)
