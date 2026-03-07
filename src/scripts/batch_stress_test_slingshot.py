import os
import sys
import pandas as pd
import numpy as np
import random
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.dhan_client import DhanDataClient

def calculate_slingshot_score(df_slice):
    if len(df_slice) < 20: return 0, [], {}
    df = df_slice.copy()
    df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['STD20'] = df['Close'].rolling(window=20).std()
    df['BW'] = (df['STD20'] * 4) / df['MA20'].replace(0, np.nan)
    df['BW_SMA'] = df['BW'].rolling(20).mean()
    
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # --- DEEP DIVE INDICATORS ---
    # 1. MACD (12, 26, 9)
    exp12 = df['Close'].ewm(span=12, adjust=False).mean()
    exp26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = exp12 - exp26
    df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = df['MACD'] - df['Signal_Line']
    
    # 2. Stochastics (14, 3, 3)
    low_min = df['Low'].rolling(window=14).min()
    high_max = df['High'].rolling(window=14).max()
    df['%K'] = (100 * (df['Close'] - low_min) / (high_max - low_min)).fillna(50)
    
    # 3. EMA Slope (Trend Strength proxy)
    df['EMA20_Slope'] = df['MA20'].diff()
    
    # Current Metrics (Recalculate or use existing)
    curr = df.iloc[-1]
    # Re-extract these to be safe as df was modified
    vwap_dist = (curr['Close'] - curr['VWAP']) / curr['VWAP'] * 100
    is_squeeze = curr['BW'] < curr['BW_SMA']
    rsi_val = curr['RSI'] # RSI was already in df
    macd_hist_val = curr['MACD_Hist']
    stoch_k = curr['%K']
    ema_slope = curr['EMA20_Slope']
    
    # History for momentum checks
    curr_hist = df['MACD_Hist'].iloc[-1]
    prev_hist = df['MACD_Hist'].iloc[-2] if len(df) > 1 else 0
    
    score = 0
    signals = []
    # Slingshot Components
    if -1.2 <= vwap_dist <= -0.3: score += 30
    if is_squeeze: score += 30
    
    # Volume Pulse
    avg_min_vol = df['Volume'].mean()
    vol_ratio = curr['Volume'] / avg_min_vol if avg_min_vol > 0 else 0
    recent_vols = df['Volume'].tail(3)
    is_vol_rising = len(recent_vols) >= 3 and recent_vols.iloc[0] < recent_vols.iloc[1] < recent_vols.iloc[2]
    if vol_ratio > 1.2 or is_vol_rising: score += 20
    if 40 <= rsi_val <= 55: score += 20
    
    # GOLDEN GUARDS (Deep Dive Filters)
    # Guard 1: Red Expansion (MACD)
    if curr_hist < 0 and curr_hist < prev_hist:
        score -= 50
        signals.append(f"SAFETY:MACD_Exp({curr_hist:.3f})")

    # Guard 2: Dead Floor (Stochastics)
    if stoch_k < 15:
        score -= 30
        signals.append(f"SAFETY:Stoch_Floor({stoch_k:.1f})")

    # Guard 3: RSI Safety Floor (Raised to 40)
    if rsi_val < 40:
        score -= 40
        signals.append(f"SAFETY:RSI<40")
    
    # Guard 4: Slope of Death
    if ema_slope < -0.1:
        score -= 20
        signals.append(f"SAFETY:Slope({ema_slope:.2f})")
    
    # Base Guards
    if vwap_dist < -1.5: score -= 50
    curr_ist = df.index[-1] + timedelta(hours=5, minutes=30)
    if curr_ist.hour == 9 and curr_ist.minute < 45: score -= 20
    
    metrics = {
        "vwap_dist": vwap_dist,
        "is_squeeze": is_squeeze,
        "rsi": rsi_val,
        "vol_ratio": vol_ratio,
        "macd_hist": macd_hist_val,
        "stoch_k": stoch_k,
        "ema_slope": ema_slope
    }
    
    return score, signals, metrics

def run_simulation(data_client, symbol):
    df = data_client.fetch_realtime_data(symbol, period="1d", interval="1m")
    if df is None or df.empty: return None
    
    results = {"symbol": symbol, "trades": [], "status": "COMPLETED"}
    in_position = False
    entry_price = 0
    entry_time = None
    score_history = []
    
    for i in range(20, len(df)):
        df_slice = df.iloc[:i+1]
        score, _, metrics = calculate_slingshot_score(df_slice)
        score_history.append(score)
        curr_price = df.iloc[i]['Close']
        ist_time = (df.index[i] + timedelta(hours=5, minutes=30)).strftime('%H:%M')
        
        if not in_position and score >= 80:
            # Check for Momentum (Avoid static scores)
            momentum = score_history[-1] - score_history[-3] if len(score_history) >= 3 else 0
            if momentum >= 10: # Only enter on progressive consensus
                in_position = True
                entry_price = curr_price
                entry_time = ist_time
        
        elif in_position:
            profit_pct = (curr_price - entry_price) / entry_price * 100
            
            # --- ACTIVE CRISIS MANAGEMENT (Panic Exits) ---
            # Checks run EVERY MINUTE while in position
            is_panic = False
            panic_reason = ""
            
            # 1. RSI Crash (Floor broken)
            if metrics['rsi'] < 35:
                is_panic = True
                panic_reason = f"PANIC:RSI_Crash({metrics['rsi']:.1f})"
            
            # 2. Stochastics Dead (Momentum dead)
            if metrics['stoch_k'] < 5:
                is_panic = True
                panic_reason = f"PANIC:Stoch_Dead({metrics['stoch_k']:.1f})"
                
            # 3. Expansion Limit (Falling Knife)
            if metrics['vwap_dist'] < -1.5:
                is_panic = True
                panic_reason = f"PANIC:Falling_Knife({metrics['vwap_dist']:.2f}%)"
            
            # Execution Priorities
            if profit_pct >= 1.0:
                results["trades"].append({"type": "PROFIT", "entry_time": entry_time, "exit_time": ist_time, "pnl": profit_pct})
                in_position = False
                
            elif is_panic:
                # Force Exit at Market
                results["trades"].append({"type": "PANIC_EXIT", "entry_time": entry_time, "exit_time": ist_time, "pnl": profit_pct, "reason": panic_reason, "metrics_at_sl": metrics})
                in_position = False
                
            elif profit_pct <= -0.5:
                results["trades"].append({"type": "SL", "entry_time": entry_time, "exit_time": ist_time, "pnl": profit_pct, "metrics_at_sl": metrics})
                in_position = False
                
    return results

def main():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    data_client = DhanDataClient(cid, token)
    
    # 1. Sniper Stocks provided by User + DB fallback
    USER_SNIPERS = [
        "SHRIRAMFIN", "KARURVYSYA", "HINDALCO", "NTPC", "BSE", "CUB", "SBIN",
        "COALINDIA", "NMDC", "CANBK", "EMBASSY", "NYKAA", "AXISBANK", "SAIL",
        "BELRISE", "BEL", "BANKINDIA", "PFOCUS", "ONGC", "POWERGRID", "OIL",
        "MRPL", "INDUSTOWER", "IDFCFIRSTB", "ASHOKLEY", "VEDL", "CUPID",
        "NATIONALUM", "AVANTIFEED", "BPCL", "RAIN", "APEX", "MAHABANK",
        "UNIONBANK", "IOC", "ADANIPORTS", "RICOAUTO", "STLTECH", "TATASTEEL"
    ]
    
    test_set = []
    try:
        from src.db.schema import Ticker
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root:password@localhost/bharatquant")
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        snipers = session.query(Ticker).filter(Ticker.oracle_status == 'SNIPER').all()
        db_snipers = [s.symbol for s in snipers]
        session.close()
        
        # Merge and deduplicate
        test_set = list(set(USER_SNIPERS + db_snipers))
        logger.info(f"Initialized with {len(test_set)} 'SNIPER' stocks (User provided + DB).")
            
    except Exception as e:
        logger.error(f"Failed to fetch snipers from DB: {e}. Using user provided list.")
        test_set = USER_SNIPERS

    if not test_set:
        csv_path = "sec_list.csv"
        symbols = pd.read_csv(csv_path)['Symbol'].tolist()
        clean_symbols = [s for s in symbols if "BEES" not in s and "GOLD" not in s and "SILVER" not in s]
        test_set = random.sample(clean_symbols, 30)
    
    # Filter out known non-tradable or ETF types if necessary
    test_set = [s for s in test_set if "BEES" not in s and "GOLDIETF" not in s]

    print("\n" + "="*145)
    print(f"PRECISION SNIPER TEST: {len(test_set)} VERIFIED SNIPER STOCKS")
    print("="*145)
    print(f"{'Symbol':<15} | {'Trades':<8} | {'Win Rate':<10} | {'Status'}")
    print("-" * 145)
    
    all_trades = []
    
    for symbol in test_set:
        try:
            res = run_simulation(data_client, symbol)
            if not res: continue
            
            trades = res["trades"]
            wins = len([t for t in trades if t["type"] == "PROFIT"])
            losses = len([t for t in trades if t["type"] == "SL"])
            win_rate = (wins / len(trades) * 100) if trades else 0
            
            print(f"{symbol:<15} | {len(trades):<8} | {win_rate:>8.1f}% | {'✅' if win_rate > 50 else '⚠️' if win_rate > 0 else '⚪'}")
            
            for t in trades:
                all_trades.append({"symbol": symbol, **t})
        except Exception as e:
            logger.error(f"Error testing {symbol}: {e}")

    print("\n" + "="*145)
    print(f"{'TYPE':<8} | {'SYMBOL':<12} | {'ENTRY':<8} | {'PnL%':<7} | {'RSI':<5} | {'MACD_H':<7} | {'%K':<5} | {'Slope':<6} | {'VWAP_D'}")
    print("-" * 145)
    
    # Generate Full Markdown Report
    report_lines = [
        "# Sniper Strategy: Granular Trade Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "\n## 📈 Detailed Trade Log\n",
        "| Symbol | Type | Entry Time | Exit Time | PnL% | Reason | RSI | MACD_H | %K | VWAP_D |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |"
    ]

    for t in all_trades:
        m = t.get("metrics_at_sl", {}) if t['type'] != 'PROFIT' else {}
        rsi = f"{m.get('rsi', 0):.1f}" if m else "-"
        macd = f"{m.get('macd_hist', 0):.3f}" if m else "-"
        stoch = f"{m.get('stoch_k', 0):.1f}" if m else "-"
        slope = f"{m.get('ema_slope', 0):.2f}" if m else "-"
        v_dist = f"{m.get('vwap_dist', 0):.2f}%" if m else "-"
        reason = t.get("reason", "-")
        
        # Consol Print
        print(f"{t['type']:<8} | {t['symbol']:<12} | {t['entry_time']:<8} | {t['pnl']:>6.2f}% | {rsi:<5} | {macd:<7} | {stoch:<5} | {slope:<6} | {v_dist}")
        
        # Report Line
        report_lines.append(f"| {t['symbol']} | {t['type']} | {t['entry_time']} | {t['exit_time']} | {t['pnl']:.2f}% | {reason} | {rsi} | {macd} | {stoch} | {v_dist} |")
    
    # Save Report
    report_path = "sniper_full_report.md"
    with open(report_path, "w") as f:
        f.write("\n".join(report_lines))
    
    print("="*145)
    print(f"\n✅ FULL DETAILED REPORT SAVED TO: {report_path}")

if __name__ == "__main__":
    main()
