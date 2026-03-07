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
    if len(df_slice) < 20: return 0, {}, {}
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
    
    # MACD & Stochastics for the Guards
    df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = df['EMA12'] - df['EMA26']
    df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = df['MACD'] - df['Signal']
    
    low_14 = df['Low'].rolling(window=14).min()
    high_14 = df['High'].rolling(window=14).max()
    df['%K'] = 100 * ((df['Close'] - low_14) / (high_14 - low_14).replace(0, np.nan))
    
    curr = df.iloc[-1]
    vwap_dist = (curr['Close'] - curr['VWAP']) / curr['VWAP'] * 100
    rsi_val = curr['RSI']
    stoch_k = curr['%K']
    
    score = 0
    if -1.2 <= vwap_dist <= -0.3: score += 30
    if curr['BW'] < curr['BW_SMA']: score += 30
    
    avg_min_vol = df['Volume'].mean()
    vol_ratio = curr['Volume'] / avg_min_vol if avg_min_vol > 0 else 0
    if vol_ratio > 1.2: score += 20
    if 40 <= rsi_val <= 55: score += 20
    
    # Simplified Guards for comparison
    if rsi_val < 35: score -= 40
    if vwap_dist < -1.5: score -= 50
    
    metrics = {"vwap_dist": vwap_dist, "rsi": rsi_val, "stoch_k": stoch_k}
    return score, metrics

def run_simulation(data_client, symbol, mode="DEFENSIVE"):
    df = data_client.fetch_realtime_data(symbol, period="1d", interval="1m")
    if df is None or df.empty: return None
    
    results = {"type": mode, "trades": []}
    in_position = False
    entry_price = 0
    
    for i in range(20, len(df)):
        df_slice = df.iloc[:i+1]
        score, metrics = calculate_slingshot_score(df_slice)
        curr_price = df.iloc[i]['Close']
        
        if not in_position and score >= 80:
            in_position = True
            entry_price = curr_price
        
        elif in_position:
            profit_pct = (curr_price - entry_price) / entry_price * 100
            
            # EXIT LOGIC
            did_exit = False
            exit_type = ""
            
            if profit_pct >= 1.0:
                did_exit = True
                exit_type = "PROFIT"
            elif profit_pct <= -0.5:
                did_exit = True
                exit_type = "SL"
            elif mode == "DEFENSIVE":
                if metrics['rsi'] < 35 or metrics['stoch_k'] < 5:
                    did_exit = True
                    exit_type = "PANIC_EXIT"
            
            if did_exit:
                results["trades"].append({"type": exit_type, "pnl": profit_pct})
                in_position = False
                
    return results

def main():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    data_client = DhanDataClient(cid, token)
    
    # Load Sniper Stocks from DB
    from src.db.schema import Ticker
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    snipers = session.query(Ticker).filter(Ticker.oracle_status == 'SNIPER').all()
    symbols = [s.symbol for s in snipers]
    session.close()
    
    if not symbols: 
        print("No sniper stocks found in DB.")
        return

    print(f"\n📊 NET COMPARISON (Tax Adjusted): 41 SNIPER STOCKS")
    print(f"Applying Friction: 0.05% per Round Trip (Tax/Brokerage/STT)")
    print("="*100)
    
    TAX_FRICTION = 0.05
    
    def_stats = {"count": 0, "gross_pnl": 0, "net_pnl": 0, "wins": 0, "losses": 0}
    pas_stats = {"count": 0, "gross_pnl": 0, "net_pnl": 0, "wins": 0, "losses": 0}
    
    for s in symbols:
        # Run Defensive
        res_d = run_simulation(data_client, s, mode="DEFENSIVE")
        if res_d:
            for t in res_d['trades']:
                def_stats["count"] += 1
                def_stats["gross_pnl"] += t['pnl']
                def_stats["net_pnl"] += (t['pnl'] - TAX_FRICTION)
                if (t['pnl'] - TAX_FRICTION) > 0: def_stats["wins"] += 1
                else: def_stats["losses"] += 1
        
        # Run Passive
        res_p = run_simulation(data_client, s, mode="PASSIVE")
        if res_p:
            for t in res_p['trades']:
                pas_stats["count"] += 1
                pas_stats["gross_pnl"] += t['pnl']
                pas_stats["net_pnl"] += (t['pnl'] - TAX_FRICTION)
                if (t['pnl'] - TAX_FRICTION) > 0: pas_stats["wins"] += 1
                else: pas_stats["losses"] += 1

    print("\n[DEFENSIVE MODE (High Frequency Protection)]")
    print(f"Total Trades:  {def_stats['count']}")
    print(f"Gross PnL:     {def_stats['gross_pnl']:>6.2f}%")
    print(f"Tax Paid:      {-(def_stats['count'] * TAX_FRICTION):>6.2f}%")
    print(f"Net PnL:       {def_stats['net_pnl']:>6.2f}%  <-- (Winner?)")
    print(f"Net Win Rate:  {(def_stats['wins']/def_stats['count']*100 if def_stats['count']>0 else 0):.1f}%")

    print("\n[PASSIVE MODE (Low Frequency Holding)]")
    print(f"Total Trades:  {pas_stats['count']}")
    print(f"Gross PnL:     {pas_stats['gross_pnl']:>6.2f}%")
    print(f"Tax Paid:      {-(pas_stats['count'] * TAX_FRICTION):>6.2f}%")
    print(f"Net PnL:       {pas_stats['net_pnl']:>6.2f}%")
    print(f"Net Win Rate:  {(pas_stats['wins']/pas_stats['count']*100 if pas_stats['count']>0 else 0):.1f}%")

    print("\nConclusion:")
    if def_stats['net_pnl'] > pas_stats['net_pnl']:
        print(f"✅ DEFENSIVE is better by {(def_stats['net_pnl'] - pas_stats['net_pnl']):.2f}%. Even with taxes, damage control wins.")
    else:
        print(f"💡 PASSIVE is better by {(pas_stats['net_pnl'] - def_stats['net_pnl']):.2f}%. Tax friction killed the Defensive edge.")

if __name__ == "__main__":
    main()
