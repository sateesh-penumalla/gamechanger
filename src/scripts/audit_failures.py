import os
import sys
import pandas as pd
import numpy as np
from loguru import logger
from dotenv import load_dotenv
from datetime import timedelta

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.dhan_client import DhanDataClient

def calculate_indicators(df):
    # MACD
    df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['Close'].ewm(span=26).mean()
    df['MACD'] = df['EMA12'] - df['EMA26']
    df['Signal'] = df['MACD'].ewm(span=9).mean()
    df['MACD_Hist'] = df['MACD'] - df['Signal']
    # Expansion check
    df['MACD_Expanding'] = (df['MACD_Hist'] < 0) & (df['MACD_Hist'] < df['MACD_Hist'].shift(1))
    
    # RSI
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # VWAP
    df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
    df['VWAP_D'] = (df['Close'] - df['VWAP']) / df['VWAP'] * 100
    
    # BW
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['STD20'] = df['Close'].rolling(window=20).std()
    df['BW'] = (df['STD20'] * 4) / df['MA20'].replace(0, np.nan)
    df['BW_SMA'] = df['BW'].rolling(20).mean()
    
    return df

def run_audit(data_client, symbol):
    df = data_client.fetch_realtime_data(symbol, period="1d", interval="1m")
    if df is None or df.empty: return []
    df = calculate_indicators(df)
    
    trades = []
    in_position = False
    entry_idx = 0
    
    for i in range(20, len(df)):
        curr = df.iloc[i]
        score = 0
        if -1.2 <= curr['VWAP_D'] <= -0.3: score += 30
        if curr['BW'] < curr['BW_SMA']: score += 30
        if curr['RSI'] >= 40: score += 20
        
        if not in_position and score >= 80:
            in_position = True
            entry_idx = i
        elif in_position:
            pnl = (curr['Close'] - df.iloc[entry_idx]['Close']) / df.iloc[entry_idx]['Close'] * 100
            if pnl >= 1.0:
                trades.append({"type": "WIN", "expanding": df.iloc[entry_idx]['MACD_Expanding']})
                in_position = False
            elif pnl <= -0.5:
                trades.append({"type": "FAIL", "expanding": df.iloc[entry_idx]['MACD_Expanding']})
                in_position = False
    return trades

def main():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    data_client = DhanDataClient(cid, token)
    
    # Load Snipers
    from src.db.schema import Ticker
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    snipers = session.query(Ticker).filter(Ticker.oracle_status.in_(['UP_SNIPER', 'DOWN_SNIPER'])).all()
    symbols = [s.symbol for s in snipers]
    session.close()

    fail_expanding = 0
    fail_total = 0
    win_expanding = 0
    win_total = 0

    for s in symbols:
        trades = run_audit(data_client, s)
        for t in trades:
            if t['type'] == "FAIL":
                fail_total += 1
                if t['expanding']: fail_expanding += 1
            else:
                win_total += 1
                if t['expanding']: win_expanding += 1

    print("\n📊 FAILURE AUDIT: THE MACD TRUTH")
    print("="*50)
    print(f"Total Failures: {fail_total}")
    print(f"Failures on Expanding Red: {fail_expanding} ({(fail_expanding/fail_total*100 if fail_total>0 else 0):.1f}%)")
    print("-" * 50)
    print(f"Total Wins: {win_total}")
    print(f"Wins on Expanding Red: {win_expanding} ({(win_expanding/win_total*100 if win_total>0 else 0):.1f}%)")
    print("="*50)

if __name__ == "__main__":
    main()
