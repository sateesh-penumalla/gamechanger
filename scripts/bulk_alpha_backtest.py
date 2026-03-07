import os
import sys
import pandas as pd
import numpy as np
from loguru import logger
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

from scripts.historical_swing_analyzer import HistoricalSwingAnalyzer

def run_bulk_backtest():
    analyzer = HistoricalSwingAnalyzer()
    
    # 1. Fetch all symbols from db
    try:
        tickers_df = pd.read_sql("SELECT symbol FROM tickers", analyzer.engine)
        symbols = tickers_df['symbol'].tolist()
    except Exception as e:
        logger.error(f"Failed to fetch tickers: {e}")
        symbols = ["ABB", "GODFRYPHLP", "TARIL", "VBL", "RELIANCE", "SBIN"] # Fallback

    logger.info(f"Starting Bulk Alpha Backtest for {len(symbols)} symbols...")
    
    all_summary_trades = []
    
    for symbol in symbols:
        try:
            df = analyzer.fetch_local_history(symbol)
            if df.empty or len(df) < 50:
                continue
                
            df = analyzer.calculate_vcp_metrics(df)
            
            # Simulated Backtest Logic (simplified for speed)
            in_trade = False
            entry_price = 0
            target = 0
            stop_loss = 0
            entry_date = None
            
            for i in range(50, len(df)):
                row = df.iloc[i]
                curr_date = df.index[i]
                
                if in_trade:
                    if row['High'] >= target:
                        all_summary_trades.append({
                            "symbol": symbol,
                            "entry_date": entry_date,
                            "exit_date": curr_date,
                            "pnl": 30.0, # Target is 30%
                            "status": "WIN"
                        })
                        in_trade = False
                    elif row['Low'] <= stop_loss:
                        loss = ((entry_price - stop_loss) / entry_price) * 100
                        all_summary_trades.append({
                            "symbol": symbol,
                            "entry_date": entry_date,
                            "exit_date": curr_date,
                            "pnl": -loss,
                            "status": "LOSS"
                        })
                        in_trade = False
                
                if not in_trade:
                    setup = analyzer.detect_alpha_momentum_setup(df, current_idx=i)
                    if setup['signal'] == 'ALPHA_BUY':
                        in_trade = True
                        entry_price = setup['entry']
                        target = setup['target']
                        stop_loss = setup['stop_loss']
                        entry_date = curr_date
                        
            # Handle open trades
            if in_trade:
                curr_pnl = ((df.iloc[-1]['Close'] - entry_price) / entry_price) * 100
                all_summary_trades.append({
                    "symbol": symbol,
                    "entry_date": entry_date,
                    "exit_date": df.index[-1],
                    "pnl": curr_pnl,
                    "status": "OPEN"
                })

        except Exception as e:
            continue

    if not all_summary_trades:
        print("\nNo Alpha signals found in historical data.")
        return

    # Process Results
    results_df = pd.DataFrame(all_summary_trades)
    
    # Filter for completed trades (WIN/LOSS) that started more than a month ago
    # We'll just show all for now.
    
    win_rate = (len(results_df[results_df['pnl'] > 0]) / len(results_df)) * 100
    avg_return = results_df['pnl'].mean()
    
    print("\n" + "="*60)
    print("🚀 BULK ALPHA MOMENTUM BACKTEST SUMMARY")
    print("="*60)
    print(f"Total Symbols Scanned:  {len(symbols)}")
    print(f"Total Alpha Signals:    {len(results_df)}")
    print(f"Overall Win Rate:       {win_rate:.1f}%")
    print(f"Average Return/Trade:   {avg_return:.2f}%")
    print("="*60)
    
    print("\n🔥 TOP 10 HISTORICAL ALPHA WINNERS:")
    top_winners = results_df.sort_values(by='pnl', ascending=False).head(10)
    print(top_winners[['symbol', 'entry_date', 'exit_date', 'pnl', 'status']])
    
    print("\n📈 RECENT ALPHA SIGNALS (IN PLAY):")
    recent = results_df[results_df['status'] == "OPEN"].tail(10)
    print(recent[['symbol', 'entry_date', 'pnl']])
    print("="*60 + "\n")

if __name__ == "__main__":
    run_bulk_backtest()
