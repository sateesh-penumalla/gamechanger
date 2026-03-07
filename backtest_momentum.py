import os
import pandas as pd
import numpy as np
import glob
from collections import deque

class MomentumBacktester:
    def __init__(self, name, surge_threshold, vqs_threshold, target_pct, sl_pct, vol_window_size=50):
        self.name = name
        self.surge_threshold = surge_threshold
        self.vqs_threshold = vqs_threshold
        self.target_pct = target_pct
        self.sl_pct = sl_pct
        self.vol_window_size = vol_window_size
        self.results = []
        self.active_trade = None

    def run_backtest(self, symbol, data_input):
        self.active_trade = None
        try:
            if isinstance(data_input, list): # List of batch files
                df_list = [pd.read_parquet(f) for f in data_input]
                df = pd.concat(df_list).sort_values('timestamp')
            else: # Single file or directory
                df = pd.read_parquet(data_input).sort_values('timestamp')
        except: return

        if 'ltp' not in df.columns: return
        
        ltps = df['ltp'].values
        if 'buy_vol' in df.columns and 'sell_vol' in df.columns:
            diffs = df['buy_vol'].values + df['sell_vol'].values
        elif 'volume' in df.columns:
            diffs = df['volume'].diff().fillna(0).values
        else: return

        timestamps = df['timestamp'].values
        vwap_num, vwap_den = 0.0, 0.0
        price_history = deque(maxlen=self.vol_window_size)
        vol_history = deque(maxlen=self.vol_window_size)
        
        for i in range(len(ltps)):
            ltp, diff, ts = ltps[i], diffs[i], timestamps[i]
            if ltp <= 0: continue
            
            if diff > 0:
                vwap_num += (ltp * diff)
                vwap_den += diff
                vol_history.append(diff)
            
            price_history.append(ltp)
            if len(vol_history) < 20: continue
            
            vwap = vwap_num / vwap_den if vwap_den > 0 else ltp
            h = list(price_history)
            ticks = [1 if h[j] > h[j-1] else -1 for j in range(1, len(h)) if h[j] != h[j-1]]
            vqs = sum(ticks)/len(ticks) if ticks else 0
            surge = diff / np.mean(vol_history) if vol_history else 0
            
            if self.active_trade:
                t = self.active_trade
                if t['side'] == 'LONG':
                    if ltp >= t['target']: self._close(t, ltp, ts, 'TARGET HIT')
                    elif ltp <= t['sl']: self._close(t, ltp, ts, 'STOP LOSS')
                else:
                    if ltp <= t['target']: self._close(t, ltp, ts, 'TARGET HIT')
                    elif ltp >= t['sl']: self._close(t, ltp, ts, 'STOP LOSS')
                
            if not self.active_trade and diff > 0:
                if surge >= self.surge_threshold and abs(vqs) >= self.vqs_threshold:
                    if vqs >= self.vqs_threshold and ltp > vwap: 
                        self._open(symbol, 'LONG', ltp, ts, surge, vqs)
                    elif vqs <= -self.vqs_threshold and ltp < vwap: 
                        self._open(symbol, 'SHORT', ltp, ts, surge, vqs)

    def _open(self, symbol, side, price, ts, surge, vqs):
        target = price * (1 + self.target_pct) if side == 'LONG' else price * (1 - self.target_pct)
        sl = price * (1 - self.sl_pct) if side == 'LONG' else price * (1 + self.sl_pct)
        self.active_trade = {'symbol': symbol, 'side': side, 'entry': price, 'ts': ts, 'target': target, 'sl': sl, 'surge': surge, 'vqs': vqs}

    def _close(self, t, price, ts, reason):
        pnl_pct = (price - t['entry'])/t['entry'] if t['side'] == 'LONG' else (t['entry'] - price)/t['entry']
        try:
            duration = round(abs((pd.to_datetime(ts) - pd.to_datetime(t['ts'])).total_seconds()) / 60, 1)
        except: duration = 0.0
        self.results.append({'Symbol': t['symbol'], 'Side': t['side'], 'PnL%': round(pnl_pct*100, 2), 'Reason': reason, 'Duration': duration, 'Surge': round(t['surge'],1), 'VQS': round(t['vqs'],2)})
        self.active_trade = None

def report(name, results):
    if not results: return
    df = pd.DataFrame(results)
    wr = (df['Reason'] == 'TARGET HIT').mean() * 100
    print(f"\n" + "="*95 + f"\nSTRATEGY: {name:10} | Trades: {len(df):3} | WinRate: {wr:5.1f}% | Net: {df['PnL%'].sum():.2f}%\n" + "-"*95)
    print(df[['Symbol', 'Side', 'PnL%', 'Reason', 'Duration', 'Surge', 'VQS']].sort_values('Symbol').to_string(index=False))

if __name__ == "__main__":
    # Group batch files by Session (Symbol + Date)
    session_data = {}
    for f in glob.glob("data/ticks/*/*.parquet"):
        symbol = f.split('/')[-2]
        date_str = os.path.basename(f).split('_')[0].split('.')[0]
        key = (symbol, date_str)
        if key not in session_data: session_data[key] = []
        session_data[key].append(f)
    
    # Run comparison on target sessions
    target_keys = [('TEJASNET', '2026-03-02'), ('BANDHANBNK', '2026-03-02'), ('BANKINDIA', '2026-03-06'), ('BANKBARODA', '2026-03-06')]
    
    strategies = [
        MomentumBacktester("GOLD_GUARD", 8.0, 0.6, 0.005, 0.01, 50),
        MomentumBacktester("SCALPER_33", 6.0, 0.5, 0.003, 0.006, 33)
    ]

    for key in target_keys:
        if key in session_data:
            for s in strategies: s.run_backtest(key[0], session_data[key])

    print("\n" + "="*95 + "\nFINAL STRATEGY SELECTION BENCHMARK\n" + "="*95)
    for s in strategies: report(s.name, s.results)
