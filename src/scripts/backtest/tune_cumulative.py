import glob
import logging
from multiprocessing import Pool
from cumulative_backtest import run_cumulative_backtest

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def process_file_with_params(args):
    file, params = args
    try:
        signals, completed = run_cumulative_backtest(
            file, 
            quiet=True, 
            window_secs=params['window_secs'],
            min_vol_ratio=params['min_vol_ratio'],
            min_vqs=params['min_vqs'],
            sl_pct=params['sl_pct'],
            tp_pct=params['tp_pct']
        )
        for s in completed: 
            s['file'] = file
            s['params'] = params
        return completed
    except Exception as e:
        return []

def main():
    target_date = "2026-03-09"
    search_pattern = f"data/ticks/*/{target_date}.parquet"
    files = glob.glob(search_pattern)
    logger.info(f"Found {len(files)} parquet files for {target_date}.")
    
    param_sets = [
        {'name': 'Hyper Aggressive (15s, 60% Flow)', 'window_secs': 15, 'min_vol_ratio': 0.60, 'min_vqs': 0.10, 'sl_pct': 0.8, 'tp_pct': 1.6},
        {'name': 'Aggressive (30s, 65% Flow)', 'window_secs': 30, 'min_vol_ratio': 0.65, 'min_vqs': 0.15, 'sl_pct': 0.8, 'tp_pct': 1.6},
        {'name': 'Moderate (60s, 70% Flow)', 'window_secs': 60, 'min_vol_ratio': 0.70, 'min_vqs': 0.20, 'sl_pct': 0.8, 'tp_pct': 1.6},
        {'name': 'Conservative (120s, 75% Flow)', 'window_secs': 120, 'min_vol_ratio': 0.75, 'min_vqs': 0.30, 'sl_pct': 0.8, 'tp_pct': 1.6},
    ]
    
    for pset in param_sets:
        logger.info(f"\n--- Testing Parameters: {pset['name']} ---")
        
        args_list = [(f, pset) for f in files]
        completed_trades = []
        
        with Pool(processes=8) as pool:
            for result in pool.imap_unordered(process_file_with_params, args_list):
                completed_trades.extend(result)
                
        logger.info(f"-> Total Trades Executed: {len(completed_trades)}")
        
        if len(completed_trades) > 0:
            wins = [t for t in completed_trades if t.get('result') == 'WIN']
            losses = [t for t in completed_trades if t.get('result') == 'LOSS']
            win_rate = (len(wins) / len(completed_trades)) * 100 if completed_trades else 0
            
            logger.info(f"-> WIN RATE: {win_rate:.1f}% ({len(wins)} W / {len(losses)} L)")
            
            pnl = sum(t['pnl_pct'] for t in completed_trades)
            logger.info(f"-> TOTAL PnL: {pnl:.2f}%")
            
            for idx, sig in enumerate(completed_trades[:10]): 
                logger.info(f"  [{idx+1}] {sig['result']} | {sig['side']} | Entry: {sig['entry']} -> Exit: {sig['exit_price']:.2f} ({sig['pnl_pct']:.2f}%) | {sig['file']}")

if __name__ == "__main__":
    main()
