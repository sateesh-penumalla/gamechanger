import os
import glob
import logging
import itertools
from multiprocessing import Pool
from momentum_backtest import run_backtest

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def process_file_with_params(args):
    file, params = args
    try:
        signals, completed = run_backtest(
            file, 
            quiet=True, 
            vol_surge_req=params['vol_surge_req'],
            vqs_req=params['vqs_req'],
            imb_req=params['imb_req'],
            confirm_pct=params['confirm_pct'],
            confirm_time=params['confirm_time']
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
        {'name': 'Current v2 Baseline',      'vol_surge_req': 8.0, 'vqs_req': 0.75, 'imb_req': 0.35, 'confirm_pct': 0.20, 'confirm_time': 30},
        {'name': 'Strategy 1 (Wide Net)',    'vol_surge_req': 2.5, 'vqs_req': 0.25, 'imb_req': 0.05, 'confirm_pct': 0.05, 'confirm_time': 60},
        {'name': 'Strategy 2 (Low Imb)',     'vol_surge_req': 3.5, 'vqs_req': 0.40, 'imb_req': 0.02, 'confirm_pct': 0.05, 'confirm_time': 90},
        {'name': 'Strategy 3 (No Imb Req)',  'vol_surge_req': 2.0, 'vqs_req': 0.30, 'imb_req': -0.50, 'confirm_pct': 0.05, 'confirm_time': 120},
    ]
    
    for pset in param_sets:
        logger.info(f"\n--- Testing Parameters: {pset['name']} ---")
        logger.info(f"Params: Vol={pset['vol_surge_req']}x, ConfPct={pset['confirm_pct']}%, ConfTime={pset['confirm_time']}s")
        
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
            
            for idx, sig in enumerate(completed_trades[:20]): # Show up to 20 examples
                logger.info(f"  [{idx+1}] {sig['result']} | {sig['side']} | Entry: {sig['entry']} -> Exit: {sig['exit_price']:.2f} ({sig['pnl_pct']:.2f}%) | {sig['file']}")

if __name__ == "__main__":
    main()
