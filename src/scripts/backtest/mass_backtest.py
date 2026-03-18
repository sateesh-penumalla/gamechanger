import os
import glob
import logging
from multiprocessing import Pool
from momentum_backtest import run_backtest
from orc_backtest import run_orc_backtest

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def process_file_orcpure(file):
    try:
        signals = run_backtest(file, quiet=True)
        for s in signals: s['file'] = file
        return signals
    except Exception as e:
        return []

def process_file_orc(file):
    try:
        signals = run_orc_backtest(file, quiet=True)
        for s in signals: s['file'] = file
        return signals
    except Exception as e:
        return []

def main():
    target_date = "2026-03-09"
    search_pattern = f"data/ticks/*/{target_date}.parquet"
    
    files = glob.glob(search_pattern)
    logger.info(f"Found {len(files)} parquet files for {target_date}.")
    
    # 1. ORCPURE LOGIC
    total_signals_orcpure = []
    with Pool(processes=8) as pool:
        for i, result in enumerate(pool.imap_unordered(process_file_orcpure, files)):
            total_signals_orcpure.extend(result)
            
    # 2. ORC LOGIC
    total_signals_orc = []
    with Pool(processes=8) as pool:
        for i, result in enumerate(pool.imap_unordered(process_file_orc, files)):
            total_signals_orc.extend(result)
            
    logger.info("====================================")
    logger.info(f"BACKTEST SUMMARY FOR {target_date}")
    logger.info("====================================")
    logger.info(f"Total Signals [orcpure.py / v2 High Precision]: {len(total_signals_orcpure)}")
    logger.info(f"Total Signals [orc.py / Legacy Gold Guard]: {len(total_signals_orc)}")
    
    if total_signals_orc:
        logger.info("--- ORC.PY SIGNALS ---")
        for idx, sig in enumerate(total_signals_orc):
            logger.info(f"[{idx+1}] {sig['time'].time()} | {sig['side']} | Entry: {sig['entry']} | SL: {sig['sl']:.2f} | TP: {sig['tp']:.2f} | File: {sig['file']}")

if __name__ == "__main__":
    main()
