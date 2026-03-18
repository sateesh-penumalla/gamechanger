import glob
import logging
from datetime import time as dt_time
from multiprocessing import Pool
from orc_backtest import run_orc_backtest
from market_regime import load_regime_filter

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

_regime_cache = {}

def _get_regime_filter(nifty_path):
    if nifty_path not in _regime_cache:
        _regime_cache[nifty_path] = load_regime_filter(nifty_path) if nifty_path else None
    return _regime_cache[nifty_path]

def process_file(args):
    file, params, nifty_path = args
    try:
        regime_filter = _get_regime_filter(nifty_path)
        signals, completed = run_orc_backtest(
            file, quiet=True,
            confirm_pct=params['confirm_pct'],
            confirm_time=params['confirm_time'],
            tick_window=params['tick_window'],
            regime_filter=regime_filter,
            start_time=params['start_time']
        )
        for s in completed:
            s['file'] = file
        return completed
    except Exception as e:
        return []

def run_for_date(target_date, start_time):
    nifty_path = f"data/ticks/NIFTY/{target_date}.parquet"
    files = glob.glob(f"data/ticks/*/{target_date}.parquet")
    label = f"Open filter: {start_time.strftime('%H:%M')}" if start_time else "No filter (09:15)"

    params = {'confirm_pct': 0.1, 'confirm_time': 30, 'tick_window': 50, 'start_time': start_time}
    logger.info(f"\n--- {target_date} | {label} ---")

    args_list = [(f, params, nifty_path) for f in files]
    completed_trades = []
    with Pool(processes=8) as pool:
        for result in pool.imap_unordered(process_file, args_list):
            completed_trades.extend(result)

    if not completed_trades:
        logger.info("-> No trades executed.")
        return

    wins   = [t for t in completed_trades if t.get('result') == 'WIN']
    losses = [t for t in completed_trades if t.get('result') == 'LOSS']
    win_rate = len(wins) / len(completed_trades) * 100
    pnl = sum(t['pnl_pct'] for t in completed_trades)

    logger.info(f"-> Trades: {len(completed_trades)} | WinRate: {win_rate:.1f}% ({len(wins)}W/{len(losses)}L) | PnL: {pnl:+.2f}%")

    win_durations = [(t['exit_time'] - t['time']).total_seconds()
                     for t in wins if 'exit_time' in t and 'time' in t]
    if win_durations:
        logger.info(f"-> Time-to-TP: avg={sum(win_durations)/len(win_durations):.0f}s  min={min(win_durations):.0f}s  max={max(win_durations):.0f}s")

def main():
    dates = ["2026-03-09", "2026-03-10"]
    filters = [
        None,                   # baseline — trade from 09:15
        dt_time(9, 25),         # skip until 09:25
        dt_time(9, 30),         # skip until 09:30
    ]

    for start_time in filters:
        logger.info(f"\n{'='*60}")
        for date in dates:
            run_for_date(date, start_time)

if __name__ == "__main__":
    main()

