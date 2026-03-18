"""
Loss Analysis Script
Runs backtest on both dates, captures detailed trade data,
and analyses patterns in losing trades vs winning trades.
"""
import glob
import logging
import pandas as pd
from datetime import datetime, time as dt_time
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
            regime_filter=regime_filter
        )
        symbol = file.split('/')[-2]
        date = file.split('/')[-1].replace('.parquet', '')
        for t in completed:
            t['symbol'] = symbol
            t['date'] = date
        return completed
    except Exception as e:
        return []

def analyse_trades(trades, label):
    if not trades:
        logger.info(f"\n[{label}] No trades to analyse.")
        return

    wins   = [t for t in trades if t.get('result') == 'WIN']
    losses = [t for t in trades if t.get('result') == 'LOSS']

    logger.info(f"\n{'='*70}")
    logger.info(f"  ANALYSIS: {label}")
    logger.info(f"  Total: {len(trades)} | Wins: {len(wins)} | Losses: {len(losses)}")
    logger.info(f"{'='*70}")

    # ── 1. Time-of-Day buckets ──────────────────────────────────────────
    def tod_bucket(t_obj):
        h = t_obj.hour
        m = t_obj.minute
        mins = h * 60 + m
        if mins < 9*60+45:           return '09:15–09:45 (Open)'
        elif mins < 10*60+30:        return '09:45–10:30'
        elif mins < 11*60+30:        return '10:30–11:30'
        elif mins < 13*60:           return '11:30–13:00'
        elif mins < 14*60+30:        return '13:00–14:30'
        else:                        return '14:30–15:00 (Close)'

    win_tod   = {}
    loss_tod  = {}
    for t in wins:
        b = tod_bucket(t['time'])
        win_tod[b]  = win_tod.get(b, 0) + 1
    for t in losses:
        b = tod_bucket(t['time'])
        loss_tod[b] = loss_tod.get(b, 0) + 1

    all_buckets = sorted(set(list(win_tod.keys()) + list(loss_tod.keys())))
    logger.info("\n  📅 Time-of-Day Breakdown:")
    logger.info(f"  {'Bucket':<30} {'W':>5} {'L':>5} {'Win%':>7}")
    logger.info(f"  {'-'*50}")
    for b in all_buckets:
        w = win_tod.get(b, 0)
        l = loss_tod.get(b, 0)
        total = w + l
        wp = f"{w/total*100:.0f}%" if total > 0 else '-'
        logger.info(f"  {b:<30} {w:>5} {l:>5} {wp:>7}")

    # ── 2. Vol Surge at entry ──────────────────────────────────────────
    def surge_bucket(s):
        if s is None:  return 'unknown'
        if s < 15:     return '12–15x'
        elif s < 25:   return '15–25x'
        elif s < 50:   return '25–50x'
        else:          return '50x+'

    win_surge  = {}
    loss_surge = {}
    for t in wins:
        b = surge_bucket(t.get('vol_surge'))
        win_surge[b]  = win_surge.get(b, 0) + 1
    for t in losses:
        b = surge_bucket(t.get('vol_surge'))
        loss_surge[b] = loss_surge.get(b, 0) + 1

    all_sb = ['12–15x', '15–25x', '25–50x', '50x+', 'unknown']
    logger.info("\n  🚀 Vol Surge at Entry:")
    logger.info(f"  {'Surge Range':<15} {'W':>5} {'L':>5} {'Win%':>7}")
    logger.info(f"  {'-'*35}")
    for b in all_sb:
        w = win_surge.get(b, 0)
        l = loss_surge.get(b, 0)
        total = w + l
        if total == 0: continue
        wp = f"{w/total*100:.0f}%"
        logger.info(f"  {b:<15} {w:>5} {l:>5} {wp:>7}")

    # ── 3. VWAP Distance at entry ──────────────────────────────────────
    def vwap_bucket(t):
        entry = t.get('entry', 0)
        vwap  = t.get('vwap', entry)
        if not vwap or vwap == 0: return 'unknown'
        pct = abs((entry - vwap) / vwap * 100)
        if pct < 0.2:   return '< 0.2%'
        elif pct < 0.5: return '0.2–0.5%'
        elif pct < 1.0: return '0.5–1.0%'
        else:           return '> 1.0%'

    win_vwap  = {}
    loss_vwap = {}
    for t in wins:
        b = vwap_bucket(t)
        win_vwap[b]  = win_vwap.get(b, 0) + 1
    for t in losses:
        b = vwap_bucket(t)
        loss_vwap[b] = loss_vwap.get(b, 0) + 1

    all_vb = ['< 0.2%', '0.2–0.5%', '0.5–1.0%', '> 1.0%', 'unknown']
    logger.info("\n  📐 VWAP Distance at Entry:")
    logger.info(f"  {'Distance':<15} {'W':>5} {'L':>5} {'Win%':>7}")
    logger.info(f"  {'-'*35}")
    for b in all_vb:
        w = win_vwap.get(b, 0)
        l = loss_vwap.get(b, 0)
        total = w + l
        if total == 0: continue
        wp = f"{w/total*100:.0f}%"
        logger.info(f"  {b:<15} {w:>5} {l:>5} {wp:>7}")

    # ── 4. Side breakdown ─────────────────────────────────────────────
    win_long  = sum(1 for t in wins   if t.get('side') == 'LONG')
    win_short = sum(1 for t in wins   if t.get('side') == 'SHORT')
    loss_long = sum(1 for t in losses if t.get('side') == 'LONG')
    loss_short= sum(1 for t in losses if t.get('side') == 'SHORT')
    logger.info(f"\n  ↕️  Side Breakdown:")
    logger.info(f"  LONG  → {win_long}W / {loss_long}L  (WinRate: {win_long/(win_long+loss_long)*100:.0f}%)" if (win_long+loss_long)>0 else "  LONG  → no trades")
    logger.info(f"  SHORT → {win_short}W / {loss_short}L  (WinRate: {win_short/(win_short+loss_short)*100:.0f}%)" if (win_short+loss_short)>0 else "  SHORT → no trades")

    # ── 5. Worst losing trades ─────────────────────────────────────────
    losses_sorted = sorted(losses, key=lambda t: t.get('pnl_pct', 0))
    logger.info(f"\n  ❌ Top 10 Losing Trades:")
    logger.info(f"  {'Symbol':<18} {'Side':<6} {'Time':<10} {'Entry':>8} {'Exit':>8} {'PnL%':>7} {'Surge':>7}")
    logger.info(f"  {'-'*68}")
    for t in losses_sorted[:10]:
        sym    = t.get('symbol', '?')
        side   = t.get('side', '?')
        tod    = t['time'].strftime('%H:%M:%S') if 'time' in t else '?'
        entry  = t.get('entry', 0)
        exit_p = t.get('exit_price', 0)
        pnl    = t.get('pnl_pct', 0)
        surge  = t.get('vol_surge') or 0
        logger.info(f"  {sym:<18} {side:<6} {tod:<10} {entry:>8.2f} {exit_p:>8.2f} {pnl:>7.2f}% {surge:>7.1f}x")


def main():
    params = {'confirm_pct': 0.1, 'confirm_time': 30, 'tick_window': 50}

    for target_date in ["2026-03-09", "2026-03-10"]:
        nifty_path = f"data/ticks/NIFTY/{target_date}.parquet"
        files = glob.glob(f"data/ticks/*/{target_date}.parquet")
        logger.info(f"\nLoading {len(files)} files for {target_date}...")

        args_list = [(f, params, nifty_path) for f in files]
        all_trades = []
        with Pool(processes=8) as pool:
            for result in pool.imap_unordered(process_file, args_list):
                all_trades.extend(result)

        analyse_trades(all_trades, f"March {target_date.split('-')[2]}")

if __name__ == "__main__":
    main()
