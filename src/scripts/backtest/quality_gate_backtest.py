"""
Signal Quality Gate Backtest
Wraps orc_backtest.py's engine directly. Quality gates are post-confirmation filters.
"""
import glob
import logging
from datetime import time as dt_time, datetime
from collections import deque
from multiprocessing import Pool
import pandas as pd

# Reuse the proven orchestrator as base
from orc_backtest import OrcBacktestOrchestrator, run_orc_backtest

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def _calc_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains  = [max(d, 0)  for d in deltas[-period:]]
    losses = [max(-d, 0) for d in deltas[-period:]]
    avg_g  = sum(gains)  / period
    avg_l  = sum(losses) / period
    if avg_l == 0:
        return 100.0
    return 100 - (100 / (1 + avg_g / avg_l))


class GatedOrcOrchestrator(OrcBacktestOrchestrator):
    """
    Extends OrcBacktestOrchestrator with optional quality gates
    checked at the moment of confirmation before generating signals.
    """
    def __init__(self, symbol, confirm_pct=0.1, confirm_time=30,
                 tick_window=50, start_time=dt_time(9, 30), gates=None):
        super().__init__(
            symbol,
            confirm_pct=confirm_pct,
            confirm_time=confirm_time,
            tick_window=tick_window,
            start_time=start_time
        )
        self.gates = gates or {}
        self.recent_surges = deque(maxlen=20)  # track side+time of each surge

    # Override _detect_momentum_surge to also record surge events
    def _detect_momentum_surge(self, ltp, packet, current_time):
        vol_surge = packet.get('vol_surge', 1.0)
        vqs_score = packet.get('vqs_score', 0.0)
        vwap      = packet.get('vwap', ltp)

        if self.start_time and current_time.time() < self.start_time:
            return

        if vol_surge >= self.momentum_vol_surge:
            side_cand = None
            if vqs_score >= self.momentum_vqs and ltp > vwap:
                side_cand = 'LONG'
            elif vqs_score <= -self.momentum_vqs and ltp < vwap:
                side_cand = 'SHORT'

            if side_cand:
                # Record every surge event (for surge_count gate)
                self.recent_surges.append({
                    'side': side_cand,
                    'ts':   current_time.timestamp()
                })
                if self.symbol not in self.pending_confirmation:
                    self.pending_confirmation[self.symbol] = {
                        'side':          side_cand,
                        'trigger_price': ltp,
                        'trigger_time':  current_time,
                        'packet':        packet.copy()
                    }

    # Override _check_pending_confirmations to apply quality gates
    def _check_pending_confirmations(self, ltp, packet, current_time):
        if self.symbol not in self.pending_confirmation:
            return

        pending = self.pending_confirmation[self.symbol]
        elapsed = (current_time - pending['trigger_time']).total_seconds()

        if elapsed > self.confirm_time:
            del self.pending_confirmation[self.symbol]
            return

        side          = pending['side']
        trigger_price = pending['trigger_price']
        required_move = trigger_price * (self.confirm_pct / 100.0)

        confirmed = (
            (side == 'LONG'  and ltp >= trigger_price + required_move) or
            (side == 'SHORT' and ltp <= trigger_price - required_move)
        )

        if not confirmed:
            return

        del self.pending_confirmation[self.symbol]

        # ── apply quality gates ──────────────────────────────────────────────
        ph   = list(self.price_history)
        vh   = list(self.vol_history)
        vwap = packet.get('vwap', ltp)
        vqs  = packet.get('vqs_score', 0.0)

        ticks = [1 if ph[i] > ph[i-1] else (-1 if ph[i] < ph[i-1] else 0)
                 for i in range(1, len(ph))] if len(ph) > 1 else []

        g = self.gates

        # 1. Tick direction consensus
        if g.get('tick_consensus') and ticks:
            needed = g['tick_consensus']
            ratio  = (sum(1 for t in ticks if t == 1) / len(ticks)  if side == 'LONG'
                      else sum(1 for t in ticks if t == -1) / len(ticks))
            if ratio < needed:
                return

        # 2. RSI gate
        if g.get('rsi_gate'):
            ob, os_ = g['rsi_gate']
            rsi = _calc_rsi(ph)
            if side == 'LONG'  and rsi > ob:  return
            if side == 'SHORT' and rsi < os_: return

        # 3. VWAP distance
        if g.get('vwap_distance') and vwap > 0:
            dist = abs((ltp - vwap) / vwap * 100)
            if dist > g['vwap_distance']:
                return

        # 4. Consecutive surge count (same side ≥ N in last 5 min)
        if g.get('surge_count'):
            needed = g['surge_count']
            cutoff = current_time.timestamp() - 300
            same   = [s for s in self.recent_surges
                      if s['side'] == side and s['ts'] >= cutoff]
            if len(same) < needed:
                return

        # 5. Volume acceleration (last 10 ticks avg > full-window avg × factor)
        if g.get('vol_acceleration') and len(vh) >= 10:
            factor     = g['vol_acceleration']
            recent_avg = sum(vh[-10:]) / 10
            older_avg  = sum(vh) / len(vh)
            if older_avg > 0 and recent_avg / older_avg < factor:
                return

        # all gates passed → fire signal
        self._evaluate_trigger(side, ltp, pending['packet'], current_time)


# ─────────────────────────────────────────────────────────────────
# File runner
# ─────────────────────────────────────────────────────────────────
def process_file(args):
    file, gates = args
    try:
        df = pd.read_parquet(file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)
        if df.empty:
            return []
        symbol = df['symbol'].iloc[0]
        orch = GatedOrcOrchestrator(symbol, gates=gates)
        for pkt in df.to_dict('records'):
            orch.process_tick(pkt)
        for t in orch.completed_trades:
            t['symbol'] = symbol
        return orch.completed_trades
    except Exception:
        return []


def run_gate_set(gates, dates):
    out = {}
    for date in dates:
        files = glob.glob(f"data/ticks/*/{date}.parquet")
        trades = []
        with Pool(processes=8) as pool:
            for r in pool.imap_unordered(process_file, [(f, gates) for f in files]):
                trades.extend(r)
        wins   = [t for t in trades if t.get('result') == 'WIN']
        losses = [t for t in trades if t.get('result') == 'LOSS']
        total  = len(trades)
        out[date] = {
            'total':  total,
            'wins':   len(wins),
            'losses': len(losses),
            'wr':     len(wins) / total * 100 if total else 0,
            'pnl':    sum(t['pnl_pct'] for t in trades)
        }
    return out


def main():
    dates = ["2026-03-09", "2026-03-10"]

    gate_sets = [
        ("Baseline (0.1%/30s/9:30)",         {}),
        ("Tick Consensus 55%",                {'tick_consensus': 0.55}),
        ("Tick Consensus 60%",                {'tick_consensus': 0.60}),
        ("Tick Consensus 65%",                {'tick_consensus': 0.65}),
        ("RSI Gate (>65/<35)",                {'rsi_gate': (65, 35)}),
        ("RSI Gate (>70/<30)",                {'rsi_gate': (70, 30)}),
        ("VWAP Distance <0.5%",               {'vwap_distance': 0.5}),
        ("VWAP Distance <0.8%",               {'vwap_distance': 0.8}),
        ("Surge Count >=2",                   {'surge_count': 2}),
        ("Surge Count >=3",                   {'surge_count': 3}),
        ("Vol Accel 1.5x",                    {'vol_acceleration': 1.5}),
        ("Tick55% + RSI65",                   {'tick_consensus': 0.55, 'rsi_gate': (65, 35)}),
        ("Tick60% + RSI65 + VWAP0.8%",        {'tick_consensus': 0.60, 'rsi_gate': (65, 35), 'vwap_distance': 0.8}),
        ("Tick60% + Surge2",                  {'tick_consensus': 0.60, 'surge_count': 2}),
        ("Tick60% + Surge2 + RSI65",          {'tick_consensus': 0.60, 'surge_count': 2, 'rsi_gate': (65, 35)}),
        ("Surge2 + VWAP0.8% + RSI65",         {'surge_count': 2, 'vwap_distance': 0.8, 'rsi_gate': (65, 35)}),
        ("All Gates",                         {'tick_consensus': 0.60, 'rsi_gate': (65, 35), 'vwap_distance': 0.8, 'surge_count': 2, 'vol_acceleration': 1.5}),
    ]

    hdr = f"\n  {'Gate Set':<42} {'Mar9 T':>6} {'WR%':>6} {'PnL':>8}   {'Mar10 T':>7} {'WR%':>6} {'PnL':>8}"
    sep = "  " + "─" * 88
    logger.info(sep)
    logger.info(hdr)
    logger.info(sep)

    for label, gates in gate_sets:
        r   = run_gate_set(gates, dates)
        r9  = r["2026-03-09"]
        r10 = r["2026-03-10"]
        logger.info(
            f"  {label:<42} {r9['total']:>6} {r9['wr']:>5.1f}% {r9['pnl']:>+7.2f}%"
            f"   {r10['total']:>7} {r10['wr']:>5.1f}% {r10['pnl']:>+7.2f}%"
        )

    logger.info(sep)

if __name__ == "__main__":
    main()
