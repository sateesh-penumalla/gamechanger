"""
Market Regime Filter
--------------------
Precomputes a rolling VQS (trend score) for NIFTY from tick data.
Returns a lookup function: given a datetime, returns ('BULL', 'BEAR', or 'NEUTRAL').

Usage:
    from market_regime import load_regime_filter
    get_regime = load_regime_filter("data/ticks/NIFTY/2026-03-10.parquet")
    regime = get_regime(signal_time)   # 'BULL' | 'BEAR' | 'NEUTRAL'
"""

import pandas as pd
from collections import deque
import bisect


def load_regime_filter(nifty_parquet_path: str, tick_window: int = 50, vqs_threshold: float = 0.15):
    """
    Reads the NIFTY parquet, computes a rolling VQS at each timestamp,
    and returns a fast lookup callable.

    vqs_threshold: minimum absolute VQS to call BULL/BEAR (below = NEUTRAL)
    """
    try:
        df = pd.read_parquet(nifty_parquet_path)
    except Exception as e:
        print(f"[regime] Could not load {nifty_parquet_path}: {e}. Defaulting to NEUTRAL always.")
        return lambda t: 'NEUTRAL'

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)

    price_history = deque(maxlen=tick_window)
    timestamps = []
    regimes = []

    for row in df.to_dict('records'):
        ltp = row['ltp']
        price_history.append(ltp)
        history = list(price_history)

        if len(history) > 1:
            ticks = [1 if history[i] > history[i-1] else (-1 if history[i] < history[i-1] else 0)
                     for i in range(1, len(history))]
            vqs = sum(ticks) / len(ticks)
        else:
            vqs = 0.0

        if vqs >= vqs_threshold:
            regime = 'BULL'
        elif vqs <= -vqs_threshold:
            regime = 'BEAR'
        else:
            regime = 'NEUTRAL'

        ts = row['timestamp']
        if hasattr(ts, 'to_pydatetime'):
            ts = ts.to_pydatetime()

        timestamps.append(ts)
        regimes.append(regime)

    def get_regime(query_time):
        if not timestamps:
            return 'NEUTRAL'
        idx = bisect.bisect_right(timestamps, query_time) - 1
        if idx < 0:
            return 'NEUTRAL'
        return regimes[idx]

    return get_regime
