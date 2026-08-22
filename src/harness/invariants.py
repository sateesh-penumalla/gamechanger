"""
Invariant checks for archived depth packets.

Each check answers one question about a single symbol-day and returns a
Violation when it fails. These are the properties that were silently untrue
while the feed looked healthy, so they double as the regression gate for the
feed fixes.
"""

import os
from dataclasses import dataclass
from datetime import time as dt_time
from typing import List, Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .tracks import split_tracks, classify_tracks

SESSION_START = dt_time(9, 15)
SESSION_END = dt_time(15, 30)

# Book snapshots are nested lists; unpacking every row is slow and adds
# nothing, so the crossed-book check works on a sample.
BOOK_SAMPLE = 2000

ERROR = "ERROR"
WARN = "WARN"


@dataclass
class Violation:
    check: str
    severity: str
    detail: str
    ratio: float = 0.0  # fraction of rows affected, where meaningful


def check_single_instrument(df: pd.DataFrame) -> (Optional[Violation], str, int):
    """The file must contain exactly one instrument.

    Returns (violation, verdict, n_tracks) so the caller can record the verdict
    even when it passes.
    """
    track_rows = split_tracks(df["volume"].to_numpy(dtype="float64"))
    tracks, verdict = classify_tracks(df, track_rows)
    material = sum(1 for t in tracks if t.n_rows >= 50)

    if verdict == "contaminated":
        desc = " | ".join(
            f"{t.n_rows} rows, ltp~{t.ltp_mean:.1f}, vol {int(t.first_volume):,}->{int(t.last_volume):,}"
            for t in tracks
            if t.n_rows >= 50
        )
        return (
            Violation(
                "single_instrument",
                ERROR,
                f"{material} concurrent instruments merged: {desc}",
                1.0,
            ),
            verdict,
            material,
        )

    if verdict == "reset":
        return (
            Violation(
                "single_instrument",
                WARN,
                f"{material} sequential volume tracks (counter reset / reconnect, recoverable)",
                0.0,
            ),
            verdict,
            material,
        )

    return None, verdict, max(material, 1)


def check_volume_monotonic(df: pd.DataFrame) -> Optional[Violation]:
    """Cumulative session volume must never decrease."""
    v = df["volume"].to_numpy(dtype="float64")
    v = v[np.isfinite(v) & (v > 0)]
    if v.size < 2:
        return None
    drops = int((np.diff(v) < 0).sum())
    if drops:
        return Violation(
            "volume_monotonic",
            ERROR,
            f"{drops:,} backward steps in cumulative volume",
            drops / max(v.size - 1, 1),
        )
    return None


def check_classified_volume(df: pd.DataFrame) -> Optional[Violation]:
    """buy_vol + sell_vol are per-publish deltas and cannot exceed traded volume.

    A large shortfall is expected and is itself the finding: the tick rule in
    market_depth_service_v2.py:520-523 has no `else`, so volume printed at an
    unchanged price is discarded from both sides.
    """
    if not {"buy_vol", "sell_vol"}.issubset(df.columns):
        return None
    v = df["volume"].to_numpy(dtype="float64")
    v = v[np.isfinite(v) & (v > 0)]
    if v.size < 2:
        return None
    traded = float(v[-1] - v[0])
    classified = float(df["buy_vol"].sum() + df["sell_vol"].sum())
    if traded <= 0:
        return None

    if classified > traded * 1.01:
        return Violation(
            "classified_volume",
            ERROR,
            f"classified {classified:,.0f} > traded {traded:,.0f}",
            classified / traded,
        )

    unclassified = 1.0 - (classified / traded)
    if unclassified > 0.20:
        return Violation(
            "classified_volume",
            WARN,
            f"{unclassified*100:.1f}% of volume unclassified "
            f"(traded {traded:,.0f}, classified {classified:,.0f})",
            unclassified,
        )
    return None


def check_seed_phantom(df: pd.DataFrame) -> Optional[Violation]:
    """Catch the priming seed leaking a whole session's volume into one packet.

    _prime_from_rest seeds current_state[symbol]['volume'] with the *last
    1-minute bar's* volume, but the live feed reports *cumulative session*
    volume. On the first live tick after a mid-session start,
    diff = cumulative - one_bar, so a single packet is credited with almost
    the entire session's volume to that point. It lands in v_buy/v_sell,
    current_bar_volume, vol_history and — worst — the session VWAP
    accumulator, where it pins VWAP to the restart price.

    Signature: one packet whose classified volume is a large fraction of the
    cumulative volume standing at that moment.
    """
    if not {"buy_vol", "sell_vol"}.issubset(df.columns):
        return None
    cls = (df["buy_vol"] + df["sell_vol"]).to_numpy(dtype="float64")
    vol = df["volume"].to_numpy(dtype="float64")
    if cls.size == 0 or not np.isfinite(cls).any():
        return None

    i = int(np.nanargmax(cls))
    if vol[i] <= 0 or cls[i] <= 0:
        return None

    ratio = cls[i] / vol[i]
    if ratio >= 0.5:
        return Violation(
            "seed_phantom",
            ERROR,
            f"packet at {df['ts'].iloc[i]} credited {cls[i]:,.0f} of "
            f"{vol[i]:,.0f} cumulative ({ratio*100:.1f}%) — priming seed leak",
            ratio,
        )
    return None


def check_session_bounds(df: pd.DataFrame) -> Optional[Violation]:
    """Packets outside 09:15-15:30 should not be archived as session data."""
    t = df["ts"].dt.time
    outside = int(((t < SESSION_START) | (t > SESSION_END)).sum())
    if outside:
        return Violation(
            "session_bounds",
            WARN,
            f"{outside:,} packets outside {SESSION_START}-{SESSION_END}",
            outside / len(df),
        )
    return None


def check_positive_ltp(df: pd.DataFrame) -> Optional[Violation]:
    bad = int((~(df["ltp"] > 0)).sum())
    if bad:
        return Violation("positive_ltp", ERROR, f"{bad:,} packets with ltp <= 0", bad / len(df))
    return None


def check_pct_sum(df: pd.DataFrame) -> Optional[Violation]:
    """bid_pct and ask_pct are two halves of one book and must sum to 100."""
    if not {"bid_pct", "ask_pct"}.issubset(df.columns):
        return None
    s = df["bid_pct"] + df["ask_pct"]
    bad = int((~s.between(99.0, 101.0)).sum())
    if bad:
        return Violation(
            "pct_sum", ERROR, f"{bad:,} packets where bid_pct+ask_pct != 100", bad / len(df)
        )
    return None


def check_book_uncrossed(df: pd.DataFrame) -> Optional[Violation]:
    """Best bid must sit below best ask."""
    if not {"bids", "asks"}.issubset(df.columns):
        return None
    sub = df if len(df) <= BOOK_SAMPLE else df.sample(BOOK_SAMPLE, random_state=0)
    crossed = checked = 0
    for bids, asks in zip(sub["bids"], sub["asks"]):
        bp = _best_price(bids)
        ap = _best_price(asks)
        if bp is None or ap is None or bp <= 0 or ap <= 0:
            continue
        checked += 1
        if bp >= ap:
            crossed += 1
    if checked and crossed:
        return Violation(
            "book_uncrossed",
            ERROR,
            f"{crossed:,}/{checked:,} sampled packets have best_bid >= best_ask",
            crossed / checked,
        )
    return None


def check_duplicate_timestamps(df: pd.DataFrame) -> Optional[Violation]:
    dupes = int(df.duplicated(subset=["ts", "ltp", "volume"]).sum())
    if dupes:
        return Violation(
            "duplicate_rows", WARN, f"{dupes:,} exact duplicate packets", dupes / len(df)
        )
    return None


def check_schema_consistency(day_dir: str) -> Optional[Violation]:
    """All shards in a day directory must share one schema.

    They currently do not: bids_20/asks_20 are attached only when deep data is
    present (market_depth_service_v2.py:676-679), so shard column counts drift
    within a single day and pyarrow can refuse to read the directory.
    """
    if not os.path.isdir(day_dir):
        return None
    shapes = {}
    for f in sorted(os.listdir(day_dir)):
        if not f.endswith(".parquet"):
            continue
        try:
            names = tuple(pq.read_schema(os.path.join(day_dir, f)).names)
        except Exception:
            continue
        shapes.setdefault(names, 0)
        shapes[names] += 1
    if len(shapes) > 1:
        counts = sorted((len(k), v) for k, v in shapes.items())
        desc = ", ".join(f"{v} shards with {n} cols" for n, v in counts)
        return Violation("schema_consistency", WARN, f"schema drift across shards: {desc}", 0.0)
    return None


def _best_price(levels) -> Optional[float]:
    if levels is None:
        return None
    try:
        if len(levels) == 0:
            return None
        top = levels[0]
    except TypeError:
        return None
    if isinstance(top, dict):
        return float(top.get("price", 0) or 0)
    if isinstance(top, (list, tuple, np.ndarray)) and len(top) >= 1:
        return float(top[0])
    return None


def session_view(df: pd.DataFrame) -> pd.DataFrame:
    """Rows from continuous trading only.

    NSE runs a pre-open auction from 09:00 to 09:08 during which the book is
    legitimately crossed — orders are collected without matching until the
    equilibrium price is struck. Including those rows makes every book and
    volume check fire spuriously, so all checks except session_bounds run
    against this view.
    """
    t = df["ts"].dt.time
    return df[(t >= SESSION_START) & (t <= SESSION_END)].reset_index(drop=True)


def check_all(df: pd.DataFrame, day_dir: str = None) -> (List[Violation], dict):
    """Run every check against one symbol-day. Returns (violations, summary)."""
    violations: List[Violation] = []

    # session_bounds is the one check that must see the unfiltered frame.
    v = check_session_bounds(df)
    if v:
        violations.append(v)

    sdf = session_view(df)
    if len(sdf) < 20:
        summary = {
            "rows": len(df), "session_rows": len(sdf), "verdict": "no_session",
            "n_tracks": 0, "errors": 0, "warns": len(violations),
        }
        return violations, summary

    inst_v, verdict, n_tracks = check_single_instrument(sdf)
    if inst_v:
        violations.append(inst_v)

    for check in (
        check_volume_monotonic,
        check_seed_phantom,
        check_classified_volume,
        check_positive_ltp,
        check_pct_sum,
        check_book_uncrossed,
        check_duplicate_timestamps,
    ):
        v = check(sdf)
        if v:
            violations.append(v)

    if day_dir:
        v = check_schema_consistency(day_dir)
        if v:
            violations.append(v)

    summary = {
        "rows": len(df),
        "session_rows": len(sdf),
        "verdict": verdict,
        "n_tracks": n_tracks,
        "errors": sum(1 for v in violations if v.severity == ERROR),
        "warns": sum(1 for v in violations if v.severity == WARN),
    }
    return violations, summary
