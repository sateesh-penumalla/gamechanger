"""
Instrument separation for contaminated symbol files.

Background
----------
DhanFeedClient keys its state by bare security_id (dhan_feed_v2.py:205) while
Dhan security IDs are only unique *within* a segment. Two instruments sharing
an ID collapse into one state entry and are published under a single symbol,
so a symbol file can interleave several instruments' packets.

Cumulative session volume is non-decreasing for any single instrument, so a
volume that drops below the running maximum cannot belong to the same
instrument. That gives a reliable separator without needing security_id
(which _publish_depth_packet strips before archiving).

Two tracks that overlap in time are concurrent instruments -> contamination.
Two tracks that run back-to-back are usually one instrument whose counter
reset (feed reconnect, day rollover) -> recoverable by concatenation.
"""

from dataclasses import dataclass, field
from typing import List

import numpy as np
import pandas as pd

# Fraction of the shorter track's time span that must overlap the longer one
# before we call two tracks concurrent rather than sequential.
CONCURRENT_OVERLAP_RATIO = 0.10


@dataclass
class Track:
    """One instrument's packet stream recovered from a mixed symbol file."""

    rows: List[int] = field(default_factory=list)
    first_volume: float = 0.0
    last_volume: float = 0.0
    t_start: pd.Timestamp = None
    t_end: pd.Timestamp = None
    ltp_mean: float = 0.0
    ltp_min: float = 0.0
    ltp_max: float = 0.0

    @property
    def n_rows(self) -> int:
        return len(self.rows)

    @property
    def volume_span(self) -> float:
        return self.last_volume - self.first_volume


def split_tracks(volumes: np.ndarray) -> List[List[int]]:
    """Partition row indices into monotonically non-decreasing volume tracks.

    Rows with volume <= 0 carry no information (pre-open packets, index ticks)
    and are left unassigned rather than forced into a track, where they would
    spawn spurious splits.

    Returns a list of row-index lists, in order of first appearance.
    """
    track_last: List[float] = []
    track_rows: List[List[int]] = []

    for i, v in enumerate(volumes):
        if not np.isfinite(v) or v <= 0:
            continue

        # Continue the track whose running total is closest below v. Choosing
        # the closest (rather than the first) keeps interleaved streams apart
        # when their volumes are of similar magnitude.
        best = -1
        best_gap = None
        for ti, last in enumerate(track_last):
            if v >= last:
                gap = v - last
                if best_gap is None or gap < best_gap:
                    best, best_gap = ti, gap

        if best < 0:
            track_last.append(v)
            track_rows.append([i])
        else:
            track_last[best] = v
            track_rows[best].append(i)

    return track_rows


def classify_tracks(df: pd.DataFrame, track_rows: List[List[int]]) -> (List[Track], str):
    """Build Track records and label the file's contamination state.

    Returns (tracks, verdict) where verdict is one of:
        "clean"        - a single instrument
        "contaminated" - concurrent instruments interleaved in time
        "reset"        - one instrument whose volume counter restarted
    """
    tracks: List[Track] = []
    for rows in track_rows:
        sub = df.iloc[rows]
        tracks.append(
            Track(
                rows=rows,
                first_volume=float(sub["volume"].iloc[0]),
                last_volume=float(sub["volume"].iloc[-1]),
                t_start=sub["ts"].iloc[0],
                t_end=sub["ts"].iloc[-1],
                ltp_mean=float(sub["ltp"].mean()),
                ltp_min=float(sub["ltp"].min()),
                ltp_max=float(sub["ltp"].max()),
            )
        )

    if len(tracks) <= 1:
        return tracks, "clean"

    # Ignore slivers: a handful of stray packets is not a second instrument.
    material = [t for t in tracks if t.n_rows >= 50]
    if len(material) <= 1:
        return tracks, "clean"

    if _any_overlap(material):
        return tracks, "contaminated"
    return tracks, "reset"


def _any_overlap(tracks: List[Track]) -> bool:
    """True if any two tracks are live at the same time."""
    for i in range(len(tracks)):
        for j in range(i + 1, len(tracks)):
            a, b = tracks[i], tracks[j]
            start = max(a.t_start, b.t_start)
            end = min(a.t_end, b.t_end)
            overlap = (end - start).total_seconds()
            if overlap <= 0:
                continue
            shorter = min(
                (a.t_end - a.t_start).total_seconds(),
                (b.t_end - b.t_start).total_seconds(),
            )
            if shorter <= 0 or overlap / shorter >= CONCURRENT_OVERLAP_RATIO:
                return True
    return False
