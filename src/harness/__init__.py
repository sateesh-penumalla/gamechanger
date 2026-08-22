"""
Offline validation harness for the archived market-depth stream.

Runs entirely against data/ticks/<SYMBOL>/<DATE>.parquet/ — never touches the
live service. Use it to (a) quantify how much of the existing archive is
usable, and (b) as a regression gate once the feed fixes land.

    python3 -m src.harness.scan --date 2026-03-20
    python3 -m src.harness.scan --sample 25 --out report.csv
"""

from .tracks import split_tracks, classify_tracks, Track
from .invariants import check_all, Violation

__all__ = ["split_tracks", "classify_tracks", "Track", "check_all", "Violation"]
