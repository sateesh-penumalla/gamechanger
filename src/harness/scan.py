"""
Archive health scanner.

Walks data/ticks/<SYMBOL>/<DATE>.parquet/ and reports, per symbol-day, whether
the packets satisfy the invariants the live pipeline assumes. Read-only.

    python3 -m src.harness.scan --date 2026-03-20
    python3 -m src.harness.scan --symbol RELIANCE
    python3 -m src.harness.scan --sample 25 --out reports/archive_health.csv
    python3 -m src.harness.scan --all --workers 8 --out reports/archive_health.csv
"""

import argparse
import os
import random
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd

from .invariants import check_all, ERROR

BASE = os.path.join("data", "ticks")

NEEDED = [
    "timestamp", "ltp", "volume", "buy_vol", "sell_vol",
    "bid_pct", "ask_pct", "bids", "asks",
]


from src.utils.storage_manager import normalize_ist as _normalize_ts  # noqa: E402

# The archive mixes representations: shards written before `.replace(tzinfo=None)`
# was dropped from _publish_depth_packet are naive IST, later ones carry +05:30.
# Each shard is normalized before it is concatenated rather than after, because
# concatenating the two first produces an object column that pandas then half
# converts to NaT. normalize_ist is shared with the writer so the reader and the
# writer can never disagree about what a timestamp means.


def load_symbol_day(day_dir: str) -> pd.DataFrame:
    """Read every shard in a day directory into one frame.

    Reads shard-by-shard rather than handing the directory to pyarrow, because
    schema drift between shards makes the directory read fail outright. A
    corrupt shard is skipped instead of losing the whole day.
    """
    frames = []
    dropped_shards = 0
    for name in sorted(os.listdir(day_dir)):
        if not name.endswith(".parquet"):
            continue
        path = os.path.join(day_dir, name)
        try:
            df = pd.read_parquet(path)
        except Exception:
            dropped_shards += 1
            continue
        keep = [c for c in NEEDED if c in df.columns]
        if not {"timestamp", "ltp", "volume"}.issubset(keep):
            dropped_shards += 1
            continue
        df = df[keep].copy()
        df["ts"] = _normalize_ts(df["timestamp"])
        frames.append(df.drop(columns=["timestamp"]))

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True, sort=False)
    before = len(out)
    out = out.dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    out.attrs["dropped_shards"] = dropped_shards
    out.attrs["dropped_rows"] = before - len(out)
    return out


def scan_one(symbol: str, date: str, root: str = None) -> dict:
    day_dir = os.path.join(root or BASE, symbol, f"{date}.parquet")
    rec = {"symbol": symbol, "date": date, "rows": 0, "verdict": "empty",
           "n_tracks": 0, "errors": 0, "warns": 0, "detail": ""}
    try:
        df = load_symbol_day(day_dir)
    except Exception as e:
        rec["verdict"] = "unreadable"
        rec["detail"] = f"{type(e).__name__}: {e}"
        return rec

    if df.empty or len(df) < 20:
        rec["rows"] = len(df)
        return rec

    violations, summary = check_all(df, day_dir=day_dir)
    rec.update(summary)
    rec["detail"] = " ;; ".join(f"[{v.severity}] {v.check}: {v.detail}" for v in violations)
    rec["checks_failed"] = ",".join(v.check for v in violations)
    return rec


def discover(symbol_filter=None, date_filter=None, root=None):
    """Yield (symbol, date) pairs present in the archive."""
    root = root or BASE
    if not os.path.isdir(root):
        return
    for symbol in sorted(os.listdir(root)):
        sym_dir = os.path.join(root, symbol)
        if not os.path.isdir(sym_dir):
            continue
        if symbol_filter and symbol != symbol_filter:
            continue
        for entry in sorted(os.listdir(sym_dir)):
            if not entry.endswith(".parquet"):
                continue
            date = entry[: -len(".parquet")]
            if date_filter and date != date_filter:
                continue
            if not os.path.isdir(os.path.join(sym_dir, entry)):
                continue
            yield symbol, date


def _job(args):
    return scan_one(*args)


def main():
    ap = argparse.ArgumentParser(description="Validate the archived depth stream.")
    ap.add_argument("--date", help="single date, e.g. 2026-03-20")
    ap.add_argument("--symbol", help="single symbol, e.g. RELIANCE")
    ap.add_argument("--sample", type=int, help="random sample of N symbol-days")
    ap.add_argument("--all", action="store_true", help="scan the entire archive")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--out", help="write per-symbol-day results to this CSV")
    ap.add_argument("--root", default=BASE,
                    help="archive root to scan (e.g. data/ticks_normalized)")
    args = ap.parse_args()

    if not (args.date or args.symbol or args.sample or args.all):
        ap.error("pick a scope: --date, --symbol, --sample N, or --all")

    targets = list(discover(args.symbol, args.date, args.root))
    if args.sample and len(targets) > args.sample:
        random.seed(0)
        targets = random.sample(targets, args.sample)

    if not targets:
        print("No matching symbol-days found under data/ticks/")
        return 1

    print(f"Scanning {len(targets)} symbol-days from {args.root}...\n", flush=True)

    records = []
    if args.workers > 1:
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(_job, (t[0], t[1], args.root)): t for t in targets}
            for i, fut in enumerate(as_completed(futures), 1):
                records.append(fut.result())
                if i % 25 == 0:
                    print(f"  ...{i}/{len(targets)}", flush=True)
    else:
        for i, t in enumerate(targets, 1):
            records.append(scan_one(t[0], t[1], args.root))
            if i % 25 == 0:
                print(f"  ...{i}/{len(targets)}", flush=True)

    report(records)

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        pd.DataFrame(records).to_csv(args.out, index=False)
        print(f"\nPer-symbol-day results written to {args.out}")

    return 0


def report(records):
    df = pd.DataFrame(records)
    total = len(df)
    verdicts = Counter(df["verdict"])

    print("=" * 78)
    print("ARCHIVE HEALTH")
    print("=" * 78)
    print(f"symbol-days scanned : {total}")
    print(f"packets             : {int(df['rows'].sum()):,}")
    print()

    print("Instrument integrity")
    for name, label in (
        ("clean", "single instrument"),
        ("reset", "one instrument, counter reset (recoverable as-is)"),
        ("contaminated", "MULTIPLE instruments merged (needs splitting)"),
        ("empty", "no usable packets"),
        ("unreadable", "could not be read"),
    ):
        n = verdicts.get(name, 0)
        if n:
            print(f"  {name:<13} {n:>5}  ({n/total*100:5.1f}%)  {label}")
    print()

    failed = Counter()
    for s in df.get("checks_failed", pd.Series(dtype=str)).fillna(""):
        for c in filter(None, s.split(",")):
            failed[c] += 1
    if failed:
        print("Failing checks (symbol-days)")
        for check, n in failed.most_common():
            print(f"  {check:<22} {n:>5}  ({n/total*100:5.1f}%)")
        print()

    usable = verdicts.get("clean", 0) + verdicts.get("reset", 0)
    splittable = verdicts.get("contaminated", 0)
    print("Recoverability")
    print(f"  usable now              : {usable:>5}  ({usable/total*100:5.1f}%)")
    print(f"  usable after splitting  : {splittable:>5}  ({splittable/total*100:5.1f}%)")
    print(f"  lost                    : {total-usable-splittable:>5}")

    worst = df[df["errors"] > 0].head(5)
    if not worst.empty:
        print("\nExamples")
        for _, r in worst.iterrows():
            print(f"  {r['symbol']} {r['date']}: {str(r['detail'])[:150]}")
    print("=" * 78)


if __name__ == "__main__":
    sys.exit(main())
