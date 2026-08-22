"""
One-off migration: rewrite the tick archive with consistent timestamps.

Three defects are repaired:

1. Mixed tz representations. Roughly half the shards carry naive IST and half
   carry +05:30. pd.read_parquet over such a directory converts the aware half
   to UTC and strips the tz, shifting it back 5.5 hours with no error — which
   silently corrupts any research run over the archive.

2. Wrong day partition. Shards were filed under datetime.now() at flush time
   rather than the packet's own timestamp, so e.g. HAVELLS/2026-03-20.parquet/
   holds packets stamped 2026-03-21 04:24.

3. Shard sprawl and schema drift. A day directory can hold 400+ tiny shards
   whose column sets differ (bids_20/asks_20 are attached only when deep data
   is present), which makes a whole-directory read fail outright.

Writes to a NEW tree by default and never mutates the source, so the migration
can be verified before anything is swapped.

    python3 -m src.harness.normalize --symbol RELIANCE --dry-run
    python3 -m src.harness.normalize --all --workers 6
"""

import argparse
import os
import shutil
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd

from src.utils.storage_manager import normalize_ist

SRC_ROOT = os.path.join("data", "ticks")
DST_ROOT = os.path.join("data", "ticks_normalized")


def _read_shard(path: str):
    """Read one shard with its timestamps normalized to naive IST."""
    df = pd.read_parquet(path)
    if "timestamp" not in df.columns:
        return None
    df["timestamp"] = normalize_ist(df["timestamp"])
    return df.dropna(subset=["timestamp"])


def normalize_symbol(symbol: str, src_root: str, dst_root: str, dry_run: bool) -> dict:
    """Rewrite every day directory for one symbol.

    Rows are regrouped by their own date, so packets that spilled into the
    wrong day directory land in the right one. Everything for a symbol is
    processed together because a spilled row's true day may be a directory
    that has already been visited.
    """
    stat = {
        "symbol": symbol, "src_shards": 0, "src_rows": 0, "out_rows": 0,
        "out_days": 0, "moved_rows": 0, "bad_shards": 0, "dropped_rows": 0,
    }
    sym_dir = os.path.join(src_root, symbol)
    if not os.path.isdir(sym_dir):
        return stat

    by_date = defaultdict(list)

    for entry in sorted(os.listdir(sym_dir)):
        if not entry.endswith(".parquet"):
            continue
        day_path = os.path.join(sym_dir, entry)
        src_date = entry[: -len(".parquet")]

        shard_paths = []
        if os.path.isdir(day_path):
            shard_paths = [
                os.path.join(day_path, f)
                for f in sorted(os.listdir(day_path))
                if f.endswith(".parquet")
            ]
        elif os.path.isfile(day_path):
            shard_paths = [day_path]

        for sp in shard_paths:
            stat["src_shards"] += 1
            try:
                df = _read_shard(sp)
            except Exception:
                stat["bad_shards"] += 1
                continue
            if df is None or df.empty:
                stat["bad_shards"] += 1
                continue

            stat["src_rows"] += len(df)
            for d, grp in df.groupby(df["timestamp"].dt.date):
                d = str(d)
                if d != src_date:
                    stat["moved_rows"] += len(grp)
                by_date[d].append(grp)

    if not by_date:
        return stat

    for d, frames in by_date.items():
        out = pd.concat(frames, ignore_index=True, sort=False)
        before = len(out)
        # Duplicates arise where a shard was written twice after a retry.
        out = out.drop_duplicates(subset=[c for c in ("timestamp", "ltp", "volume") if c in out.columns])
        out = out.sort_values("timestamp").reset_index(drop=True)
        stat["dropped_rows"] += before - len(out)
        stat["out_rows"] += len(out)
        stat["out_days"] += 1

        if dry_run:
            continue

        day_dir = os.path.join(dst_root, symbol, f"{d}.parquet")
        os.makedirs(day_dir, exist_ok=True)
        final = os.path.join(day_dir, "part-000.parquet")
        tmp = final + ".tmp"
        out.to_parquet(tmp, compression="snappy", index=False)
        os.replace(tmp, final)

    return stat


def _job(args):
    try:
        return normalize_symbol(*args)
    except Exception as e:
        return {"symbol": args[0], "error": f"{type(e).__name__}: {e}",
                "src_shards": 0, "src_rows": 0, "out_rows": 0, "out_days": 0,
                "moved_rows": 0, "bad_shards": 0, "dropped_rows": 0}


def main():
    ap = argparse.ArgumentParser(description="Normalize the tick archive.")
    ap.add_argument("--symbol", help="single symbol")
    ap.add_argument("--all", action="store_true", help="every symbol")
    ap.add_argument("--src", default=SRC_ROOT)
    ap.add_argument("--dst", default=DST_ROOT)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--dry-run", action="store_true", help="report without writing")
    args = ap.parse_args()

    if not (args.symbol or args.all):
        ap.error("pick a scope: --symbol SYM or --all")

    symbols = (
        [args.symbol] if args.symbol
        else sorted(s for s in os.listdir(args.src) if os.path.isdir(os.path.join(args.src, s)))
    )

    if not args.dry_run:
        free = shutil.disk_usage(os.path.dirname(os.path.abspath(args.src))).free
        print(f"Writing to {args.dst}  (free disk: {free/1e9:.1f} GB)")
    print(f"{'DRY RUN — ' if args.dry_run else ''}Normalizing {len(symbols)} symbols...\n", flush=True)

    jobs = [(s, args.src, args.dst, args.dry_run) for s in symbols]
    results = []
    if args.workers > 1:
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(_job, j) for j in jobs]
            for i, f in enumerate(as_completed(futs), 1):
                results.append(f.result())
                if i % 20 == 0:
                    print(f"  ...{i}/{len(jobs)}", flush=True)
    else:
        for i, j in enumerate(jobs, 1):
            results.append(_job(j))
            if i % 20 == 0:
                print(f"  ...{i}/{len(jobs)}", flush=True)

    df = pd.DataFrame(results)
    print("\n" + "=" * 70)
    print("ARCHIVE NORMALIZATION" + (" (DRY RUN)" if args.dry_run else ""))
    print("=" * 70)
    print(f"symbols            : {len(df)}")
    print(f"source shards read : {int(df['src_shards'].sum()):,}")
    print(f"source rows        : {int(df['src_rows'].sum()):,}")
    print(f"output rows        : {int(df['out_rows'].sum()):,}")
    print(f"output day-files   : {int(df['out_days'].sum()):,}")
    print(f"rows moved to the correct day : {int(df['moved_rows'].sum()):,}")
    print(f"duplicate rows dropped        : {int(df['dropped_rows'].sum()):,}")
    print(f"unreadable shards             : {int(df['bad_shards'].sum()):,}")
    if "error" in df.columns:
        errs = df[df["error"].notna()]
        if not errs.empty:
            print(f"\nsymbols with errors: {len(errs)}")
            for _, r in errs.head(5).iterrows():
                print(f"  {r['symbol']}: {r['error']}")
    if int(df["src_shards"].sum()):
        print(f"\nshard reduction    : {int(df['src_shards'].sum()):,} -> {int(df['out_days'].sum()):,}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
