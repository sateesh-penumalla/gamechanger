"""
Rebuild derived order-flow columns from the raw archived fields.

The archive stores both raw feed values and values the service derived from
them. The raw ones (`ltp`, cumulative `volume`, `bids`, `asks`) are sound. The
derived ones carry two defects:

1. Priming seed leak. current_state[symbol]['volume'] was seeded from the last
   1-minute BAR's volume while the feed reports CUMULATIVE session volume, so
   the first live tick after a mid-session start computed
   diff = cumulative - one_bar and credited a single packet with nearly the
   whole session. That phantom flowed into buy_vol/sell_vol, current_bar_volume
   and — worst — the VWAP accumulator, which never recovers for the rest of the
   session.

2. Flat-price volume discarded. The old tick rule had no `else`, so volume that
   printed at an unchanged price was dropped from both sides: 4-13% of every
   session, and biased, because the trades it *could* classify were exactly the
   ones that moved the price.

Recomputing from cumulative `volume` removes the phantom by construction — the
per-packet delta is genuine regardless of what the service's seed was — and
reclassifying with src.utils.trade_classifier (the same function the live path
now uses) closes the flat-price gap.

Emitted / replaced columns:
    vol_delta   per-packet traded volume  (NEW — the field every consumer
                needed and had to guess at; `volume` stays cumulative)
    buy_vol     recomputed, Lee-Ready
    sell_vol    recomputed, Lee-Ready
    side        'B' / 'S' / None for this packet
    side_src    'quote' / 'tick' / 'carry' / 'split'
    vwap        recomputed session VWAP, free of the phantom

Run after src.harness.normalize. Reads a normalized tree, writes a new one.

    python3 -m src.harness.reprocess --symbol HAVELLS --dry-run
    python3 -m src.harness.reprocess --all --workers 6
"""

import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

from src.harness.invariants import _best_price, SESSION_START, SESSION_END
from src.utils.storage_manager import normalize_ist
from src.utils.trade_classifier import classify_trade_side, BUY, SELL

SRC_ROOT = os.path.join("data", "ticks_normalized")
DST_ROOT = os.path.join("data", "ticks_clean")

# A packet crediting this share of standing cumulative volume is the seed
# phantom, not a trade.
PHANTOM_RATIO = 0.5

# A hole this long is unambiguously a delivery failure rather than ordinary
# sparse sampling: NSE trades continuously, so 30s of silence in a name that
# then reports volume means packets were lost.
GAP_SECONDS = 30.0


def reprocess_frame(df: pd.DataFrame) -> (pd.DataFrame, dict):
    """Recompute vol_delta, buy/sell classification and VWAP for one symbol-day."""
    df = df.sort_values("timestamp").reset_index(drop=True)

    ltp = pd.to_numeric(df["ltp"], errors="coerce").fillna(0.0).to_numpy("float64")
    vol = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0).to_numpy("float64")

    # Per-packet traded volume, taken against a RUNNING MAXIMUM of the
    # cumulative counter rather than the raw previous value.
    #
    # The counter occasionally drops mid-session and recovers a packet later
    # (feed reconnect, or a transient bogus reading) — e.g. JINDALSTEL
    # 2026-03-02 goes 264,584 -> 166 -> 466,464 within 400ms. Simply clipping
    # the negative step leaves the *recovery* step fabricating a 466,298 delta
    # out of the 166. Diffing the running maximum instead yields 201,880: the
    # genuine volume traded since the last trustworthy reading.
    running = np.maximum.accumulate(vol)
    n_backward = int((np.diff(vol, prepend=vol[0]) < 0).sum())
    dv = np.diff(running, prepend=running[0])
    dv[dv < 0] = 0.0

    # Every volume-bearing packet aggregates the trades since the previous one
    # — this is snapshot data, not per-trade data — so "how much aggregation"
    # is a continuum, not a yes/no. secs_since_prev exposes it directly and
    # lets analysis pick its own tolerance.
    #
    # vol_gap is reserved for the unambiguous cases: the counter dropped and
    # recovered, or the feed went silent for GAP_SECONDS and then reported
    # volume (e.g. GRASIM 2026-03-18, a 65s hole covering 135,474 shares).
    # An earlier 10x-median rule flagged 48% of all volume, which conflated
    # genuine outages with normal sparseness on quieter symbols.
    below = vol < np.concatenate([[vol[0]], running[:-1]])
    secs = df["timestamp"].diff().dt.total_seconds().fillna(0.0).to_numpy("float64")

    gap = np.zeros(len(vol), dtype=bool)
    pending = False
    for i in range(len(vol)):
        if below[i]:
            pending = True
        if dv[i] > 0 and (pending or secs[i] > GAP_SECONDS):
            gap[i] = True
            pending = False

    # Best bid/ask as they stood BEFORE each trade: the archived packet carries
    # the post-update book, so shift it forward by one row.
    bid = np.array([_best_price(b) or 0.0 for b in df.get("bids", [None] * len(df))])
    ask = np.array([_best_price(a) or 0.0 for a in df.get("asks", [None] * len(df))])
    bid = np.concatenate([[0.0], bid[:-1]])
    ask = np.concatenate([[0.0], ask[:-1]])

    buy = np.zeros(len(df))
    sell = np.zeros(len(df))
    sides = np.empty(len(df), dtype=object)
    srcs = np.empty(len(df), dtype=object)

    last_trade_ltp = 0.0
    last_side = None
    src_counts = {"quote": 0, "tick": 0, "carry": 0, "split": 0}

    for i in range(len(df)):
        if dv[i] <= 0:
            # No trade in this packet — a quote-only update. last_trade_ltp must
            # keep pointing at the last actual trade, so it is deliberately not
            # advanced here. (The live path had the opposite bug: it advanced
            # prev_ltp on every tick, which blinded the tick rule.)
            continue

        side, src = classify_trade_side(ltp[i], bid[i], ask[i], last_trade_ltp, last_side)

        if side == BUY:
            buy[i] = dv[i]
        elif side == SELL:
            sell[i] = dv[i]
        else:
            # Nothing to go on (first trade, no book, no history). Split so the
            # total always reconciles instead of vanishing.
            buy[i] = sell[i] = dv[i] / 2.0
            src = "split"

        sides[i] = side
        srcs[i] = src
        src_counts[src] = src_counts.get(src, 0) + 1
        if side:
            last_side = side
        if ltp[i] > 0:
            last_trade_ltp = ltp[i]

    # Session VWAP, rebuilt from scratch so the phantom cannot contaminate it.
    num = np.cumsum(ltp * dv)
    den = np.cumsum(dv)
    vwap = np.divide(num, den, out=ltp.copy(), where=den > 0)

    out = df.copy()
    out["vol_delta"] = dv
    out["vol_gap"] = gap
    out["secs_since_prev"] = secs
    out["buy_vol"] = buy
    out["sell_vol"] = sell
    out["side"] = sides
    out["side_src"] = srcs
    out["vwap"] = vwap

    # Did we join mid-session? If so the VWAP and totals cover only the part of
    # the day actually captured, which downstream analysis must know.
    t = out["timestamp"].dt.time
    in_sess = out[(t >= SESSION_START) & (t <= SESSION_END)]
    first = in_sess["timestamp"].iloc[0] if len(in_sess) else None
    partial = bool(first is not None and first.time() > SESSION_START.replace(minute=16))

    stats = {
        "rows": len(out),
        "traded": float(dv.sum()),
        "classified": float(buy.sum() + sell.sum()),
        "backward_steps": n_backward,
        "gap_packets": int(gap.sum()),
        "gap_volume": float(dv[gap].sum()),
        "partial_session": partial,
        "first_packet": str(first) if first is not None else "",
        **{f"src_{k}": v for k, v in src_counts.items()},
    }
    return out, stats


def _detect_phantom(df: pd.DataFrame) -> float:
    """Largest share of standing cumulative volume credited to one packet."""
    if not {"buy_vol", "sell_vol", "volume"}.issubset(df.columns):
        return 0.0
    cls = (pd.to_numeric(df["buy_vol"], errors="coerce").fillna(0)
           + pd.to_numeric(df["sell_vol"], errors="coerce").fillna(0)).to_numpy("float64")
    vol = pd.to_numeric(df["volume"], errors="coerce").fillna(0).to_numpy("float64")
    if not len(cls) or cls.max() <= 0:
        return 0.0
    i = int(np.argmax(cls))
    return float(cls[i] / vol[i]) if vol[i] > 0 else 0.0


def reprocess_symbol(symbol: str, src_root: str, dst_root: str, dry_run: bool) -> dict:
    stat = {"symbol": symbol, "days": 0, "rows": 0, "traded": 0.0, "classified": 0.0,
            "phantoms_before": 0, "phantoms_after": 0, "partial_days": 0,
            "backward_steps": 0, "gap_packets": 0, "gap_volume": 0.0,
            "src_quote": 0, "src_tick": 0, "src_carry": 0,
            "src_split": 0, "errors": 0}

    sym_dir = os.path.join(src_root, symbol)
    if not os.path.isdir(sym_dir):
        return stat

    for entry in sorted(os.listdir(sym_dir)):
        if not entry.endswith(".parquet"):
            continue
        day_path = os.path.join(sym_dir, entry)
        date_str = entry[: -len(".parquet")]
        try:
            df = pd.read_parquet(day_path)
            if df.empty or "volume" not in df.columns or "ltp" not in df.columns:
                continue
            df["timestamp"] = normalize_ist(df["timestamp"])
            df = df.dropna(subset=["timestamp"])
            if len(df) < 20:
                continue

            if _detect_phantom(df) >= PHANTOM_RATIO:
                stat["phantoms_before"] += 1

            out, s = reprocess_frame(df)

            if _detect_phantom(out) >= PHANTOM_RATIO:
                stat["phantoms_after"] += 1

            stat["days"] += 1
            stat["rows"] += s["rows"]
            stat["traded"] += s["traded"]
            stat["classified"] += s["classified"]
            stat["backward_steps"] += s["backward_steps"]
            stat["gap_packets"] += s["gap_packets"]
            stat["gap_volume"] += s["gap_volume"]
            stat["partial_days"] += int(s["partial_session"])
            for k in ("quote", "tick", "carry", "split"):
                stat[f"src_{k}"] += s.get(f"src_{k}", 0)

            if dry_run:
                continue

            out_dir = os.path.join(dst_root, symbol, f"{date_str}.parquet")
            os.makedirs(out_dir, exist_ok=True)
            final = os.path.join(out_dir, "part-000.parquet")
            tmp = final + ".tmp"
            out.to_parquet(tmp, compression="snappy", index=False)
            os.replace(tmp, final)

        except Exception:
            stat["errors"] += 1

    return stat


def _job(a):
    try:
        return reprocess_symbol(*a)
    except Exception as e:
        return {"symbol": a[0], "days": 0, "rows": 0, "traded": 0.0, "classified": 0.0,
                "phantoms_before": 0, "phantoms_after": 0, "partial_days": 0,
                "backward_steps": 0, "gap_packets": 0, "gap_volume": 0.0,
                "src_quote": 0, "src_tick": 0, "src_carry": 0,
                "src_split": 0, "errors": 1, "error": f"{type(e).__name__}: {e}"}


def _explicit(spec):
    """--symbols accepts 'A,B,C' or '@path/to/list.txt'."""
    if spec.startswith("@"):
        return [x.strip() for x in open(spec[1:]) if x.strip()]
    return [x.strip() for x in spec.split(",") if x.strip()]


def main():
    ap = argparse.ArgumentParser(description="Rebuild derived order-flow columns.")
    ap.add_argument("--symbols", help="comma-separated list, or @file with one per line")
    ap.add_argument("--symbol")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--src", default=SRC_ROOT)
    ap.add_argument("--dst", default=DST_ROOT)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not (args.symbol or args.all or args.symbols):
        ap.error("pick a scope: --symbol SYM, --symbols A,B,C, or --all")
    if not os.path.isdir(args.src):
        print(f"Source {args.src} not found — run src.harness.normalize first.")
        return 1

    symbols = (_explicit(args.symbols) if args.symbols
               else [args.symbol] if args.symbol
               else sorted(s for s in os.listdir(args.src)
                           if os.path.isdir(os.path.join(args.src, s))))

    print(f"{'DRY RUN — ' if args.dry_run else ''}Reprocessing {len(symbols)} symbols "
          f"from {args.src}...\n", flush=True)

    jobs = [(s, args.src, args.dst, args.dry_run) for s in symbols]
    res = []
    if args.workers > 1:
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(_job, j) for j in jobs]
            for i, f in enumerate(as_completed(futs), 1):
                res.append(f.result())
                if i % 20 == 0:
                    print(f"  ...{i}/{len(jobs)}", flush=True)
    else:
        for i, j in enumerate(jobs, 1):
            res.append(_job(j))
            if i % 20 == 0:
                print(f"  ...{i}/{len(jobs)}", flush=True)

    df = pd.DataFrame(res)
    traded = df["traded"].sum()
    classified = df["classified"].sum()
    tot_src = sum(int(df[f"src_{k}"].sum()) for k in ("quote", "tick", "carry", "split"))

    print("\n" + "=" * 72)
    print("REPROCESSING" + (" (DRY RUN)" if args.dry_run else ""))
    print("=" * 72)
    print(f"symbols / symbol-days : {len(df)} / {int(df['days'].sum()):,}")
    print(f"packets               : {int(df['rows'].sum()):,}")
    print(f"traded volume         : {traded:,.0f}")
    print(f"classified            : {classified:,.0f}  "
          f"({(1 - classified/traded)*100 if traded else 0:.2f}% unclassified)")
    print()
    print(f"seed phantoms before  : {int(df['phantoms_before'].sum()):,} symbol-days")
    print(f"seed phantoms after   : {int(df['phantoms_after'].sum()):,} symbol-days")
    print(f"partial-session days  : {int(df['partial_days'].sum()):,} (joined after 09:16)")
    print(f"backward volume steps : {int(df['backward_steps'].sum()):,} packets")
    gp = int(df['gap_packets'].sum()) if 'gap_packets' in df else 0
    gv = float(df['gap_volume'].sum()) if 'gap_volume' in df else 0.0
    print(f"gap-spanning packets  : {gp:,}  carrying {gv:,.0f} volume "
          f"({gv/traded*100 if traded else 0:.2f}% of traded) — flagged vol_gap")
    if tot_src:
        print("\nClassification source")
        for k in ("quote", "tick", "carry", "split"):
            n = int(df[f"src_{k}"].sum())
            print(f"  {k:<8} {n:>12,}  ({n/tot_src*100:5.1f}%)")
    if int(df["errors"].sum()):
        print(f"\nerrors: {int(df['errors'].sum())}")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
