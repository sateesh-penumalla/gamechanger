"""
Walk-forward evaluation against data/ticks_clean.

Answers two different questions, because the dataset can only support one of
them honestly:

A. FEATURE TEST — does any order-flow feature predict forward return?
   Samples each session on a fixed grid, computes features from a trailing
   window and the realised return over the next N minutes, then measures the
   rank correlation (information coefficient) per day. Dates are split
   chronologically: the model is never scored on a day it was ranked on.
   With ~780 symbol-days this yields hundreds of thousands of observations,
   which is enough to distinguish signal from noise.

B. STRATEGY TEST — what does the live filter stack actually produce?
   Replays the orc_sniper gate (magic volume threshold, efficiency, aggression,
   pulses, bid%) and counts signals with realistic costs. With at most 14
   sessions per symbol and a one-trade-per-symbol-per-day cap, this cannot
   produce a statistically meaningful win rate — the trade COUNT is the finding.

    python3 -m src.harness.walkforward --features
    python3 -m src.harness.walkforward --strategy
    python3 -m src.harness.walkforward --features --strategy --workers 6
"""

import argparse
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

SRC = os.path.join("data", "ticks_clean")

SESSION_START = pd.Timestamp("09:15").time()
SESSION_END = pd.Timestamp("15:30").time()

SAMPLE_SECS = 60      # grid spacing
WINDOW_SECS = 300     # trailing feature window
HORIZON_SECS = 300    # forward return horizon

# Round-trip cost on a ~Rs5000 intraday ticket: Dhan brokerage both legs, STT,
# exchange + SEBI charges, stamp duty, GST. ~0.106% of turnover.
COST_PCT = 0.106
SLIPPAGE_PCT = 0.05

FEATURES = ["bid_pct_c", "imbalance", "ofi", "aggression_l", "vwap_dist",
            "efficiency", "rvol", "ret_5m"]


def _session(df):
    t = df["timestamp"].dt.time
    return df[(t >= SESSION_START) & (t <= SESSION_END)].reset_index(drop=True)


def build_samples(df, symbol, date):
    """Feature rows on a fixed time grid for one symbol-day."""
    d = _session(df)
    if len(d) < 200:
        return None

    ts = d["timestamp"].values.astype("datetime64[s]").astype(np.int64)
    ltp = d["ltp"].to_numpy("float64")
    dv = d["vol_delta"].to_numpy("float64")
    bv = d["buy_vol"].to_numpy("float64")
    sv = d["sell_vol"].to_numpy("float64")
    vwap = d["vwap"].to_numpy("float64")
    bidp = d["bid_pct"].to_numpy("float64") if "bid_pct" in d else np.full(len(d), 50.0)
    imb = d["imbalance"].to_numpy("float64") if "imbalance" in d else np.zeros(len(d))
    gap = d["vol_gap"].to_numpy(bool) if "vol_gap" in d else np.zeros(len(d), bool)

    cdv = np.concatenate([[0.0], np.cumsum(dv)])
    cbv = np.concatenate([[0.0], np.cumsum(bv)])
    csv_ = np.concatenate([[0.0], np.cumsum(sv)])
    cgap = np.concatenate([[0.0], np.cumsum(np.where(gap, dv, 0.0))])

    t0, t1 = ts[0], ts[-1]
    grid = np.arange(t0 + WINDOW_SECS, t1 - HORIZON_SECS, SAMPLE_SECS)
    if grid.size < 5:
        return None

    i_now = np.searchsorted(ts, grid, side="right") - 1
    i_win = np.searchsorted(ts, grid - WINDOW_SECS, side="left")
    i_fwd = np.searchsorted(ts, grid + HORIZON_SECS, side="right") - 1
    ok = (i_now > i_win) & (i_fwd > i_now) & (i_now >= 0)
    if not ok.any():
        return None
    i_now, i_win, i_fwd, grid = i_now[ok], i_win[ok], i_fwd[ok], grid[ok]

    vol_w = cdv[i_now + 1] - cdv[i_win]
    buy_w = cbv[i_now + 1] - cbv[i_win]
    sell_w = csv_[i_now + 1] - csv_[i_win]
    gap_w = cgap[i_now + 1] - cgap[i_win]

    px = ltp[i_now]
    px_prev = ltp[i_win]
    ret_5m = np.where(px_prev > 0, px / np.maximum(px_prev, 1e-9) - 1.0, 0.0) * 100

    with np.errstate(divide="ignore", invalid="ignore"):
        aggression = buy_w / (sell_w + 1.0)
        ofi = (buy_w - sell_w) / (buy_w + sell_w + 1.0)
        efficiency = ret_5m / ((vol_w / 1e6) + 0.001)
        vwap_dist = np.where(vwap[i_now] > 0, px / vwap[i_now] - 1.0, 0.0) * 100

    med = np.median(vol_w[vol_w > 0]) if (vol_w > 0).any() else 0.0
    rvol = vol_w / med if med > 0 else np.zeros_like(vol_w)

    out = pd.DataFrame({
        "symbol": symbol, "date": date,
        "ts": pd.to_datetime(grid, unit="s"),
        "bid_pct_c": bidp[i_now] - 50.0,      # centred so sign is meaningful
        "imbalance": imb[i_now],
        "ofi": ofi,
        "aggression_l": np.log1p(np.clip(aggression, 0, 100)),
        "vwap_dist": vwap_dist,
        "efficiency": np.clip(efficiency, -50, 50),
        "rvol": np.clip(rvol, 0, 20),
        "ret_5m": ret_5m,
        # Forward return — the target. Costs are applied at evaluation.
        "fwd_ret": (ltp[i_fwd] / np.maximum(px, 1e-9) - 1.0) * 100,
        # Share of the window's volume that arrived through a delivery gap.
        "gap_frac": np.where(vol_w > 0, gap_w / np.maximum(vol_w, 1e-9), 1.0),
        "vol_w": vol_w,
    })
    return out.replace([np.inf, -np.inf], np.nan).dropna()


def _load(symbol, date_file):
    p = os.path.join(SRC, symbol, date_file)
    df = pd.read_parquet(p)
    return df


def _sample_job(args):
    symbol, files = args
    frames = []
    for f in files:
        try:
            s = build_samples(_load(symbol, f), symbol, f[:-len(".parquet")])
            if s is not None:
                frames.append(s)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else None


def collect(workers=1, symbols=None):
    if not os.path.isdir(SRC):
        print(f"{SRC} not found — run src.harness.reprocess first.")
        return None
    syms = symbols or sorted(os.listdir(SRC))
    jobs = []
    for s in syms:
        d = os.path.join(SRC, s)
        if not os.path.isdir(d):
            continue
        files = sorted(f for f in os.listdir(d) if f.endswith(".parquet"))
        if files:
            jobs.append((s, files))

    frames = []
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_sample_job, j) for j in jobs]
            for i, f in enumerate(as_completed(futs), 1):
                r = f.result()
                if r is not None:
                    frames.append(r)
                if i % 20 == 0:
                    print(f"  ...{i}/{len(jobs)} symbols", flush=True)
    else:
        for i, j in enumerate(jobs, 1):
            r = _sample_job(j)
            if r is not None:
                frames.append(r)
            if i % 20 == 0:
                print(f"  ...{i}/{len(jobs)} symbols", flush=True)

    return pd.concat(frames, ignore_index=True) if frames else None


def ic_report(df, label, clean_only=False):
    """Rank IC per day for each feature, with a t-stat across days."""
    if clean_only:
        df = df[df.gap_frac < 0.10]
    rows = []
    for feat in FEATURES:
        per_day = []
        for (_, _), g in df.groupby(["date", "symbol"]):
            if len(g) < 20:
                continue
            c = g[feat].corr(g["fwd_ret"], method="spearman")
            if pd.notna(c):
                per_day.append(c)
        if len(per_day) < 10:
            continue
        a = np.array(per_day)
        t = a.mean() / (a.std(ddof=1) / np.sqrt(len(a))) if a.std(ddof=1) > 0 else 0.0
        rows.append({"feature": feat, "mean_ic": a.mean(), "t_stat": t,
                     "pct_positive": (a > 0).mean() * 100, "n_days": len(a)})
    r = pd.DataFrame(rows).sort_values("t_stat", key=abs, ascending=False)
    print(f"\n{label}  (n={len(df):,} samples)")
    if r.empty:
        print("  not enough data")
        return r
    print(f"  {'feature':<14}{'mean IC':>10}{'t-stat':>9}{'% days +':>10}{'days':>7}")
    for _, x in r.iterrows():
        print(f"  {x.feature:<14}{x.mean_ic:>10.4f}{x.t_stat:>9.2f}"
              f"{x.pct_positive:>9.1f}%{int(x.n_days):>7}")
    return r


def run_features(workers, symbols=None):
    print("Collecting feature samples...", flush=True)
    df = collect(workers, symbols)
    if df is None or df.empty:
        print("No samples built.")
        return

    dates = sorted(df.date.unique())
    split = dates[int(len(dates) * 0.6)]
    train = df[df.date < split]
    test = df[df.date >= split]

    print("\n" + "=" * 70)
    print("A. FEATURE TEST — does order flow predict the next 5 minutes?")
    print("=" * 70)
    print(f"symbol-days : {df.groupby(['symbol','date']).ngroups:,}")
    print(f"samples     : {len(df):,}")
    print(f"dates       : {dates[0]} .. {dates[-1]}  ({len(dates)} sessions)")
    print(f"split       : train < {split} <= test   ({len(train):,} / {len(test):,})")
    print(f"\nfwd 5m return: mean {df.fwd_ret.mean():+.4f}%  sd {df.fwd_ret.std():.4f}%")
    print(f"round-trip cost assumption: {COST_PCT + SLIPPAGE_PCT:.3f}%")

    ic_report(train, "IN-SAMPLE (train dates)")
    t = ic_report(test, "OUT-OF-SAMPLE (held-out dates)")
    ic_report(test, "OUT-OF-SAMPLE, gap-free windows only", clean_only=True)

    if not t.empty:
        best = t.iloc[0]
        print(f"\n  Strongest out-of-sample feature: {best.feature} "
              f"(IC {best.mean_ic:+.4f}, t={best.t_stat:.2f})")
        edge = abs(best.mean_ic) * df.fwd_ret.std()
        print(f"  Implied per-trade edge at that IC: ~{edge:.4f}% "
              f"vs {COST_PCT + SLIPPAGE_PCT:.3f}% round-trip cost")
    return df


def run_strategy(workers, symbols=None):
    """Replay the live orc_sniper gate over the clean archive."""
    magic = {}
    try:
        magic = json.load(open("magic_volume.json"))
    except Exception:
        pass

    print("\n" + "=" * 70)
    print("B. STRATEGY TEST — what the live filter stack produces")
    print("=" * 70)
    if not magic:
        print("magic_volume.json not loaded; cannot replay the volume gate.")
        return

    syms = symbols or sorted(os.listdir(SRC))
    syms = [s for s in syms if s in magic]
    print(f"symbols in both ticks_clean and magic_volume.json: {len(syms)}")
    if not syms:
        print("  none — the magic universe and the reprocessed universe do not overlap.")
        return

    signals, stage = [], {"days": 0, "vol_gate": 0, "eff": 0, "agg": 0, "pulse": 0, "bid": 0}
    for s in syms:
        d = os.path.join(SRC, s)
        thr = magic[s]
        v1m = thr.get("high_conviction_magic", 1e12)
        v3m = thr.get("high_conviction_magic_3m", 1e12)
        for f in sorted(os.listdir(d)):
            if not f.endswith(".parquet"):
                continue
            try:
                df = _session(pd.read_parquet(os.path.join(d, f)))
            except Exception:
                continue
            if len(df) < 200:
                continue
            stage["days"] += 1
            bars = df.set_index("timestamp").resample("1min").agg(
                open=("ltp", "first"), close=("ltp", "last"),
                vol=("vol_delta", "sum"), buy=("buy_vol", "sum"),
                sell=("sell_vol", "sum"), bidp=("bid_pct", "mean")).dropna()
            if len(bars) < 6:
                continue
            v = bars["vol"].to_numpy()
            roll3 = pd.Series(v).rolling(3).sum().to_numpy()
            fired = False
            for i in range(5, len(bars)):
                if fired:
                    break
                if not (v[i] >= v1m or (roll3[i] if not np.isnan(roll3[i]) else 0) >= v3m):
                    continue
                stage["vol_gate"] += 1
                vol5 = v[max(0, i - 4):i + 1].sum()
                buy5 = bars["buy"].to_numpy()[max(0, i - 4):i + 1].sum()
                sell5 = bars["sell"].to_numpy()[max(0, i - 4):i + 1].sum()
                p0 = bars["open"].to_numpy()[max(0, i - 4)]
                px = bars["close"].to_numpy()[i]
                chg = (px / p0 - 1) * 100 if p0 > 0 else 0.0
                eff = chg / ((vol5 / 1e6) + 0.001)
                agg = buy5 / (sell5 + 1)
                if not ((eff > 0.6) or (eff < -5.0)):
                    continue
                stage["eff"] += 1
                if not ((4.0 <= agg <= 35.0) or (agg <= 0.10)):
                    continue
                stage["agg"] += 1
                pulses = int((v[max(0, i - 14):i + 1] >= v1m * 0.3).sum())
                if pulses < 3:
                    continue
                stage["pulse"] += 1
                side = "LONG" if eff > 0 else "SHORT"
                bp = bars["bidp"].to_numpy()[i]
                if side == "LONG" and not (53.0 <= bp <= 65.0):
                    continue
                if side == "SHORT" and not (35.0 <= bp <= 45.0):
                    continue
                stage["bid"] += 1
                # Outcome: 1% TP / 2% SL on the remaining session
                fut = bars["close"].to_numpy()[i + 1:]
                if fut.size == 0:
                    continue
                tp = px * (1.01 if side == "LONG" else 0.99)
                sl = px * (0.98 if side == "LONG" else 1.02)
                res = "EOD"
                for q in fut:
                    if (side == "LONG" and q >= tp) or (side == "SHORT" and q <= tp):
                        res = "TP"
                        break
                    if (side == "LONG" and q <= sl) or (side == "SHORT" and q >= sl):
                        res = "SL"
                        break
                gross = 1.0 if res == "TP" else (-2.0 if res == "SL" else
                                                 ((fut[-1] / px - 1) * 100 * (1 if side == "LONG" else -1)))
                signals.append({"symbol": s, "date": f[:-8], "side": side,
                                "result": res, "gross_pct": gross,
                                "net_pct": gross - COST_PCT - SLIPPAGE_PCT})
                fired = True

    print(f"\nFilter funnel over {stage['days']} symbol-days:")
    for k, label in [("vol_gate", "magic volume gate"), ("eff", "+ efficiency"),
                     ("agg", "+ aggression"), ("pulse", "+ pulses>=3"), ("bid", "+ bid% band")]:
        print(f"  {label:<22} {stage[k]:>7,}")

    if not signals:
        print("\n  NO SIGNALS produced. Nothing to evaluate.")
        return
    sg = pd.DataFrame(signals)
    wins = (sg.net_pct > 0).sum()
    print(f"\ntrades: {len(sg)}  |  wins {wins} ({wins/len(sg)*100:.1f}%)  "
          f"|  mean net {sg.net_pct.mean():+.3f}%  |  total {sg.net_pct.sum():+.2f}%")
    print(sg.result.value_counts().to_string())
    print(f"\n  Break-even win rate at 1%TP/2%SL with costs: "
          f"{(2 + COST_PCT + SLIPPAGE_PCT) / (3) * 100:.1f}%")
    if len(sg) < 30:
        print(f"  NOTE: {len(sg)} trades cannot support a win-rate claim. "
              f"A 95% CI on this sample spans most of the plausible range.")
    return sg


def main():
    global HORIZON_SECS
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", action="store_true")
    ap.add_argument("--strategy", action="store_true")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--symbols", help="comma-separated subset")
    ap.add_argument("--horizon", type=int, default=HORIZON_SECS,
                    help="forward-return horizon in seconds")
    a = ap.parse_args()
    HORIZON_SECS = a.horizon
    if not (a.features or a.strategy):
        ap.error("pick --features and/or --strategy")
    syms = a.symbols.split(",") if a.symbols else None
    if a.features:
        run_features(a.workers, syms)
    if a.strategy:
        run_strategy(a.workers, syms)
    return 0


if __name__ == "__main__":
    sys.exit(main())
