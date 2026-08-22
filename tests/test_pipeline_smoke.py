"""
Offline smoke test for the market-depth ingest path.

Exercises the code changed during the data-integrity work without touching
Dhan, Redis or MySQL, so it is safe to run any time — including while the
live service is running.

    python3 -m tests.test_pipeline_smoke

Covers:
  * priming seed no longer leaks a whole session into one packet
  * Lee-Ready classification (quote / tick / carry), buy+sell reconciles
  * published timestamps are naive IST
  * storage partitions by packet date and survives mixed tz input
  * the standard-feed frame walker reads every packet in a frame
"""

import os
import queue
import struct
import sys
import tempfile
import threading
from collections import defaultdict, deque
from datetime import date, datetime

import pandas as pd

os.environ.setdefault("DATABASE_URL", "sqlite://")

from src.services.market_depth_service_v2 import MarketDepthService
from src.utils.storage_manager import StorageManager, normalize_ist
from src.utils.trade_classifier import classify_trade_side
from src.data.dhan_feed_v2 import DhanFeedClient

FAILURES = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def build_service():
    """A MarketDepthService with state only — no DB, Redis or feed."""
    svc = MarketDepthService.__new__(MarketDepthService)
    svc.redis_client = None
    svc.running = True
    svc.depth_source = "TEST"
    svc.current_state = defaultdict(dict)
    svc.price_history = defaultdict(lambda: deque(maxlen=500))
    svc.vol_history = defaultdict(lambda: deque(maxlen=100))
    svc.vwap_num = defaultdict(float)
    svc.vwap_den = defaultdict(float)
    svc.pcr_data = defaultdict(dict)
    svc.tick_buffer = defaultdict(list)
    svc.last_ttq = defaultdict(int)
    svc.buffer_lock = threading.Lock()
    svc.state_lock = threading.Lock()
    svc.tick_count = 0
    svc._drop_count = 0
    svc.last_reset_date = date.today()
    svc.redis_queue = queue.Queue(maxsize=10000)
    return svc


def tick(sym, ltp, volume, bid, ask):
    return {
        "symbol": sym, "ltp": ltp, "volume": volume,
        "bid": [{"price": bid, "qty": 100, "orders": 1}],
        "ask": [{"price": ask, "qty": 100, "orders": 1}],
        "total_buy_qty": 1000, "total_sell_qty": 900,
    }


def test_no_seed_phantom():
    print("\nseed phantom")
    svc = build_service()
    # Mimic priming: the old code put the last BAR's volume here (e.g. 830).
    # The fix seeds 0 so the first live tick only establishes a baseline.
    svc.current_state["ACME"] = {"ltp": 100.0, "volume": 0, "v_buy": 0.0, "v_sell": 0.0,
                                 "last_trade_ltp": 100.0, "last_side": None}
    svc._process_dhan_tick(tick("ACME", 100.0, 298_008, 99.9, 100.1))
    svc._process_dhan_tick(tick("ACME", 100.5, 298_540, 100.4, 100.6))

    pkts = svc.tick_buffer["ACME"]
    worst = max(p["buy_vol"] + p["sell_vol"] for p in pkts)
    check("first live tick does not emit a phantom",
          worst <= 532, f"largest classified={worst}, expected <= 532")
    check("second tick carries the genuine 532 delta",
          any(abs((p["buy_vol"] + p["sell_vol"]) - 532) < 1e-6 for p in pkts))


def test_classification_reconciles():
    print("\nclassification")
    svc = build_service()
    svc.current_state["ACME"] = {"ltp": 100.0, "volume": 0, "v_buy": 0.0, "v_sell": 0.0,
                                 "last_trade_ltp": 0.0, "last_side": None}
    seq = [
        (100.00, 1_000, 99.99, 100.01),
        (100.02, 1_100, 100.00, 100.04),   # above mid -> buy
        (100.01, 1_250, 100.00, 100.04),   # below mid -> sell
        (100.01, 1_400, 100.00, 100.04),   # flat, at/near mid -> carry
        (100.05, 1_600, 100.00, 100.02),   # outside spread -> tick rule
    ]
    for ltp, vol, b, a in seq:
        svc._process_dhan_tick(tick("ACME", ltp, vol, b, a))

    pkts = svc.tick_buffer["ACME"]
    classified = sum(p["buy_vol"] + p["sell_vol"] for p in pkts)
    traded = seq[-1][1] - seq[0][1]
    check("buy+sell reconciles with traded volume",
          abs(classified - traded) < 1e-6, f"classified={classified} traded={traded}")
    check("nothing is left unclassified", classified > 0)


def test_timestamps_naive_ist():
    print("\ntimestamps")
    svc = build_service()
    svc.current_state["ACME"] = {"ltp": 100.0, "volume": 0, "v_buy": 0.0, "v_sell": 0.0,
                                 "last_trade_ltp": 0.0, "last_side": None}
    svc._process_dhan_tick(tick("ACME", 100.0, 500, 99.9, 100.1))
    ts = svc.tick_buffer["ACME"][0]["timestamp"]
    check("published timestamp has no tz offset", "+" not in ts and "Z" not in ts, ts)
    check("published timestamp parses", pd.notna(pd.to_datetime(ts)))


def test_storage_roundtrip():
    print("\nstorage")
    d = tempfile.mkdtemp()
    sm = StorageManager(base_path=d)
    sm.save_ticks_parquet("ACME", [
        {"symbol": "ACME", "timestamp": "2026-03-20T15:29:59", "ltp": 100.0, "volume": 500},
        {"symbol": "ACME", "timestamp": "2026-03-20T15:30:00+05:30", "ltp": 100.5, "volume": 600},
        {"symbol": "ACME", "timestamp": "2026-03-21T09:15:00", "ltp": 101.0, "volume": 10},
    ])
    days = sorted(os.listdir(os.path.join(d, "ACME")))
    total = sum(len(pd.read_parquet(os.path.join(d, "ACME", x))) for x in days)
    check("rows partitioned by packet date, not wall clock", days ==
          ["2026-03-20.parquet", "2026-03-21.parquet"], str(days))
    check("no rows lost to mixed tz input", total == 3, f"kept {total}/3")
    check("no .tmp files left behind",
          not any(f.endswith(".tmp") for _, _, fs in os.walk(d) for f in fs))


def test_frame_walker():
    print("\nframe walker")
    def pkt(code, sec_id, ltp):
        body = struct.pack("<BHBIf", code, 0, 0, sec_id, ltp) + b"\x00" * 8
        return struct.pack("<B", code) + struct.pack("<H", len(body)) + body[3:]

    c = DhanFeedClient("x", "y")
    c._id_to_symbol = {101: "AAA", 102: "BBB", 103: "CCC"}
    c._id_meta = {101: {}, 102: {}, 103: {}}
    seen = []
    c.on_tick_callback = lambda t: seen.append(t["symbol"])

    c._parse_standard_binary(pkt(2, 101, 100.5))
    check("single-packet frame still yields 1 tick", seen == ["AAA"], str(seen))

    seen.clear()
    c._parse_standard_binary(pkt(2, 101, 100.5) + pkt(2, 102, 200.5) + pkt(2, 103, 300.5))
    check("3-packet frame yields 3 ticks (was 1)", seen == ["AAA", "BBB", "CCC"], str(seen))

    seen.clear()
    c._parse_standard_binary(b"\xff\xff\xff" + b"\x00" * 20)
    check("unusable length header does not crash", True)


def test_packet_contract():
    """The published packet carries the fields consumers now depend on."""
    print("\npacket contract")
    svc = build_service()
    svc.current_state["ACME"] = {"ltp": 100.0, "volume": 0, "v_buy": 0.0, "v_sell": 0.0,
                                 "last_trade_ltp": 0.0, "last_side": None,
                                 "v_delta": 0.0, "last_src": None, "last_pub_ts": 0.0}
    for ltp, vol, b, a in [(100.00, 1_000, 99.99, 100.01),
                           (100.02, 1_100, 100.00, 100.04),
                           (100.01, 1_250, 100.00, 100.04)]:
        svc._process_dhan_tick(tick("ACME", ltp, vol, b, a))

    pkts = svc.tick_buffer["ACME"]
    check("vol_delta present in every packet", all("vol_delta" in p for p in pkts))
    check("side_src present", all("side_src" in p for p in pkts))
    check("secs_since_prev present", all("secs_since_prev" in p for p in pkts))
    check("volume is still cumulative (non-decreasing)",
          [p["volume"] for p in pkts] == sorted(p["volume"] for p in pkts))
    check("vol_delta == buy_vol + sell_vol in every packet",
          all(abs(p["vol_delta"] - (p["buy_vol"] + p["sell_vol"])) < 1e-6 for p in pkts))
    check("vol_delta sums to traded volume",
          abs(sum(p["vol_delta"] for p in pkts) - 250) < 1e-6,
          f'got {sum(p["vol_delta"] for p in pkts)}, expected 250')


def test_packet_reader():
    """Consumers read the contract one way, with a legacy fallback."""
    print("\npacket reader")
    from src.utils.depth_packet import PacketReader

    r = PacketReader("test")
    v, b, s = r.deltas({"symbol": "ACME", "vol_delta": 250.0,
                        "buy_vol": 150.0, "sell_vol": 100.0, "volume": 9999})
    check("uses vol_delta, not cumulative volume", (v, b, s) == (250.0, 150.0, 100.0), str((v, b, s)))

    # Legacy service with no vol_delta: derive from the cumulative counter.
    r2 = PacketReader("legacy")
    r2.deltas({"symbol": "ACME", "volume": 1000, "buy_vol": 0, "sell_vol": 0})
    v2, _, _ = r2.deltas({"symbol": "ACME", "volume": 1250, "buy_vol": 0, "sell_vol": 0})
    check("legacy fallback derives 250 from cumulative", v2 == 250.0, str(v2))

    v3, _, _ = r2.deltas({"symbol": "ACME", "volume": 900, "buy_vol": 0, "sell_vol": 0})
    check("legacy fallback ignores backward steps", v3 == 0.0, str(v3))

    check("cumulative_volume accessor", PacketReader.cumulative_volume({"volume": 5}) == 5.0)
    check("stale_seconds accessor", PacketReader.stale_seconds({"secs_since_prev": 3.5}) == 3.5)
    check("missing fields degrade to 0.0", PacketReader.stale_seconds({}) == 0.0)


def test_classifier_units():
    print("\nclassifier")
    cases = [
        ((101.5, 100.0, 102.0, 0, None), ("B", "quote")),
        ((100.5, 100.0, 102.0, 0, None), ("S", "quote")),
        ((101.0, 100.0, 102.0, 100.0, None), ("B", "tick")),
        ((105.0, 100.0, 102.0, 104.0, None), ("B", "tick")),
        ((100.0, 0, 0, 100.0, "S"), ("S", "carry")),
        ((100.0, 0, 0, 0, None), (None, None)),
    ]
    for args, expected in cases:
        check(f"classify{args}", classify_trade_side(*args) == expected,
              str(classify_trade_side(*args)))


def main():
    print("=" * 66)
    print("PIPELINE SMOKE TEST")
    print("=" * 66)
    for fn in (test_no_seed_phantom, test_classification_reconciles,
               test_timestamps_naive_ist, test_storage_roundtrip,
               test_frame_walker, test_classifier_units,
               test_packet_contract, test_packet_reader):
        try:
            fn()
        except Exception as e:
            print(f"  ERROR in {fn.__name__}: {type(e).__name__}: {e}")
            FAILURES.append(fn.__name__)

    print("\n" + "=" * 66)
    if FAILURES:
        print(f"FAILED ({len(FAILURES)}): " + ", ".join(FAILURES))
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
