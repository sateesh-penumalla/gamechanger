"""
Trade side classification (Lee & Ready, 1991).

Dhan does not report which side crossed the spread, so buyer- vs
seller-initiated volume has to be inferred. This module is the single
implementation, shared by the live path (MarketDepthService._process_dhan_tick)
and offline reprocessing (src.harness.reprocess), so the two cannot drift.

Being a pure function of five scalars, it is also directly testable.
"""

# Side constants
BUY = "B"
SELL = "S"

# Source of the decision, useful for filtering research to high-confidence rows
SRC_QUOTE = "quote"
SRC_TICK = "tick"
SRC_CARRY = "carry"


def classify_trade_side(ltp, bid, ask, last_trade_ltp, last_side):
    """Return (side, source) for a trade at `ltp`.

    side   : BUY, SELL or None
    source : SRC_QUOTE, SRC_TICK, SRC_CARRY or None

    Parameters
    ----------
    ltp             price the trade printed at
    bid, ask        best bid/ask *prevailing before the trade*
    last_trade_ltp  price of the previous trade (not the previous quote)
    last_side       last successfully classified side, for the zero-tick carry

    1. Quote rule — a trade above the midpoint is buyer-initiated, below it
       seller-initiated. More accurate than the tick rule (~93% vs ~85% in the
       literature) and the part that handles flat prices, since a trade at an
       unchanged price still sits on one side of the mid.

       Applied only when the trade printed INSIDE the prevailing spread. The
       book is sampled at roughly 1 Hz and, measured against the archive,
       35-66% of trades print outside the last-seen quote — the book moved
       between snapshots. A midpoint that stale is not evidence, so those
       defer to the tick rule instead of yielding a confident wrong answer.

    2. Tick rule — compare against the last traded price.

    3. Zero tick — an unchanged price inherits the previous direction.
    """
    if ltp is None or ltp <= 0:
        return None, None

    # 1. Quote rule, only on an unstale book
    if bid and ask and bid > 0 and ask > bid and bid <= ltp <= ask:
        mid = (bid + ask) / 2.0
        if ltp > mid:
            return BUY, SRC_QUOTE
        if ltp < mid:
            return SELL, SRC_QUOTE
        # exactly at the midpoint — fall through

    # 2. Tick rule against the last traded price
    if last_trade_ltp and last_trade_ltp > 0:
        if ltp > last_trade_ltp:
            return BUY, SRC_TICK
        if ltp < last_trade_ltp:
            return SELL, SRC_TICK

    # 3. Zero tick — carry the previous direction
    if last_side in (BUY, SELL):
        return last_side, SRC_CARRY

    return None, None
