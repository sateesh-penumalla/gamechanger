"""
Canonical reader for market_depth:LIVE packets.

The packet mixes cumulative and per-tick fields, and every consumer previously
guessed differently — which is how four strategy files ended up with three
mutually incompatible readings of the same three fields:

    volume      CUMULATIVE session volume. Not a delta. Reading it as one
                inflates 5-minute sums by the packet count.
    vol_delta   traded volume since the previous packet. This is the delta.
    buy_vol     buyer-initiated share of vol_delta. Already a delta; do NOT
    sell_vol    difference it against a previous value.

    vol_delta == buy_vol + sell_vol always holds.

Historically the bugs were:
    orc_sniper.py      read 'buy_volume'/'sell_volume' — keys that do not
                       exist, so aggression was pinned at 0.0 all session
    sniper_orc.py      read buy_vol/sell_vol correctly, then differenced them
                       against a previous value, yielding ~0
    orc_sniper_v2.py   read cumulative `volume` as a per-tick delta
    sniper_orc_v2.py   same

Use PacketReader so there is one implementation to be wrong in.
"""

from loguru import logger


class PacketReader:
    """Extracts per-tick deltas from depth packets.

    Falls back to differencing the cumulative counter when `vol_delta` is
    absent, so a strategy keeps working against a market-depth service that
    predates the field. The fallback warns once per process.
    """

    def __init__(self, name: str = "consumer"):
        self.name = name
        self._prev_cumulative = {}
        self._warned_legacy = False

    def deltas(self, packet: dict) -> (float, float, float):
        """Return (vol_delta, buy_delta, sell_delta) for one packet."""
        symbol = packet.get('symbol')

        if 'vol_delta' in packet and packet['vol_delta'] is not None:
            vol_delta = _f(packet['vol_delta'])
        else:
            # Legacy service: derive the delta ourselves from the cumulative
            # counter. Backward steps are out-of-order packets, not negative
            # trades, so they contribute nothing.
            cumulative = _f(packet.get('volume'))
            prev = self._prev_cumulative.get(symbol)
            vol_delta = max(0.0, cumulative - prev) if prev is not None else 0.0
            self._prev_cumulative[symbol] = cumulative
            if not self._warned_legacy:
                self._warned_legacy = True
                logger.warning(
                    f"[{self.name}] packets carry no 'vol_delta' — falling back to "
                    f"differencing the cumulative counter. Update MarketDepthService."
                )

        return vol_delta, _f(packet.get('buy_vol')), _f(packet.get('sell_vol'))

    @staticmethod
    def cumulative_volume(packet: dict) -> float:
        """Session-to-date traded volume. Explicit, so nobody reads it as a delta."""
        return _f(packet.get('volume'))

    @staticmethod
    def stale_seconds(packet: dict) -> float:
        """Seconds since this symbol's previous packet.

        A large value means the accompanying vol_delta aggregates trading the
        feed did not deliver, so surge/pulse logic computed across it is
        measuring a delivery gap rather than genuine activity.
        """
        return _f(packet.get('secs_since_prev'))

    @staticmethod
    def side_confidence(packet: dict):
        """'quote' | 'tick' | 'carry' | None — which rule assigned the side."""
        return packet.get('side_src')


def _f(v) -> float:
    try:
        return float(v or 0.0)
    except (TypeError, ValueError):
        return 0.0
