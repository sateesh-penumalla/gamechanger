Oracle sniper command: 

export PYTHONPATH=$PYTHONPATH:. && python3 src/scripts/sync_market_data.py --no-sentiment


TrueData Downloads

To download today's data for all existing symbols:
python3 scripts/download_today_batch.py


To download data for the last 3 days for all symbols:
python3 scripts/download_today_batch.py --days 3


To download just today's data for a single specific symbol:
python3 scripts/tick_downloader.py --symbol RELIANCE --days 0


python3 scripts/download_today_batch.py --exchange BSE --days 2


Dhan daily ohlc :

python3 scripts/download_historical_ohlc.py --days 1


DELETE FROM intraday_ticks WHERE timestamp < DATE_SUB(NOW(), INTERVAL 2 DAY);




--------------------------------------------------------------------




To implement your strategies, you can subscribe to the Redis channel market_depth or poll the keys depth:<SYMBOL>.

Every packet is a JSON string with the following structure:

1. Data Fields
Field	Type	Description
symbol
string	The ticker symbol (e.g., "SBIN").
ltp	float	Last Traded Price.
imbalance	float	Order book imbalance (Level 1). Range: -1.0 (Sell heavy) to 1.0 (Buy heavy).
vwap	float	Real-time calculated Intraday VWAP.
vqs_score	float	Momentum score based on tick-by-tick direction.
total_bid_qty	
int
Combined quantity of all 20 levels of the bid-side book.
total_ask_qty	
int
Combined quantity of all 20 levels of the ask-side book.
bid_pct / ask_pct	float	Percentage of the total book volume on each side.
buy_vol / sell_vol	
int
Aggregated aggressive buyer/seller volume since start of day.
bids / asks	
list
Top 5 price/quantity pairs: [[price, qty], ...]
bids_20 / asks_20	
list
Full 20 levels (Only present for the primary 50 focus symbols).
2. Example Structure
json
{
  "symbol": "OLAELEC",
  "ltp": 28.21,
  "imbalance": 0.05,
  "vwap": 28.18,
  "vqs_score": 0.1245,
  "total_bid_qty": 3124500,
  "total_ask_qty": 2845000,
  "bid_pct": 52.34,
  "ask_pct": 47.66,
  "buy_vol": 150000,
  "sell_vol": 120000,
  "timestamp": "2026-02-18T20:10:21.952",
  "source": "DHAN",
  "exchange_bridge": "Dhan20",
  "bids": [[28.21, 4500], [28.20, 10000], [28.19, 5000], [28.18, 1200], [28.17, 3000]],
  "asks": [[28.22, 4200], [28.23, 8000], [28.24, 15000], [28.25, 2000], [28.26, 5000]],
  "bids_20": [ ... 20 levels ... ],
  "asks_20": [ ... 20 levels ... ]
}
3. Usage Tips
Momentum Strategy: Use the vqs_score. If > 0.5, the price is aggressively ticking up.
Liquidity Strategy: Compare total_bid_qty vs total_ask_qty for high-resolution sentiment.
Scale: You can monitor up to 500 of these packets per second across your local Redis instance.



redis replay

--------------------

Start Shadow Redis: redis-server --port 6380
Run Replay: python3 -m src.scripts.parquet_replay --date 2026-02-06 --port 6380 --speed 5.0
Run Signal Generator: REDIS_PORT=6380 python3 -m src.services.signal_generator





redis channels:


Institutional Activity (Iceberg Alerts)
Channel: icebergs
Frequency: On detection (Asynchronous)
Purpose: Immediate notification when large hidden orders are absorbing retail liquidity.
Field	Type	Description

symbol
String	Target symbol.
ltp	Float	Price where absorption occurred.
actiontobetaken	String	BUY (Hidden buyer) or SELL (Hidden seller).

time
String	ISO Timestamp of the peak volume tick.
Example Message:

json
{
  "symbol": "RELIANCE",
  "ltp": 2942.15,
  "actiontobetaken": "BUY",
  "time": "2026-02-22T11:45:02"
}




 Main Depth & Orderflow Hub
Channel: market_depth:LIVE
Frequency: Every Price/Depth tick (High Frequency)
Purpose: Real-time signal generation, PCR sentiment, and Orderflow metrics.
Field	Type	Description

symbol
String	The base NSE symbol (e.g., "RELIANCE").
ltp	Float	Last Traded Price from NSE.
imbalance	Float	Orderbook bias (-1.0 to 1.0). >0 means more Bids.
vqs_score	Float	Price momentum (-1.0 to 1.0). Directional velocity.
oi	Float	Real-time Open Interest (mapped from active Future).
pcr_oi	Float	Put-Call Ratio (based on full Option Chain OI).
max_pain	Float	Price strike where option writers lose the least.
bids / asks	List	Top 5 price/qty pairs.
bids_20 / asks_20	List	Full 20-level depth (for priority focus symbols).
Example Message:

json
{
  "symbol": "SBIN",
  "ltp": 785.45,
  "imbalance": 0.35,
  "vqs_score": 0.12,
  "vwap": 784.10,
  "oi": 5420000.0,
  "pcr_oi": 1.15,
  "max_pain": 780.0,
  "bid_pct": 62.5,
  "ask_pct": 37.5,
  "buy_vol": 1500,
  "sell_vol": 200,
  "timestamp": "2026-02-22T10:15:30.123",
  "bids": [[785.40, 500], [785.35, 1200]],
  "asks": [[785.50, 400], [785.55, 900]]
}



1. Restart the OrderFlow Orchestrator
Since the Momentum Squeeze and VWAP Deviation logic is now built directly into the Orchestrator, you just need to restart it.

If you are running it natively on your Mac (recommended for lowest latency), run this in a terminal:

bash
export PYTHONPATH=$PYTHONPATH:.
python3 src/services/orderflow_orchestrator.py
2. Start the Background Market Scanner
The Volume Surge Scanner is a new standalone service. You should start it in a separate terminal window so it can continuously monitor the database for surges across all stocks:

bash
export PYTHONPATH=$PYTHONPATH:.
python3 src/services/vol_surge_scanner.py





# 1. Kill the old orchestrator
pkill -f "src.services.orderflow_orchestrator"
# 2. Start in "Momentum Only" mode
export ENABLED_SIGNALS=MOMENTUM_SQUEEZE
export PYTHONPATH=$PYTHONPATH:.
python3 -m src.services.orderflow_orchestrator > logs_momentum_only.txt 2>&1 &



The MOMENTUM_SQUEEZE strategy is a high-conviction momentum breakout system that operates in five distinct stages. It is designed to capture rapid price expansions that are backed by "smart money" (institutional) volume surges.

Here is the step-by-step breakdown of how it works:

1. The Independent Metric Engine
Instead of relying on delayed exchange data, the system calculates its own real-time metrics for every stock tick it receives from the Dhan API:

Local VWAP: It calculates the Volume Weighted Average Price from the very first tick of the day.
Volume Surge: It compares the volume of the latest tick to the average tick volume over the last 100 ticks.
VQS (Velocity Quality Score): This measures the "cleanliness" of the move. If every tick is higher than the previous one, the VQS is +1.0. If they are alternating, it drops toward 0.
2. The Squeeze Triggers (Thresholds)
The system continuously monitors all stocks in your focus list. A MOMENTUM_SQUEEZE is only flagged when the following "Gold Guard" thresholds are met simultaneously:

Volume Surge must be ≥ 12x the average volume. This ensures we are only entering when there is an actual explosion of activity.
VQS Score must be ≥ 0.70 (for Long) or ≤ -0.70 (for Short). This filters out "choppy" price action and only targets "one-way" directional moves.
3. Directional Alignment (The Logic)
Once the surge is detected, the system determines the direction:

LONG SQUEEZE:
Price must be Above VWAP.
VQS must be positive (≥ 0.70).
SHORT SQUEEZE:
Price must be Below VWAP.
VQS must be negative (≤ -0.70).
4. The "Gold Guard" & Safety Filters
Before a signal is actually generated, it passes through three final safety gates:

Sniper Alignment: The stock must have been identified by the OracleAgent earlier in the day as an UP_SNIPER (for Longs) or DOWN_SNIPER (for Shorts). If the signal is Long but the stock is a Down Sniper, it is ignored.
Precision Balance (35% - 65%): The system checks the Order Book imbalance. If the Bid % is > 65% or < 35%, it considers the move "exhausted" or "one-sided" and skips it.
Debounce Timer: To prevent "over-trading," the system will only allow one signal per stock every 5 minutes.
5. Automated Execution
If ENABLE_AUTO_TRADING is set to true, the system immediately fires a Dhan Super Order:

Entry: Market/Limit order at current price.
Stop Loss (SL): Set strictly at 2% from the entry price.
Take Profit (TP): Set strictly at 1% from the entry price (as per your current configuration).
Summary Table
Metric	Threshold	Rationale
Volume Surge	12x	Minimum required intensity to confirm institutional entry.
VQS Score	0.70	Ensures price is moving in a straight, high-velocity line.
VWAP	Above/Below	Confirms the trend is holding above the day's average cost.
Precision	35% to 65%	Prevents entering into "overcrowded" trades.
In the logs I reviewed earlier, you can see these surges being rejected if they don’t hit the 12.0x surge or 0.7 VQS required for the "Gold Guard" protection.