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