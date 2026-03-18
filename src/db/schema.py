from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta, timezone
import os

def get_ist_now():
    # IST is UTC+5:30. Return naive datetime for DB compatibility.
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist_tz).replace(tzinfo=None)

Base = declarative_base()

class TradeRecommendation(Base):
    __tablename__ = 'trade_recommendations'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50), nullable=False) # Changed length
    signal_type = Column(String(20))  # BREAKUP, BREAKDOWN, REVERSION # Changed length
    entry_price = Column(Float)
    stop_loss = Column(Float)
    target_1 = Column(Float)
    target_2 = Column(Float)
    target_3 = Column(Float) # SWING Target
    hold_type = Column(String(20), default="INTRADAY") # INTRADAY, SWING
    daily_ema_sl = Column(Float) # Trailing SL for Swing
    confidence_score = Column(Float)
    agent_votes = Column(JSON)  # Store votes from all agents
    timestamp = Column(DateTime, default=get_ist_now)
    outcome = Column(String(255))  # SUCCESS, FAILURE, EXITED
    pnl = Column(Float)

class PerformanceLog(Base):
    __tablename__ = 'performance_logs'
    
    id = Column(Integer, primary_key=True)
    setup_name = Column(String(255))  # e.g., "15m Range Breakout"
    failure_reason = Column(String(255))
    timestamp = Column(DateTime, default=get_ist_now)

class Ticker(Base):
    __tablename__ = 'tickers'
    
    symbol = Column(String(50), primary_key=True)
    sector = Column(String(50)) # e.g., NIFTY_IT, NIFTY_BANK
    weekly_rsi = Column(Float)
    weekly_sma = Column(Float)
    oracle_status = Column(String(50))  # SNIPER, FILTERED
    scout_score = Column(Float)
    tech_score = Column(Float)
    mood_score = Column(Float)
    sentiment_score = Column(Float)
    confidence_score = Column(Float)
    sentiment_updated_at = Column(DateTime)
    # ORB Analysis
    ORB_high = Column(Float)
    ORB_low = Column(Float)
    ORB_high_clean = Column(Float) # Body High
    ORB_low_clean = Column(Float) # Body Low
    ORB_direction = Column(String(20)) # BULLISH, BEARISH, NEUTRAL
    ORB_range_pct = Column(Float)
    ORB_window = Column(Integer) # Duration in minutes (e.g., 15, 30)
    breakout_events = Column(JSON)  # List of HH:MM strings with strength metrics
    breakdown_events = Column(JSON) # List of HH:MM strings with strength metrics
    avg_daily_turnover = Column(Float) # 20-week ADTV in ₹ Crores (Cr)
    dhan_index_name = Column(JSON) # Array of associated indices for feed subscription
    last_updated = Column(DateTime, default=get_ist_now, onupdate=get_ist_now)

class Portfolio(Base):
    __tablename__ = 'portfolio'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50))
    qty = Column(Integer)
    avg_price = Column(Float)
    invested_amount = Column(Float)
    target_price = Column(Float)
    stop_loss = Column(Float)
    hold_type = Column(String(20), default="INTRADAY")
    entry_time = Column(DateTime, default=get_ist_now)

class RealizedPnL(Base):
    __tablename__ = 'realized_pnl'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50))
    buy_price = Column(Float)
    sell_price = Column(Float)
    qty = Column(Integer)
    profit_amount = Column(Float)
    profit_pct = Column(Float)
    timestamp = Column(DateTime, default=get_ist_now)

class CatalystScan(Base):
    __tablename__ = 'catalyst_scans'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50))
    scan_type = Column(String(20)) # CATALYST_HIT, CATALYST_NEAR
    current_price = Column(Float)
    volume_multiplier = Column(Float)
    dist_to_high_pct = Column(Float)
    sentiment_score = Column(Float)
    target_1 = Column(Float)
    target_2 = Column(Float)
    target_3 = Column(Float)
    stop_loss = Column(Float)
    timestamp = Column(DateTime, default=get_ist_now)

class AlphaSignal(Base):
    __tablename__ = 'alpha_signals'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50))
    signal_date = Column(DateTime)
    price = Column(Float)
    score = Column(Float)
    rs_score = Column(Float)
    vol_mag = Column(Float)
    pcr = Column(Float)
    sentiment = Column(String(50))
    target = Column(Float)
    stop_loss = Column(Float)
    status = Column(String(20), default="ACTIVE") # ACTIVE, HIT, FAILED
    timestamp = Column(DateTime, default=get_ist_now)

class ScanLog(Base):
    __tablename__ = 'scan_logs'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(255), nullable=False)
    price = Column(Float)
    volume_ratio = Column(Float)
    is_open_low = Column(Integer)  # 1 for True, 0 for False
    is_open_high = Column(Integer)
    day_change_pct = Column(Float)
    scout_score = Column(Float)
    mood_score = Column(Float)
    final_confidence = Column(Float)
    status = Column(String(255))  # REJECTED, CANDIDATE, RECOMMENDED
    signals = Column(JSON)
    timestamp = Column(DateTime, default=datetime.utcnow)

class IcebergAlert(Base):
    """Refined institutional absorption alerts"""
    __tablename__ = 'iceberg_alerts'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50), index=True)
    timestamp = Column(DateTime, index=True)
    ltp = Column(Float)
    action = Column(String(255)) # BUY, SELL
    created_at = Column(DateTime, default=get_ist_now)

class IntradayTick(Base):
    __tablename__ = 'intraday_ticks'
    
    symbol = Column(String(50), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    total_turnover = Column(Float)
    source = Column(String(20)) # TRUEDATA, DHAN_REST
    
    # Order Flow Intelligence (NEW)
    buy_volume = Column(Float, default=0.0)
    sell_volume = Column(Float, default=0.0)
    avg_bid_qty = Column(Float, default=0.0)
    avg_ask_qty = Column(Float, default=0.0)
    mean_imbalance = Column(Float, default=0.0)
    iceberg_score = Column(Float, default=0.0) # 0.0 to 1.0 (Detection Confidence)
    iceberg_timestamp = Column(DateTime, nullable=True) # Exact time of detection
    iceberg_side = Column(String(10), nullable=True) # BUY or SELL
    total_bid_qty = Column(Float, default=0.0)
    total_ask_qty = Column(Float, default=0.0)
    bid_pct = Column(Float, default=50.0)
    ask_pct = Column(Float, default=50.0)
    
    # Option Chain & OI Intelligence (NEW)
    oi = Column(Float, default=0.0) # Real-time Futures OI
    pcr_oi = Column(Float, default=0.0)
    pcr_vol = Column(Float, default=0.0)
    max_pain = Column(Float, default=0.0)

    last_updated = Column(DateTime, default=get_ist_now, onupdate=get_ist_now)


class DailyFocus(Base):
    __tablename__ = 'daily_focus'
    
    symbol = Column(String(50), primary_key=True)
    date = Column(DateTime, primary_key=True)
    sector = Column(String(50))
    
    # Oracle Data (Start of Day)
    oracle_status = Column(String(50)) # SNIPER, FILTERED, IGNORE
    weekly_rsi = Column(Float)
    weekly_sma = Column(Float)
    scout_score = Column(Float)
    tech_score = Column(Float)
    
    # ORB Data (Updated at 9:32)
    orb_high = Column(Float)
    orb_low = Column(Float)
    orb_high_clean = Column(Float)
    orb_low_clean = Column(Float)
    orb_range_pct = Column(Float)
    orb_direction = Column(String(20))
    orb_window = Column(Integer) # 15 or 30
    
    # Macro Data (Updated Live)
    news_score = Column(Float)
    news_summary = Column(String(1000))
    market_mood = Column(String(50)) # Globalist
    sector_trend = Column(String(50)) # SectorAgent
    avg_daily_turnover = Column(Float) # 20-week ADTV in ₹ Crores (Cr)
    active_signal_id = Column(Integer) # Link to active trade if any
    last_updated = Column(DateTime, default=get_ist_now, onupdate=get_ist_now)

class SystemJob(Base):
    __tablename__ = 'system_jobs'
    
    job_id = Column(String(50), primary_key=True) # e.g., 'news_agent', 'intraday_feeder'
    status = Column(String(20)) # RUNNING, STOPPED, FAILED
    last_run = Column(DateTime)
    next_run = Column(DateTime)
    config = Column(JSON) # Interval, Toggles, Parameters
    error_log = Column(String(1000))

class Position(Base):
    """Enhanced Trade Management Table"""
    __tablename__ = 'positions'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50))
    side = Column(String(10)) # LONG, SHORT
    date = Column(DateTime)
    
    # Entry Details
    entry_time = Column(DateTime)
    entry_price = Column(Float)
    qty = Column(Integer)
    signal_type = Column(String(50)) # BREAKOUT, BREAKDOWN, REVERSION
    
    # Risk Management
    sl = Column(Float)
    tp = Column(Float)
    sl_type = Column(String(20)) # ORB_BOUNDARY, PERCENTAGE
    
    # Trade State
    status = Column(String(20)) # OPEN, CLOSED, PENDING, FAILED
    rider_active = Column(Integer, default=0) # Boolean 0/1
    max_profit = Column(Float, default=0.0)
    trail_sl = Column(Float)
    
    # Exit Details
    exit_time = Column(DateTime)
    exit_price = Column(Float)
    exit_reason = Column(String(50)) # TP, SL, SQUAREOFF, TRAIL_HIT
    pnl_abs = Column(Float)
    pnl_pct = Column(Float)
    
    # Order Tracking
    entry_order_id = Column(String(50)) # Dhan order ID for entry
    target_order_id = Column(String(50)) # Dhan order ID for target
    sl_order_id = Column(String(50)) # Dhan order ID for stop-loss
    
    # Rich Context for Replay/Audit
    entry_metrics = Column(JSON) # Snapshot of RSI, VolSurge, VQS, Slope etc.
    agent_audit_log = Column(Text) # Detailed decision log

class ORBSignal(Base):
    """Signals generated but not yet executed"""
    __tablename__ = 'orb_signals'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50))
    side = Column(String(10)) # LONG, SHORT
    date = Column(DateTime)
    
    # Entry Details
    timestamp = Column(DateTime, default=get_ist_now)
    entry_price = Column(Float)
    signal_type = Column(String(50)) # BREAKOUT, BREAKDOWN
    
    # Risk parameters suggested
    sl = Column(Float)
    tp = Column(Float)
    
    # Metrics Snapshot
    metrics = Column(JSON)
    status = Column(String(20), default="PENDING") # PENDING, EXECUTED, REJECTED, TARGET_HIT, SL_HIT
    execution_pos_id = Column(Integer) # Link to positions table once executed (entry order ID)
    target_order_id = Column(String(50)) # Dhan order ID for target order (super order)
    sl_order_id = Column(String(50)) # Dhan order ID for stop-loss order (super order)
    bid_pct = Column(Float)
    ask_pct = Column(Float)

class HistoryDailyOHLC(Base):
    """Historical Daily OHLCV data for long-term analysis"""
    __tablename__ = 'history_daily_ohlc'
    
    symbol = Column(String(50), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    oi = Column(Float, default=0.0)
    
    # Historical Options Sentiment
    pcr_oi = Column(Float, default=0.0)
    pcr_vol = Column(Float, default=0.0)
    max_pain = Column(Float, default=0.0)
    atm_iv = Column(Float, default=0.0) # Implied Volatility for VCP confirmation
    
    source = Column(String(20), default="TRUEDATA")
    last_updated = Column(DateTime, default=get_ist_now, onupdate=get_ist_now)

class HistoricalIntradayTick(Base):
    """Clean historical 1-minute OHLCV data from sources like Dhan"""
    __tablename__ = 'historical_intraday_ticks'
    
    symbol = Column(String(50), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    source = Column(String(20)) # e.g. DHAN_HISTORY
    last_updated = Column(DateTime, default=get_ist_now, onupdate=get_ist_now)

class ORBSignalNearMiss(Base):
    """Signals that failed criteria but are worth tracking"""
    __tablename__ = 'orb_signals_nearmiss'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50))
    side = Column(String(10)) # LONG, SHORT
    date = Column(DateTime)
    timestamp = Column(DateTime, default=get_ist_now)
    entry_price = Column(Float)
    signal_type = Column(String(50)) # BREAKOUT, BREAKDOWN
    
    # Risk parameters (Calculated even for near misses so we can promote them)
    sl = Column(Float)
    tp = Column(Float)
    
    # Why it missed
    fail_reasons = Column(JSON) # List of strings
    
    # Metrics Snapshot for context
    metrics = Column(JSON)

def init_db(engine):
    Base.metadata.create_all(engine)
