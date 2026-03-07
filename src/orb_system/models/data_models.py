from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict
from enum import Enum

class SignalType(Enum):
    BREAKOUT = "BREAKOUT"
    BREAKDOWN = "BREAKDOWN"

class ExitReason(Enum):
    TARGET_1 = "TARGET_1"
    TARGET_2 = "TARGET_2"
    TARGET_3 = "TARGET_3"
    STOP_LOSS = "STOP_LOSS"
    TIME_STOP = "TIME_STOP"
    CHECKPOINT_5MIN_FAIL = "5MIN_FAIL"
    CHECKPOINT_15MIN_FAIL = "15MIN_FAIL"
    MANUAL = "MANUAL"
    SQUARE_OFF = "SQUARE_OFF"
    ACTIVE_DEFENSE = "ACTIVE_DEFENSE"

@dataclass
class ORBSetup:
    """ORB Setup data structure"""
    symbol: str
    date: datetime
    orb_high: float
    orb_low: float
    orb_range_pct: float
    orb_formation_volume: float
    
    # Metadata for UI / Clean ORB tracking
    clean_orb_high: Optional[float] = None
    clean_orb_low: Optional[float] = None
    orb_window_mins: int = 30
    orb_style: str = "STANDARD" # 'STANDARD' or 'CLEAN'
    
    # Scores
    orb_quality_score: int = 0
    range_score: int = 0
    volume_score: int = 0
    price_action_score: int = 0
    volatility_score: int = 0
    
    market_regime_score: int = 0
    sector_alignment_score: int = 0
    is_tradeable: bool = False
    news_sentiment_score: int = 50
    tradeable_reason: str = ""

@dataclass
class ConfluenceScores:
    """Confluence indicator scores"""
    rsi_score: int
    macd_score: int
    trend_score: int
    volume_score: int
    tech_score: int = 0  # 5-min technical pulse bonus
    total_score: int = 0
    
    # Raw values for reference
    rsi_value: float = 50.0
    macd_value: float = 0.0
    trend_value: float = 0.0
    volume_ratio: float = 1.0

@dataclass
class TradingSignal:
    """Trading signal with all conviction data"""
    symbol: str
    signal_time: datetime
    signal_type: SignalType
    entry_price: float
    orb_boundary: float
    confluence: ConfluenceScores
    orb_quality_score: int
    market_regime_score: int
    sector_alignment_score: int
    total_conviction: int
    position_size_pct: float
    stop_loss: float
    target_1: float
    target_2: float
    # Defaults must follow all non-defaults
    sl_strategy: str = "ORB_STANDARD"
    target_3: Optional[float] = None
    trailing_sl: Optional[float] = None
    tradeable_reason: str = ""

@dataclass
class TradeCheckpoint:
    """Trade checkpoint result"""
    checkpoint_time: datetime
    checkpoint_type: str  # '5MIN' or '15MIN'
    entry_price: float
    current_price: float
    move_pct: float
    result: str  # 'PASS', 'FAIL', 'NEUTRAL'
    action: str  # 'HOLD', 'EXIT', 'MOVE_STOP_TO_BE'

@dataclass
class Position:
    """Active position data"""
    trade_id: int
    symbol: str
    entry_price: float
    current_price: float
    quantity: int
    
    entry_time: datetime
    current_stop: float
    current_target: float
    target_3: Optional[float] = None
    
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    minutes_held: int = 0
    
    last_checkpoint: Optional[TradeCheckpoint] = None
    next_checkpoint_type: str = "5MIN"
    next_checkpoint_due: Optional[datetime] = None
    trail_activated: bool = False

@dataclass
class MarketRegime:
    """Market regime analysis"""
    date: datetime
    nifty_trend: str  # 'BULLISH', 'BEARISH', 'SIDEWAYS'
    nifty_ema20: float
    nifty_ema50: float
    india_vix: float
    advance_decline_ratio: float
    regime_score: int
    trade_orb: bool
    reason: str = ""
