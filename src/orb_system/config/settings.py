from datetime import time
from typing import Dict, Any
import os

class TradingSettings:
    """System-wide trading settings for ORB System"""
    
    # Market Timings
    PRE_MARKET_START = time(8, 0)
    MARKET_OPEN = time(9, 15)
    ORB_FORMATION_END = time(9, 45)
    TRADING_END = time(14, 30)
    SQUARE_OFF_TIME = time(15, 10)
    MARKET_CLOSE = time(15, 30)
    
    # ORB Parameters
    ORB_PERIOD_MINUTES = 30  # 9:15 to 9:45
    
    # Scoring Thresholds
    MIN_MARKET_REGIME_SCORE = 50
    MIN_ORB_QUALITY_SCORE = 60
    MIN_CONFLUENCE_SCORE = 50
    MIN_TOTAL_CONVICTION = 100  # Conservative for backtesting
    
    # Indicator Stability
    INDICATOR_STABILITY_WINDOW = 15 # Minutes post-ORB to wait for technicals
    
    # Position Management
    MAX_CONCURRENT_POSITIONS = 3
    BASE_POSITION_SIZE = 100000  # INR
    RISK_PER_TRADE_PCT = 1.0
    
    # Checkpoint Thresholds
    CHECKPOINT_5MIN_MOVE_PCT = 0.2
    CHECKPOINT_15MIN_MOVE_PCT = 0.4
    
    # Targets
    TARGET_1_PCT = 1.0
    TARGET_2_PCT = 1.5
    TARGET_3_PCT = 3.0
    MAX_LOSS_PCT = 0.8
    
    # Stock Tier Criteria
    A_TIER_MIN_WINRATE = 70.0
    BLACKLIST_MAX_WINRATE = 40.0
    
    # Technical Indicator Settings
    RSI_PERIOD = 14
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    
    # Volume Analysis
    VOLUME_LOOKBACK_DAYS = 20
    
    @classmethod
    def load_from_db(cls, db_instance):
        """Load settings from database config table"""
        try:
            with db_instance.get_cursor() as cursor:
                cursor.execute("SELECT config_key, config_value FROM system_config")
                
                config_map: Dict[str, str] = {
                    'ORB_START_TIME': 'MARKET_OPEN',
                    'ORB_END_TIME': 'ORB_FORMATION_END',
                    'MARKET_REGIME_MIN_SCORE': 'MIN_MARKET_REGIME_SCORE',
                    'ORB_QUALITY_MIN_SCORE': 'MIN_ORB_QUALITY_SCORE',
                    'CONFLUENCE_MIN_SCORE': 'MIN_CONFLUENCE_SCORE',
                    'TOTAL_CONVICTION_MIN': 'MIN_TOTAL_CONVICTION',
                    'MAX_POSITIONS': 'MAX_CONCURRENT_POSITIONS',
                    'POSITION_SIZE_BASE': 'BASE_POSITION_SIZE',
                    'RISK_PER_TRADE_PCT': 'RISK_PER_TRADE_PCT',
                    'CHECKPOINT_5MIN_THRESHOLD': 'CHECKPOINT_5MIN_MOVE_PCT',
                    'CHECKPOINT_15MIN_THRESHOLD': 'CHECKPOINT_15MIN_MOVE_PCT',
                    'TARGET_1_PCT': 'TARGET_1_PCT',
                    'TARGET_2_PCT': 'TARGET_2_PCT',
                    'MAX_LOSS_PER_TRADE_PCT': 'MAX_LOSS_PCT',
                    'SQUARE_OFF_TIME': 'SQUARE_OFF_TIME',
                }
                
                rows = cursor.fetchall()
                for row in rows:
                    key = row['config_key']
                    if key in config_map:
                        attr_name = config_map[key]
                        raw_val = row['config_value']
                        
                        # Handle time conversion
                        if ':' in raw_val and len(raw_val) <= 5:
                            h, m = map(int, raw_val.split(':'))
                            value = time(h, m)
                        elif '.' in raw_val:
                            value = float(raw_val)
                        else:
                            try:
                                value = int(raw_val)
                            except:
                                value = raw_val
                                
                        setattr(cls, attr_name, value)
        except Exception as e:
            print(f"Error loading settings from DB: {e}")
            
        return cls

# Note: We'll initialize this later once DB is ready
settings = TradingSettings()
