-- =====================================================
-- ORB TRADING SYSTEM - DATABASE SCHEMA
-- =====================================================

USE orb_trading;

-- 1. Market Regime Table (Daily snapshot)
CREATE TABLE IF NOT EXISTS market_regime (
    date DATE PRIMARY KEY,
    nifty_trend VARCHAR(20),           -- 'BULLISH', 'BEARISH', 'SIDEWAYS'
    nifty_ema20 FLOAT,
    nifty_ema50 FLOAT,
    india_vix FLOAT,
    advance_decline_ratio FLOAT,
    regime_score INT,                   -- 0-100
    trade_orb BOOLEAN,                  -- TRUE if regime_score > 70
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Sector Performance Table (Daily)
CREATE TABLE IF NOT EXISTS sector_performance (
    date DATE,
    sector VARCHAR(50),
    sector_change_pct FLOAT,
    sector_trend VARCHAR(20),           -- 'UP', 'DOWN', 'FLAT'
    avg_volume_ratio FLOAT,
    is_leading BOOLEAN,
    PRIMARY KEY (date, sector)
);

-- 3. ORB Setup Table (Per stock, per day)
CREATE TABLE IF NOT EXISTS orb_setup (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    symbol VARCHAR(50),
    
    -- ORB Boundaries
    orb_high FLOAT,
    orb_low FLOAT,
    orb_range_pct FLOAT,
    orb_formation_volume FLOAT,         -- Avg volume during ORB period
    
    -- ORB Quality Score
    orb_quality_score INT,              -- 0-100
    range_score INT,                    -- 0-30
    volume_score INT,                   -- 0-25
    price_action_score INT,             -- 0-25
    volatility_score INT,               -- 0-20
    
    -- Market Context
    market_regime_score INT,
    sector_alignment_score INT,
    
    -- Trade Eligibility
    is_tradeable BOOLEAN,
    tradeable_reason TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_orb (date, symbol)
);

-- 4. ORB Signals Table (Entry signals)
CREATE TABLE IF NOT EXISTS orb_signals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    symbol VARCHAR(50),
    signal_time DATETIME,
    
    -- Signal Details
    signal_type VARCHAR(20),            -- 'BREAKOUT', 'BREAKDOWN'
    entry_price FLOAT,
    orb_boundary FLOAT,
    
    -- Confluence Scores
    confluence_score INT,               -- 0-100
    rsi_score INT,                      -- 0-25
    macd_score INT,                     -- 0-25
    trend_score INT,                    -- 0-25
    volume_score INT,                   -- 0-25
    
    -- Total Conviction
    total_conviction INT,               -- 0-400 (Quality + Confluence + Regime + Sector)
    position_size_pct FLOAT,            -- Based on conviction
    
    -- Trade Parameters
    stop_loss FLOAT,
    target_1 FLOAT,
    target_2 FLOAT,
    
    status VARCHAR(20),                 -- 'PENDING', 'ENTERED', 'REJECTED', 'EXPIRED'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Trades Table (Actual executed trades)
CREATE TABLE IF NOT EXISTS orb_trades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    signal_id INT,
    date DATE,
    symbol VARCHAR(50),
    
    -- Entry Details
    entry_time DATETIME,
    entry_price FLOAT,
    quantity INT,
    position_size_pct FLOAT,
    
    -- Trade Setup
    trade_type VARCHAR(20),             -- 'BREAKOUT', 'BREAKDOWN'
    conviction_score INT,
    stop_loss FLOAT,
    target_1 FLOAT,
    target_2 FLOAT,
    
    -- Checkpoints
    checkpoint_5min VARCHAR(20),        -- 'PASS', 'FAIL', 'NEUTRAL'
    checkpoint_15min VARCHAR(20),
    checkpoint_5min_price FLOAT,
    checkpoint_15min_price FLOAT,
    
    -- Exit Details
    exit_time DATETIME,
    exit_price FLOAT,
    exit_reason VARCHAR(50),            -- 'TARGET_1', 'TARGET_2', 'STOP_LOSS', 'TIME_STOP', 'MANUAL', '5MIN_FAIL'
    holding_minutes INT,
    
    -- P&L
    pnl_points FLOAT,
    pnl_pct FLOAT,
    pnl_amount FLOAT,
    
    -- Performance Metrics
    max_favorable_excursion FLOAT,      -- Best price achieved
    max_adverse_excursion FLOAT,        -- Worst price hit
    
    status VARCHAR(20),                 -- 'OPEN', 'CLOSED'
    
    -- Metadata
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (signal_id) REFERENCES orb_signals(id)
);

-- 6. Real-time Market Data Cache
CREATE TABLE IF NOT EXISTS market_data_cache (
    symbol VARCHAR(50),
    timestamp DATETIME,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    
    -- Technical Indicators (1-min)
    rsi_14 FLOAT,
    macd FLOAT,
    macd_signal FLOAT,
    macd_hist FLOAT,
    
    -- Calculated fields
    vwap FLOAT,
    volume_ratio FLOAT,                 -- Current vol / avg vol
    
    PRIMARY KEY (symbol, timestamp),
    INDEX idx_symbol_time (symbol, timestamp)
);

-- 7. Live Positions Monitor
CREATE TABLE IF NOT EXISTS live_positions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    trade_id INT,
    symbol VARCHAR(50),
    
    -- Position Details
    entry_price FLOAT,
    current_price FLOAT,
    quantity INT,
    
    -- Monitoring
    last_checkpoint_time DATETIME,
    next_checkpoint_type VARCHAR(20),   -- '5MIN', '15MIN', 'PROFIT_TRAIL'
    next_checkpoint_due DATETIME,
    
    -- Dynamic Management
    current_stop FLOAT,
    current_target FLOAT,
    trail_activated BOOLEAN DEFAULT FALSE,
    
    -- Real-time Stats
    unrealized_pnl FLOAT,
    unrealized_pnl_pct FLOAT,
    minutes_held INT,
    
    status VARCHAR(20),                 -- 'ACTIVE', 'MONITORING', 'EXITING'
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (trade_id) REFERENCES orb_trades(id),
    UNIQUE KEY unique_position (symbol)
);

-- 8. Stock Performance Tracker (Learning system)
CREATE TABLE IF NOT EXISTS stock_performance (
    symbol VARCHAR(50) PRIMARY KEY,
    
    -- ORB Statistics
    total_orb_trades INT DEFAULT 0,
    winning_trades INT DEFAULT 0,
    losing_trades INT DEFAULT 0,
    win_rate FLOAT,
    
    avg_win_pct FLOAT,
    avg_loss_pct FLOAT,
    avg_holding_minutes INT,
    
    -- Best/Worst
    best_trade_pct FLOAT,
    worst_trade_pct FLOAT,
    
    -- Tier Classification
    tier VARCHAR(10),                   -- 'A', 'B', 'C', 'BLACKLIST'
    tier_reason TEXT,
    
    -- Last Performance
    last_30_days_win_rate FLOAT,
    last_10_trades_result VARCHAR(100), -- e.g., "W,W,L,W,W,L,W,W,W,W"
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 9. System Logs
CREATE TABLE IF NOT EXISTS system_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    module VARCHAR(50),                 -- 'PRE_MARKET', 'ORB_FORMATION', 'EXECUTION', etc.
    level VARCHAR(20),                  -- 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    message TEXT,
    details JSON,
    INDEX idx_timestamp (timestamp),
    INDEX idx_module (module)
);

-- 10. Configuration Table
CREATE TABLE IF NOT EXISTS system_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value TEXT,
    description TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert default configurations
INSERT IGNORE INTO system_config (config_key, config_value, description) VALUES
('ORB_START_TIME', '09:15', 'ORB formation start time'),
('ORB_END_TIME', '09:45', 'ORB formation end time'),
('MARKET_REGIME_MIN_SCORE', '70', 'Minimum regime score to trade'),
('ORB_QUALITY_MIN_SCORE', '70', 'Minimum ORB quality to consider'),
('CONFLUENCE_MIN_SCORE', '75', 'Minimum confluence for entry'),
('TOTAL_CONVICTION_MIN', '300', 'Minimum total conviction (out of 400)'),
('MAX_POSITIONS', '3', 'Maximum concurrent positions'),
('POSITION_SIZE_BASE', '100000', 'Base position size in INR'),
('RISK_PER_TRADE_PCT', '1.0', 'Risk per trade as % of capital'),
('CHECKPOINT_5MIN_THRESHOLD', '0.2', 'Min move % for 5-min pass'),
('CHECKPOINT_15MIN_THRESHOLD', '0.4', 'Min move % for 15-min pass'),
('TARGET_1_PCT', '1.0', 'First target %'),
('TARGET_2_PCT', '1.5', 'Second target %'),
('MAX_LOSS_PER_TRADE_PCT', '0.8', 'Maximum loss per trade'),
('SQUARE_OFF_TIME', '15:10', 'Time to square off all positions'),
('A_TIER_MIN_WINRATE', '70', 'Minimum win rate for A-tier'),
('BLACKLIST_MAX_WINRATE', '40', 'Maximum win rate before blacklist');
