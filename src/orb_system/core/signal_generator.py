from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from loguru import logger
import pandas as pd
from src.orb_system.config.database import db
from src.orb_system.config.settings import settings
from src.orb_system.models.data_models import TradingSignal, ORBSetup, SignalType, ConfluenceScores
from src.orb_system.services.market_data import MarketDataService
from src.orb_system.services.technical_indicators import TechnicalIndicators
from src.agents.chartist import ChartistAgent

class SignalGenerator:
    """Monitors for ORB breakouts and generates trading signals with high-fidelity confluence"""
    
    def __init__(self, market_data_service: MarketDataService):
        self.mds = market_data_service
        self.ti = TechnicalIndicators()
        self.chartist = ChartistAgent()
        
    def monitor_breakouts(self, setup: ORBSetup, sl_strategy: str = "ORB_STANDARD") -> Optional[TradingSignal]:
        """Watch for breakouts above ORB high or below ORB low"""
        symbol = setup.symbol
        trade_date = setup.date
        
        # Define monitoring window
        monitor_start = datetime.combine(trade_date, settings.MARKET_OPEN) + timedelta(minutes=setup.orb_window_mins)
        monitor_now = datetime.now()
        
        if monitor_now.time() > settings.TRADING_END:
            return None
            
        try:
             # Fetch latest 1-min data since ORB ended
             df = self.mds.get_historical_data(symbol, monitor_start, monitor_now, interval="1")
             
             if df.empty: return None
             
             last_price = df['close'].iloc[-1]
             last_time = df['timestamp'].iloc[-1]
             
             signal_type = None
             
             # Check for Breakout
             if last_price > setup.orb_high:
                 signal_type = SignalType.BREAKOUT
             # Check for Breakdown
             elif last_price < setup.orb_low:
                 signal_type = SignalType.BREAKDOWN
                 
             if signal_type:
                 return self._generate_signal(setup, signal_type, last_price, last_time, df, sl_strategy=sl_strategy)
                 
             return None
             
        except Exception as e:
            logger.error(f"Error monitoring breakouts for {symbol}: {e}")
            return None

    def _generate_signal(self, 
                        setup: ORBSetup, 
                        signal_type: SignalType, 
                        price: float, 
                        timestamp: datetime,
                        df: pd.DataFrame,
                        sl_strategy: str = "ORB_STANDARD",
                        params: Optional[Dict] = None) -> Optional[TradingSignal]:
        """Calculate confluence and finalize the signal"""
        params = params or {}
        presets = params.get('presets', {})
        
        # 0. Enforce Bias Check (if enabled)
        if presets.get('enforce_bias', False):
             # Only trade in direction of sector setup
             sector_status = setup.tradeable_reason # This is a bit hacky, check actual setup
             # Better check: compare signal with setup.sector_alignment_score or raw sector data
             if setup.sector_alignment_score < 20: # Neutral/Opposite
                  logger.info(f"Signal rejected for {setup.symbol}: Bias enforcement active and sector alignment weak.")
                  return None
        
        # 0.5 VWAP Alignment Filter (if enabled)
        if params.get('use_vwap_filter', False):
             vwap_series = self.ti.calculate_vwap(df)
             vwap = vwap_series.iloc[-1]
             if signal_type == SignalType.BREAKOUT and price < vwap:
                  logger.info(f"Signal rejected for {setup.symbol}: Price {price} < VWAP {vwap:.2f} (Long Bias required)")
                  return None
             elif signal_type == SignalType.BREAKDOWN and price > vwap:
                  logger.info(f"Signal rejected for {setup.symbol}: Price {price} > VWAP {vwap:.2f} (Short Bias required)")
                  return None

        # 1. Market Regime Check (Dynamic Threshold)
        min_regime = params.get('min_regime', settings.MIN_MARKET_REGIME_SCORE)
        if setup.market_regime_score < min_regime:
             logger.info(f"Signal rejected for {setup.symbol}: Market Regime {setup.market_regime_score} < {min_regime}")
             return None

        # 2. Calculate Confluence Scores
        confluence = self._calculate_confluence(setup, signal_type, df, params=params)
        
        # 2. Confluence Check (Dynamic Threshold)
        min_conf = params.get('min_confluence', settings.MIN_CONFLUENCE_SCORE)
        if confluence.total_score < min_conf:
             logger.info(f"Signal rejected for {setup.symbol}: Confluence {confluence.total_score} < {min_conf}")
             return None
             
        # 3. Final Conviction Filter
        total_conviction = setup.orb_quality_score + confluence.total_score + setup.market_regime_score + setup.sector_alignment_score
        min_conv = params.get('min_conviction', settings.MIN_TOTAL_CONVICTION)
        
        if total_conviction < min_conv:
            logger.info(f"Signal rejected for {setup.symbol}: Total Conviction {total_conviction} < {min_conv}")
            logger.info(f"Breakdown: ORB Qual: {setup.orb_quality_score}, Confluence: {confluence.total_score}, Regime: {setup.market_regime_score}, News: {setup.news_sentiment_score}")
            return None
            
        # 4. Define Trade Parameters with Flexible Stop Loss
        sl_type = params.get('sl_type', 'STRATEGIC')
        sl = 0.0
        
        if sl_type == "FIXED %":
            sl_dist = price * (params.get('fixed_sl_pct', 0.8) / 100)
            sl = price - sl_dist if signal_type == SignalType.BREAKOUT else price + sl_dist
        else: # STRATEGIC
            # Strategies: ORB_STANDARD, ORB_CLEAN, LAST_5M, LAST_10M
            sl_strategy = params.get('sl_strategy', 'ORB_STANDARD')
            if sl_strategy == "ORB_STANDARD":
                sl = setup.orb_low if signal_type == SignalType.BREAKOUT else setup.orb_high
            elif sl_strategy == "ORB_CLEAN":
                sl = setup.clean_orb_low if signal_type == SignalType.BREAKOUT else setup.clean_orb_high
            elif sl_strategy in ["LAST_5M", "LAST_10M"]:
                lookback = 5 if sl_strategy == "LAST_5M" else 10
                recent_df = df.tail(lookback)
                if not recent_df.empty:
                    sl = recent_df['low'].min() if signal_type == SignalType.BREAKOUT else recent_df['high'].max()
                else:
                    sl = setup.orb_low if signal_type == SignalType.BREAKOUT else setup.orb_high
        
        # Guard SL with Max Risk Pct
        risk_pct = abs(price - sl) / price * 100
        max_risk = params.get('max_risk_pct', settings.MAX_LOSS_PCT)
        
        logger.debug(f"DEBUG SL for {setup.symbol}: Strategy={sl_strategy}, Type={sl_type}, RawSL={sl}, Entry={price}, Risk={risk_pct:.2f}%, MaxRisk={max_risk}%")
        
        if risk_pct > max_risk:
             logger.info(f"SL capped for {setup.symbol} by Max Risk ({risk_pct:.2f}% > {max_risk}%)")
             sl = price * (1 - max_risk/100) if signal_type == SignalType.BREAKOUT else price * (1 + max_risk/100)
             logger.debug(f"DEBUG SL New Capped SL: {sl}")
        
        # Profit Targets (Intervention over settings)
        t1_val = params.get('t1_pct', settings.TARGET_1_PCT)
        t2_val = params.get('t2_pct', settings.TARGET_2_PCT)
        
        target_1 = price * (1 + t1_val/100) if signal_type == SignalType.BREAKOUT else price * (1 - t1_val/100)
        target_2 = price * (1 + t2_val/100) if signal_type == SignalType.BREAKOUT else price * (1 - t2_val/100)
        
        # 5. Technical Confirmation Bonus
        tech_analysis = self.chartist.analyze_technical_setup(df)
        if tech_analysis.get('conviction', 0) > 50:
             total_conviction += 25
             
        orb_boundary = setup.orb_high if signal_type == SignalType.BREAKOUT else setup.orb_low
        
        signal = TradingSignal(
            symbol=setup.symbol,
            signal_time=timestamp,
            signal_type=signal_type,
            entry_price=price,
            orb_boundary=orb_boundary,
            sl_strategy=sl_strategy,
            confluence=confluence,
            orb_quality_score=setup.orb_quality_score,
            market_regime_score=setup.market_regime_score,
            sector_alignment_score=setup.sector_alignment_score,
            total_conviction=total_conviction,
            position_size_pct=(total_conviction / 400) * 100,
            stop_loss=sl,
            target_1=target_1,
            target_2=target_2,
            tradeable_reason=setup.tradeable_reason
        )
        
        # Log Signal
        self._log_signal(signal)
        return signal

    def _calculate_confluence(self, 
                             setup: ORBSetup, 
                             signal_type: SignalType, 
                             df: pd.DataFrame,
                             params: Optional[Dict] = None) -> ConfluenceScores:
        """Fetch and score indicators (RSI, MACD, Trend, Volume)"""
        params = params or {}
        presets = params.get('presets', {})
        
        symbol = setup.symbol
        trade_date = setup.date
        closes = df['close'].tolist()
        
        # Override periods from params if provided
        rsi_period = params.get('rsi_period', 14)
        macd_fast = params.get('macd_fast', 12)
        macd_slow = params.get('macd_slow', 26)
        
        rsi = self.ti.calculate_rsi(closes, period=rsi_period)
        macd, signal, hist = self.ti.calculate_macd(closes, fast=macd_fast, slow=macd_slow)
        trend = self.ti.calculate_trend_strength(closes)
        
        # Volume ratio relative to ORB formation avg
        current_vol = df['volume'].iloc[-1]
        orb_avg_vol = setup.orb_formation_volume if setup.orb_formation_volume > 0 else 1.0
        vol_ratio = current_vol / orb_avg_vol
        
        # Logic: If preset thresholds exist, they provide a binary 'pass/fail' or base weight
        # Otherwise use standard scoring
        
        rsi_score = self.ti.score_rsi(rsi, signal_type)
        if signal_type == SignalType.BREAKOUT and "rsi_l_min" in params:
            if not (params["rsi_l_min"] <= rsi <= params.get("rsi_l_max", 100)): rsi_score = -50
        elif signal_type == SignalType.BREAKDOWN and "rsi_s_min" in params:
            if not (params["rsi_s_min"] <= rsi <= params["rsi_s_max"]): rsi_score = -50
            
        macd_score = self.ti.score_macd(macd, signal_type)
        if signal_type == SignalType.BREAKOUT and "macd_l_min" in params:
             if macd < params["macd_l_min"]: macd_score = -50
        elif signal_type == SignalType.BREAKDOWN and "macd_s_max" in params:
             if macd > params["macd_s_max"]: macd_score = -50
             
        trend_score = self.ti.score_trend(trend, signal_type)
        volume_score = self.ti.score_volume(vol_ratio)
        
        # Apply Custom weights if provided
        weights = params.get('weights', {})
        if weights:
            # Rebalance scores based on weights (assuming weights sum to 100 roughly)
            rsi_score = int(rsi_score * (weights.get('rsi', 25) / 25))
            macd_score = int(macd_score * (weights.get('macd', 25) / 25))
            trend_score = int(trend_score * (weights.get('trend', 25) / 25))
            volume_score = int(volume_score * (weights.get('volume', 25) / 25))
        
        return ConfluenceScores(
            rsi_score=rsi_score,
            macd_score=macd_score,
            trend_score=trend_score,
            volume_score=volume_score,
            total_score=rsi_score + macd_score + trend_score + volume_score,
            rsi_value=rsi,
            macd_value=macd,
            trend_value=trend,
            volume_ratio=vol_ratio
        )

    def _log_signal(self, signal: TradingSignal):
        """Persist signal to database"""
        try:
            with db.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO orb_signals 
                    (date, symbol, signal_time, signal_type, entry_price, orb_boundary,
                     confluence_score, rsi_score, macd_score, trend_score, volume_score,
                     total_conviction, position_size_pct, stop_loss, target_1, target_2, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    signal.signal_time.date(), signal.symbol, signal.signal_time,
                    signal.signal_type.value, signal.entry_price, signal.orb_boundary,
                    signal.confluence.total_score, signal.confluence.rsi_score,
                    signal.confluence.macd_score, signal.confluence.trend_score,
                    signal.confluence.volume_score, signal.total_conviction,
                    signal.position_size_pct, signal.stop_loss, signal.target_1,
                    signal.target_2, 'PENDING'
                ))
        except Exception as e:
            logger.error(f"Error logging signal for {signal.symbol}: {e}")
