import pandas as pd
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import os
import pytz
from datetime import datetime
from src.db.schema import IntradayTick, DailyFocus
from dotenv import load_dotenv
import json

load_dotenv()

# --- DUPLICATED CORE LOGIC FROM SIGNAL_GENERATOR.PY FOR ISOLATION ---
def calculate_indicators(df, side='LONG'):
    # Volume Surge (20-min Moving Average - EXCLUDING current candle for Parity)
    df['Vol_Avg'] = df['Volume'].shift(1).rolling(window=20).mean()
    df['Vol_Surge'] = (df['Volume'] / df['Vol_Avg']).fillna(1.0)
    
    # RSI (SMA Method for 100% Research Parity)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['RSI'] = (100 - (100 / (1 + rs))).fillna(50.0) # Neutral RSI
    
    # MACD Histogram (MACD - Signal)
    exp12 = df['Close'].ewm(span=12, adjust=False).mean()
    exp26 = df['Close'].ewm(span=26, adjust=False).mean()
    macd_line = exp12 - exp26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    df['MACD'] = (macd_line - signal_line).fillna(0.0)
    
    # EMA Slope (Absolute Difference - Research Standard)
    df['MA20'] = df['Close'].rolling(window=20, min_periods=1).mean()
    df['Slope'] = df['MA20'].diff().fillna(0.0)

    # VQS Calculation
    candle_range = df['High'] - df['Low']
    if side == 'LONG':
        df['VQS'] = (df['Close'] - df['Low']) / candle_range.replace(0, np.nan)
    else:
        df['VQS'] = (df['High'] - df['Close']) / candle_range.replace(0, np.nan)
    df['VQS'] = df['VQS'].fillna(1.0)
    
    return df

def meets_filters(row, side, p, orb_h_std, orb_l_std, range_dir, htf_metrics=None, orb_h_cln=None, orb_l_cln=None, imbalance=None, is_live=False, bid_pct=None, ask_pct=None):
    reasons = []
    
    # 0. Order Flow Imbalance Guard
    enforce_imbalance = p.get('enforce_imbalance', False)
    min_imbalance = p.get('min_imbalance', 0.15)
    
    if enforce_imbalance and imbalance is not None:
        if side == 'LONG' and imbalance < min_imbalance:
            reasons.append(f"OrderFlow: Imbalance {imbalance:.2f} < {min_imbalance}")
        elif side == 'SHORT' and imbalance > -min_imbalance:
            reasons.append(f"OrderFlow: Imbalance {imbalance:.2f} > -{min_imbalance}")

    # 0.1 Bid/Ask Balance Guard (Elite Selection: 35% - 65%)
    if bid_pct is not None and ask_pct is not None:
        strength = bid_pct if side == 'LONG' else ask_pct
        if strength < 35.0:
            reasons.append(f"Balance: Weak {side} Interest ({strength:.1f}%) < 35%")
        elif strength > 65.0:
            reasons.append(f"Balance: Exhausted {side} Interest ({strength:.1f}%) > 65%")

    # Institutional HTF Filters
    htf_metrics = htf_metrics or {}
    w_rsi = htf_metrics.get('weekly_rsi')
    w_sma = htf_metrics.get('weekly_sma')
    adtv_cr = htf_metrics.get('adtv_cr', 0)

    # 1. Liquidity Guard
    min_adtv = p.get('min_adtv', 0.5)
    if adtv_cr < min_adtv: 
        reasons.append(f"Liquidity: ADTV {adtv_cr:.2f} Cr < {min_adtv}")

    # 2. Weekly RSI Guard
    if w_rsi is not None:
        if side == 'LONG':
            rsi_range = p.get('weekly_rsi_l', [60, 100])
            if not (rsi_range[0] <= w_rsi <= rsi_range[1]):
                reasons.append(f"HTF: Weekly RSI {w_rsi:.1f} not in {rsi_range}")
        else: # SHORT
            rsi_range = p.get('weekly_rsi_s', [0, 40])
            if not (rsi_range[0] <= w_rsi <= rsi_range[1]):
                reasons.append(f"HTF: Weekly RSI {w_rsi:.1f} not in {rsi_range}")

    # 3. Weekly SMA Alignment Guard
    if w_sma is not None:
        if side == 'LONG' and p.get('sma_align_l', True) and row['Close'] < w_sma:
            reasons.append(f"HTF: Price below Weekly SMA {w_sma:.2f}")
        if side == 'SHORT' and p.get('sma_align_s', True) and row['Close'] > w_sma:
            reasons.append(f"HTF: Price above Weekly SMA {w_sma:.2f}")

    # 4. Volatility Gate
    vol_min = p.get('vol_min', 0.9)
    # If is_live, we might be on a forming bar, so Vol_Surge might be low if just started.
    # However, for consistency, we use what's calculated.
    current_surge = row.get('Vol_Surge', 1.0)
    if current_surge < vol_min:
        reasons.append(f"Low Volatility: {current_surge:.2f}x < {vol_min}x")

    # 4.1 VQS Quality / Momentum
    vqs_threshold = p.get('vol_quality', 0.15)
    vqs_val = row.get('VQS', 1.0)
    if vqs_val < vqs_threshold:
        reasons.append(f"Low VQS Quality: {vqs_val:.2f} < {vqs_threshold}")

    # 5. RSI Momentum
    rsi = row.get('RSI', 50.0)
    if side == 'LONG':
        r_min, r_max = p.get('rsi_l_min', 65), p.get('rsi_l_max', 75)
        if not (r_min <= rsi <= r_max):
            reasons.append(f"RSI {rsi:.1f} outside {r_min}-{r_max}")
    else: # SHORT
        r_min, r_max = p.get('rsi_s_min', 30), p.get('rsi_s_max', 45)
        if not (r_min <= rsi <= r_max):
            reasons.append(f"RSI {rsi:.1f} outside {r_min}-{r_max}")

    # 6. MACD Histogram
    macd = row.get('MACD', 0.0)
    if side == 'LONG':
        m_min = p.get('macd_l_min', 0.1)
        if macd < m_min:
            reasons.append(f"MACD {macd:.2f} < {m_min}")
    else: # SHORT
        m_max = p.get('macd_s_max', 0.0)
        if macd > m_max:
            reasons.append(f"MACD {macd:.2f} > {m_max}")

    # 7. EMA Slope (Trend)
    slope = row.get('Slope', 0.0)
    if side == 'LONG':
        s_min = p.get('trend_l_min', 0.0)
        if slope < s_min:
            reasons.append(f"Trend: Slope {slope:.2f} < {s_min}")
    else: # SHORT
        s_max = p.get('trend_s_max', -0.05)
        if slope > s_max:
            reasons.append(f"Trend: Slope {slope:.2f} > {s_max}")

    # 8. Directional Bias check
    if p.get('enforce_bias', True):
        if range_dir == 'BULLISH' and side == 'SHORT': reasons.append("Range Bias: BEARISH signal in BULLISH Range")
        if range_dir == 'BEARISH' and side == 'LONG': reasons.append("Range Bias: BULLISH signal in BEARISH Range")
            
    return (len(reasons) == 0, reasons, {})

class DailyAnalysisService:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

    def run_analysis(self):
        """Replays today's market data to find all opportunities."""
        session = self.Session()
        results = []
        try:
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)
            today = now_ist.date()
            
            # Load Presets
            preset_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'strategy_presets.json')
            with open(preset_file, "r") as f:
                presets = json.load(f)
            p = presets.get("sateesh", {}).copy()

            # Get Focus Stocks
            focus_stocks = session.query(DailyFocus).filter(DailyFocus.date >= today).all()
            if not focus_stocks: return []

            print(f"AnalysisService: Replaying {len(focus_stocks)} stocks...")

            for stock in focus_stocks:
                # Get Intraday Data
                ticks = session.query(IntradayTick).filter(
                    IntradayTick.symbol == stock.symbol,
                    IntradayTick.timestamp >= today
                ).order_by(IntradayTick.timestamp.asc()).all()
                
                if not ticks: continue
                
                # Convert to DF
                data = [{'Datetime': t.timestamp, 'Open': t.open, 'High': t.high, 'Low': t.low, 'Close': t.close, 'Volume': t.volume} for t in ticks]
                df_all = pd.DataFrame(data).set_index('Datetime')
                
                orb_h = stock.orb_high_clean if stock.orb_high_clean else stock.orb_high
                orb_l = stock.orb_low_clean if stock.orb_low_clean else stock.orb_low
                range_dir = stock.orb_direction or "NEUTRAL"
                oracle_status = stock.oracle_status or "NEUTRAL"

                if not orb_h or not orb_l: continue

                # Calculate Indicators
                df_processed = calculate_indicators(df_all.copy(), side='LONG' if oracle_status == 'UP_SNIPER' else 'SHORT')

                in_trade = False
                trade_side = None
                sl_price = 0
                target_price = 0
                
                for ts, row in df_processed.iterrows():
                    curr_time = ts.strftime("%H:%M")
                    
                    # Manage Active Trade Simulation
                    if in_trade:
                        outcome = None
                        
                        if trade_side == 'LONG':
                            if row['High'] >= target_price: outcome = 'TARGET_HIT'
                            elif row['Low'] <= sl_price: outcome = 'SL_HIT'
                        else: # SHORT
                            if row['Low'] <= target_price: outcome = 'TARGET_HIT'
                            elif row['High'] >= sl_price: outcome = 'SL_HIT'
                        
                        if outcome:
                            if results and results[-1]['symbol'] == stock.symbol and results[-1]['status'] == 'OPEN':
                                results[-1]['status'] = outcome
                                results[-1]['exit_time'] = ts
                                results[-1]['pnl_pct'] = p.get('tp_pct', 1.0) if outcome == 'TARGET_HIT' else -p.get('sl_pct', 0.5)
                            in_trade = False
                            continue

                    # Look for Entries
                    if not in_trade:
                        strat_start = p.get('start', '09:30')[:5]
                        if curr_time < strat_start: continue
                        if curr_time > "15:20": break

                        signal_found = False
                        signal_side = None
                        
                        # LONG
                        if row['High'] > orb_h:
                            is_sniper = oracle_status in p.get('long_snipers', ['UP_SNIPER'])
                            is_valid, _, _ = meets_filters(row, 'LONG', p, stock.orb_high, stock.orb_low, range_dir, htf_metrics={'weekly_rsi': stock.weekly_rsi, 'weekly_sma': stock.weekly_sma, 'adtv_cr': stock.avg_daily_turnover})
                            if is_sniper and is_valid:
                                signal_found = True
                                signal_side = 'LONG'

                        # SHORT
                        elif row['Low'] < orb_l:
                            is_sniper = oracle_status in p.get('short_snipers', ['DOWN_SNIPER'])
                            is_valid, _, _ = meets_filters(row, 'SHORT', p, stock.orb_high, stock.orb_low, range_dir, htf_metrics={'weekly_rsi': stock.weekly_rsi, 'weekly_sma': stock.weekly_sma, 'adtv_cr': stock.avg_daily_turnover})
                            if is_sniper and is_valid:
                                signal_found = True
                                signal_side = 'SHORT'

                        if signal_found:
                            in_trade = True
                            trade_side = signal_side
                            entry_price = row['Close']
                            
                            sl_pct = p.get('sl_pct', 0.5)
                            tp_pct = p.get('tp_pct', 1.0)
                            
                            if trade_side == 'LONG':
                                if p.get('sl_type') == 'ORB_BOUNDARY':
                                    sl_price = stock.orb_low
                                else:
                                    sl_price = entry_price * (1 - sl_pct/100)
                                target_price = entry_price * (1 + tp_pct/100)
                            else:
                                if p.get('sl_type') == 'ORB_BOUNDARY':
                                    sl_price = stock.orb_high
                                else:
                                    sl_price = entry_price * (1 + sl_pct/100)
                                target_price = entry_price * (1 - tp_pct/100)

                            results.append({
                                'entry_ts': ts.isoformat(),
                                'symbol': stock.symbol,
                                'side': signal_side,
                                'type': 'BREAKOUT' if signal_side == 'LONG' else 'BREAKDOWN',
                                'price': entry_price,
                                'sl': round(sl_price, 2),
                                'tp': round(target_price, 2),
                                'status': 'OPEN',
                                'pnl_pct': 0.0,
                                'rsi': round(float(row['RSI']), 2),
                                'vol': round(float(row['Vol_Surge']), 2),
                                'exit_time': None
                            })

            return results

        except Exception as e:
            print(f"Analysis Error: {e}")
            import traceback
            traceback.print_exc()
            return []
        finally:
            session.close()
