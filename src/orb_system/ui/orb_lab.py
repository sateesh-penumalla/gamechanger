import sys
import os
from datetime import datetime, time, timedelta

# Add root directory to path for imports - MUST BE AT TOP
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from src.orb_system.config.database import db
from src.orb_system.config.settings import settings
from src.orb_system.utils.helpers import get_ist_now
from src.orb_system.ui.backtester import BacktestingEngine

# Page Configuration
st.set_page_config(page_title="BharatQuant ORB Lab", layout="wide", page_icon="🎯")

# Styling - Light Mode
st.markdown("""
    <style>
    .main { background-color: #f6f8fa; color: #24292f; }
    .stMetric { background-color: #ffffff; padding: 20px; border-radius: 12px; border: 1px solid #d0d7de; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .stButton>button { background-color: #2ea44f; color: white; border-radius: 8px; width: 100%; border: none; font-weight: 600; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: #f6f8fa; border-radius: 4px 4px 0 0; gap: 1px; padding: 10px 20px; color: #57606a; }
    .stTabs [aria-selected="true"] { background-color: #ffffff; color: #24292f; font-weight: 600; border-bottom: 2px solid #fd8c73; }
    h1, h2, h3 { color: #24292f; }
    .stDataFrame { border: 1px solid #d0d7de; border-radius: 8px; }
    </style>
""", unsafe_allow_html=True)

# Helper for DB Fetching
def fetch_data(query, params=None):
    try:
        with db.get_cursor(dictionary=True) as cursor:
            cursor.execute(query, params or ())
            return pd.DataFrame(cursor.fetchall())
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()

# --- SIDEBAR: CONTROLS & PARAMS ---
st.sidebar.title("🎯 ORB Control Center")
mode = st.sidebar.radio("Lab Mode", ["Live Monitoring", "Backtesting Replay", "Parameter Lab"])

# Strategy Presets
@st.cache_data
def load_presets():
    import json
    preset_path = os.path.join(ROOT_DIR, "src", "config", "strategy_presets.json")
    if os.path.exists(preset_path):
        with open(preset_path, 'r') as f:
            return json.load(f)
    return {}

presets = load_presets()
preset_names = ["Custom"] + list(presets.keys())
selected_preset = st.sidebar.selectbox("Apply Strategy Preset", preset_names, index=0)

# Merge preset values if selected
current_params = {}
if selected_preset != "Custom":
    current_params = presets[selected_preset]
    st.sidebar.info(f"Using '{selected_preset}' profile settings.")

# Parameter UI
st.sidebar.subheader("📐 ORB Strategy")
orb_window = st.sidebar.slider("ORB Window (mins)", 5, 60, current_params.get("orb_window", 15)) # Window handled separately via timings if needed
st_style = 1 if current_params.get("orb_style") == "CLEAN" else 0
orb_style = st.sidebar.selectbox("ORB Style", ["STANDARD", "CLEAN"], index=st_style)
sl_strategy = st.sidebar.selectbox("SL Strategy", ["ORB_STANDARD", "ORB_CLEAN", "LAST_5M", "LAST_10M"], index=["ORB_STANDARD", "ORB_CLEAN", "LAST_5M", "LAST_10M"].index(current_params.get("sl_strategy", "ORB_CLEAN")))

# Finetune Indicators section
st.sidebar.subheader("⚡ Finetune Indicators")
# Use preset values if they exist, otherwise defaults
rsi_l_min = st.sidebar.slider("RSI Long Min", 0, 100, current_params.get("rsi_l_min", 60))
rsi_s_max = st.sidebar.slider("RSI Short Max", 0, 100, current_params.get("rsi_s_max", 45))
macd_l_min = st.sidebar.number_input("MACD Long Min", value=current_params.get("macd_l_min", 0.0), step=0.01)
macd_s_max = st.sidebar.number_input("MACD Short Max", value=current_params.get("macd_s_max", 0.0), step=0.01)
rsi_period = st.sidebar.number_input("RSI Period", 5, 30, current_params.get("rsi_period", 14))
macd_fast = st.sidebar.number_input("MACD Fast", 5, 20, current_params.get("macd_fast", 12))
macd_slow = st.sidebar.number_input("MACD Slow", 20, 50, current_params.get("macd_slow", 26))

# Risk Management Intervention
st.sidebar.subheader("🛡️ Risk Management")
sl_type = st.sidebar.radio("Stop Loss Type", ["Strategic", "Fixed %"], index=0)
fixed_sl_pct = 0.8
if sl_type == "Fixed %":
    fixed_sl_pct = st.sidebar.slider("Fixed SL %", 0.1, 2.0, 0.8)

max_risk_pct = st.sidebar.slider("Max Risk Cap %", 0.5, 5.0, settings.MAX_LOSS_PCT if hasattr(settings, 'MAX_LOSS_PCT') else 1.0)
t1_pct = st.sidebar.slider("Target 1 %", 0.3, 3.0, settings.TARGET_1_PCT if hasattr(settings, 'TARGET_1_PCT') else 0.5)
t2_pct = st.sidebar.slider("Target 2 %", 0.5, 6.0, settings.TARGET_2_PCT if hasattr(settings, 'TARGET_2_PCT') else 1.5)

# Scoring Thresholds
st.sidebar.subheader("🎯 Scoring Thresholds")
min_orb_quality = st.sidebar.slider("Min ORB Quality", 20, 100, settings.MIN_ORB_QUALITY_SCORE if hasattr(settings, 'MIN_ORB_QUALITY_SCORE') else 60)
min_confluence = st.sidebar.slider("Min Confluence", 20, 100, settings.MIN_CONFLUENCE_SCORE if hasattr(settings, 'MIN_CONFLUENCE_SCORE') else 50)
min_regime = st.sidebar.slider("Min Market Regime", 0, 100, settings.MIN_MARKET_REGIME_SCORE if hasattr(settings, 'MIN_MARKET_REGIME_SCORE') else 50)
min_conviction = st.sidebar.slider("Min Total Conviction", 50, 200, settings.MIN_TOTAL_CONVICTION if hasattr(settings, 'MIN_TOTAL_CONVICTION') else 100)

st.sidebar.subheader("🛡️ Advanced Filters & Risk")
use_vwap_filter = st.sidebar.toggle("Enforce VWAP Alignment", value=False, help="Long signals only if Price > VWAP")
partial_profits = st.sidebar.toggle("Partial Profit Booking", value=False, help="Book 50% at T1, move rest to Trailing SL")
trail_style = st.sidebar.selectbox("Trailing SL Style", ["None", "EMA-9", "Candle-Step", "ATR-Based"], index=0)
eod_exit = st.sidebar.toggle("Strict EOD Square-off (3:15 PM)", value=True)

# Backtesting Parameters packaging
backtest_params = {
    "orb_window": orb_window,
    "orb_style": orb_style,
    "sl_strategy": sl_strategy,
    "rsi_l_min": rsi_l_min,
    "rsi_s_max": rsi_s_max,
    "macd_l_min": macd_l_min,
    "macd_s_max": macd_s_max,
    "rsi_period": rsi_period,
    "macd_fast": macd_fast,
    "macd_slow": macd_slow,
    "sl_type": sl_type.upper(),
    "fixed_sl_pct": fixed_sl_pct,
    "max_risk_pct": max_risk_pct,
    "t1_pct": t1_pct,
    "t2_pct": t2_pct,
    "min_orb_quality": min_orb_quality,
    "min_confluence": min_confluence,
    "min_regime": min_regime,
    "min_conviction": min_conviction,
    "use_vwap_filter": use_vwap_filter,
    "partial_profits": partial_profits,
    "trail_style": trail_style,
    "eod_exit": eod_exit,
    "presets": current_params # Pass full preset for granular logic
}

if mode == "Parameter Lab":
    st.sidebar.subheader("📈 Confluence Weights")
    w_orb = st.sidebar.slider("Trend Weight", 0, 100, current_params.get("weights", {}).get("trend", 25))
    w_rsi = st.sidebar.slider("RSI Weight", 0, 100, current_params.get("weights", {}).get("rsi", 25))
    w_macd = st.sidebar.slider("MACD Weight", 0, 100, current_params.get("weights", {}).get("macd", 25))
    w_vol = st.sidebar.slider("Volume Weight", 0, 100, current_params.get("weights", {}).get("volume", 25))
    backtest_params.update({"weights": {"trend": w_orb, "rsi": w_rsi, "macd": w_macd, "volume": w_vol}})
    # The original "Finetune Indicators" section for Parameter Lab is now integrated above.
    # The old tech_weight, sent_weight, sector_weight are replaced by the new weights.
    # The old rsi_period, macd_fast, macd_slow are now general parameters.

# --- MAIN DASHBOARD ---
tab1, tab2, tab3 = st.tabs(["📊 Performance Overview", "🚀 Active Signals", "🏛️ Historical Evidence"])

# ... (tab1 and tab2 code remains same or similar)
with tab1:
    st.title("Performance Overview")
    m1, m2, m3, m4 = st.columns(4)
    trades_df = fetch_data("SELECT * FROM orb_trades ORDER BY entry_time DESC")
    if not trades_df.empty:
        total_trades = len(trades_df)
        wins = len(trades_df[trades_df['pnl_pct'] > 0])
        win_rate = (wins / total_trades) * 100
        net_pnl = trades_df['pnl_pct'].sum()
        avg_rr = abs(trades_df[trades_df['pnl_pct'] > 0]['pnl_pct'].mean() / trades_df[trades_df['pnl_pct'] < 0]['pnl_pct'].mean()) if any(trades_df['pnl_pct'] < 0) else 0
        m1.metric("Total Trades", total_trades)
        m2.metric("Win Rate", f"{win_rate:.1f}%")
        m3.metric("Net ROI %", f"{net_pnl:.2f}%")
        m4.metric("Avg R/R", f"{avg_rr:.2f}")
    else:
        st.info("No trades recorded yet.")

with tab2:
    st.title("Live Signals & Positions")
    col_sig, col_pos = st.columns(2)
    with col_sig:
        st.subheader("Latest Breaking Signals")
        signals_df = fetch_data("SELECT * FROM orb_signals WHERE date = CURDATE() ORDER BY signal_time DESC LIMIT 10")
        if not signals_df.empty:
            st.dataframe(signals_df[['symbol', 'signal_time', 'signal_type', 'entry_price', 'total_conviction', 'status']])
    with col_pos:
        st.subheader("Active Monitor")
        live_df = fetch_data("SELECT * FROM live_positions")
        if not live_df.empty: st.dataframe(live_df)

with tab3:
    st.title("Historical Evidence Replay")
    col1, col2 = st.columns([1, 3])
    with col1:
        date_select = st.date_input("Select Replay Date", value=get_ist_now().date() - timedelta(days=2))
        symbol_options = fetch_data("SELECT symbol FROM bharatquant_sniper.tickers WHERE oracle_status IN ('UP_SNIPER', 'DOWN_SNIPER')")
        all_symbols = symbol_options['symbol'].tolist() if not symbol_options.empty else ["RELIANCE", "TCS"]
        selected_symbols = st.multiselect("Select Symbols", options=all_symbols, default=all_symbols)
        run_btn = st.button("Launch High-Fidelity Replay")
    
    if run_btn:
        if not selected_symbols:
            st.warning("Please select symbols.")
        else:
            # Use the backtest_params defined globally in the sidebar section
            with st.spinner(f"Replaying Strategy..."):
                engine = BacktestingEngine()
                results_df = engine.run_backtest(selected_symbols, date_select, date_select, params=backtest_params)
                
                if not results_df.empty:
                    st.success(f"Replay Completed")
                    
                    total = len(results_df)
                    wins = len(results_df[results_df['pnl_pct'] > 0])
                    net_pnl = results_df['pnl_pct'].sum()
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Total Trades", total)
                    m2.metric("Win Rate", f"{(wins/total)*100:.1f}%")
                    m3.metric("Net PnL %", f"{net_pnl:.2f}%")
                    
                    st.subheader("Detailed Trade Logs")
                    # Expanded Columns for transparency
                    display_cols = ['symbol', 'type', 'entry_time', 'entry_price', 'orb_high', 'orb_low', 'range_pct', 'sl', 'sl_style', 'target_1', 'target_2', 'exit_time', 'exit_price', 'outcome', 'pnl_pct', 'breakdown']
                    st.dataframe(results_df[display_cols].style.applymap(lambda x: 'color: green' if x > 0 else 'color: red', subset=['pnl_pct']), use_container_width=True)
                else:
                    st.info("No trade signals generated for these settings.")
    
    st.markdown("---")
    st.write("🛠️ **Pro Tip:** Switch to 'CLEAN' mode or shorten the 'ORB Window' to tighten entries in volatile markets.")

st.markdown("---")
st.caption("GameChanger AI: BharatQuant ORB Intelligence Suite")
