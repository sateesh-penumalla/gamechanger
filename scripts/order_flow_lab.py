import os
import sys

# Standardize project root injection
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"DEBUG: Order Flow Lab starting from {project_root}")

import streamlit as st
import pandas as pd
import json
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, time, timedelta
import importlib

import src.services.order_flow_engine as order_flow_engine_mod
from src.services.order_flow_engine import OrderFlowEngine

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Page Configuration
st.set_page_config(page_title="Order Flow Lab", layout="wide", page_icon="🌊")

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
engine_db = create_engine(DB_URL) if DB_URL else None

st.markdown("""
    <style>
    .main { background-color: #0e1117; color: white; }
    .stMetric { background-color: #1f2937; padding: 15px; border-radius: 10px; border: 1px solid #374151; }
    </style>
""", unsafe_allow_html=True)

# --- UTILS ---
def get_oracle_info(symbol, date_str):
    if not engine_db: return {}
    with engine_db.connect() as conn:
        # Check if date is today to query live tickers table
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        if date_str == today_str:
            query = text("SELECT oracle_status, weekly_rsi, weekly_sma FROM tickers WHERE symbol = :s")
            res = conn.execute(query, {"s": symbol}).fetchone()
        else:
            query = text("SELECT oracle_status, weekly_rsi, weekly_sma FROM history_testing WHERE symbol = :s AND trade_date = :d")
            res = conn.execute(query, {"s": symbol, "d": date_str}).fetchone()
            
        if res:
            return {"status": res[0], "weekly_rsi": res[1], "weekly_sma": res[2]}
    return {}

# --- SIDEBAR ---
st.sidebar.header("🌊 Order Flow Backtester")

# Data Discovery
base_dir = "data/historical_ticks"
available_symbols = []
if os.path.exists(base_dir):
    available_symbols = sorted([d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))])

selected_symbol = st.sidebar.selectbox("Select Stock", options=available_symbols)

available_dates = []
if selected_symbol:
    symbol_path = os.path.join(base_dir, selected_symbol)
    available_dates = sorted([f.replace(".parquet", "") for f in os.listdir(symbol_path) if f.endswith(".parquet")], reverse=True)

selected_date_str = st.sidebar.selectbox("Select Date", options=available_dates)

# Fetch Oracle Context
oracle_ctx = get_oracle_info(selected_symbol, selected_date_str)
if oracle_ctx:
    st.sidebar.info(f"📍 Oracle: {oracle_ctx['status']} | RSI: {oracle_ctx.get('weekly_rsi', 'N/A')}")
else:
    st.sidebar.warning("⚠️ No Oracle data found for this date/symbol.")

st.sidebar.divider()
st.sidebar.subheader("📐 ORB & Flow Params")
orb_window = st.sidebar.select_slider("ORB Window (Min)", options=[15, 30, 45, 60], value=15)
imbalance_thr = st.sidebar.slider("Flow Imbalance Threshold", 0.0, 1.0, 0.6, step=0.05)

st.sidebar.subheader("🕰️ Time Constraints")
start_time = st.sidebar.text_input("Signal Start (HH:MM)", value="09:30")
end_time = st.sidebar.text_input("Signal End (HH:MM)", value="14:00")

st.sidebar.subheader("🏛️ Institutional Filters")
oracle_filter = st.sidebar.toggle("Oracle Sniper Alignment", value=True, help="Long only if UP_SNIPER, Short only if DOWN_SNIPER.")
htf_filter = st.sidebar.toggle("Weekly RSI Guard", value=False, help="Long only if Weekly RSI > 60, Short only if Weekly RSI < 40.")

st.sidebar.subheader("🎯 Execution Filters")
use_vwap = st.sidebar.toggle("VWAP Alignment", value=True, help="Price must be above VWAP for Longs and below for Shorts.")
use_vol_surge = st.sidebar.toggle("Volume Surge", value=False, help="Signal must have significantly higher volume than rolling avg.")
vol_surge_thr = st.sidebar.slider("Surge Threshold (x)", 1, 30, 10) if use_vol_surge else 10
use_vqs = st.sidebar.toggle("VQS Momentum (5s)", value=True, help="Only enter if last 5 seconds show consistent one-way move.")
use_spread = st.sidebar.toggle("Spread Filter", value=True, help="Skip if Bid-Ask spread is too wide.")
max_spread = st.sidebar.slider("Max Spread %", 0.01, 0.2, 0.05, step=0.01) if use_spread else 0.05

st.sidebar.subheader("🛡️ Risk Management")
sl_type = st.sidebar.radio("Stop Loss Method", options=["FIXED_PCT", "ORB_BOUNDARY"], index=0, help="FIXED_PCT uses the slider below. ORB_BOUNDARY uses the opposite extreme of the opening range.")
sl_pct = 0.5
if sl_type == "FIXED_PCT":
    sl_pct = st.sidebar.slider("Hard Stop Loss (%)", 0.1, 5.0, 0.5, step=0.1)

tp_pct = st.sidebar.slider("Target 1 (TP) %", 0.1, 5.0, 1.0, step=0.1)

enable_tsl = st.sidebar.toggle("Enable Trailing Stop Loss", value=True)
tsl_step = 0.2
if enable_tsl:
    tsl_step = st.sidebar.slider("TSL Step %", 0.05, 2.0, 0.2, step=0.05)

# --- MAIN ---
# --- MAIN ---
st.title(f"🌊 Order Flow Lab: {selected_symbol}")

tab1, tab2 = st.tabs(["📊 Backtester", "🛰️ Live Signal Monitor"])

with tab1:
    st.caption(f"Analyzing {selected_date_str} at millisecond fidelity")
    
    if not selected_symbol or not selected_date_str:
        st.info("Please select a symbol and date in the sidebar.")
    else:
        selected_date = datetime.strptime(selected_date_str, "%Y-%m-%d")

        # Execution
        if st.sidebar.button("⚡ Run Backtest", use_container_width=True):
            importlib.reload(order_flow_engine_mod)
            from src.services.order_flow_engine import OrderFlowEngine
            engine = OrderFlowEngine(selected_symbol, selected_date)
            
            with st.spinner("Replaying Ticks..."):
                if engine.load_data():
                    config = {
                        'orb_window': orb_window,
                        'imbalance_threshold': imbalance_thr,
                        'tp_pct': tp_pct,
                        'sl_pct': sl_pct,
                        'sl_type': sl_type,
                        'enable_tsl': enable_tsl,
                        'tsl_step': tsl_step,
                        'oracle_filter': oracle_filter,
                        'oracle_status': oracle_ctx.get('status', 'N/A'),
                        'start_time': start_time,
                        'end_time': end_time,
                        'use_vwap_filter': use_vwap,
                        'use_vol_surge': use_vol_surge,
                        'vol_surge_threshold': vol_surge_thr,
                        'use_vqs_filter': use_vqs,
                        'use_spread_filter': use_spread,
                        'max_spread_pct': max_spread
                    }
                    
                    skip_session = False
                    if htf_filter:
                        wrsi = oracle_ctx.get('weekly_rsi')
                        if wrsi:
                            if wrsi < 60 and wrsi > 40: # Weak momentum
                                st.error(f"HTF Filter: Weekly RSI is {wrsi}. Skipping session.")
                                skip_session = True
                    
                    trades = []
                    if not skip_session:
                        trades = engine.run_simulation(config)
                    
                    if not trades:
                        st.warning("No trades triggered with these parameters.")
                    else:
                        df_trades = pd.DataFrame(trades)
                        
                        # 1. Summary Metrics
                        winners = df_trades[df_trades['pnl_pct'] > 0]
                        win_rate = (len(winners) / len(df_trades)) * 100
                        total_pnl = df_trades['pnl_pct'].sum()
                        avg_duration = df_trades['duration_secs'].mean()
                        
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Total Trades", len(df_trades))
                        c2.metric("Win Rate", f"{win_rate:.1f}%")
                        c3.metric("Net PnL %", f"{total_pnl:.2f}%")
                        c4.metric("Avg Duration", f"{avg_duration:.0f}s")

                        # 2. Results Table
                        st.subheader("📜 Execution Audit")
                        df_display = df_trades.copy()
                        df_display['entry_time'] = pd.to_datetime(df_display['entry_time']).dt.strftime('%H:%M:%S.%f').str[:-3]
                        df_display['exit_time'] = pd.to_datetime(df_display['exit_time']).dt.strftime('%H:%M:%S.%f').str[:-3]
                        st.dataframe(df_display[['side', 'entry_time', 'entry_price', 'exit_time', 'exit_price', 'pnl_pct', 'exit_reason', 'duration_secs']], use_container_width=True)

                        # 3. Efficiency Chart
                        st.subheader("🚀 Efficiency Check: Speed to TP")
                        fig_dur = px.bar(df_trades[df_trades['exit_reason'] == 'TARGET'], 
                                        x='entry_time', y='duration_secs', 
                                        color='pnl_pct', 
                                        title="Time taken to hit Target 1 (seconds)")
                        st.plotly_chart(fig_dur, use_container_width=True)

                        # 4. Price Chart
                        st.subheader("💹 Signal Visualization")
                        ticks_df = engine.ticks_df
                        fig = go.Figure()
                        sample_rate = max(1, len(ticks_df) // 1000)
                        df_sampled = ticks_df.iloc[::sample_rate]
                        fig.add_trace(go.Scatter(x=df_sampled['timestamp'], y=df_sampled['ltp'], name="Price", line=dict(color='gray', width=1)))
                        for _, t in df_trades.iterrows():
                            color = 'green' if t['side'] == 'LONG' else 'red'
                            fig.add_trace(go.Scatter(x=[t['entry_time']], y=[t['entry_price']], mode='markers', marker=dict(color=color, size=12, symbol='triangle-up' if t['side'] == 'LONG' else 'triangle-down')))
                        fig.update_layout(template="plotly_dark")
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error("Failed to load data.")
        else:
            st.write("Click 'Run Backtest' to begin analysis.")

with tab2:
    st.subheader("🛰️ Real-Time Signal Stream")
    st.caption("Monitoring `orb_signals` table for today's live/simulated entries")
    
    if st.button("🔄 Refresh Live Feed"):
        if engine_db:
            with engine_db.connect() as conn:
                today = datetime.now().strftime("%Y-%m-%d")
                query = text("SELECT * FROM orb_signals WHERE DATE(date) = :d ORDER BY timestamp DESC")
                res = conn.execute(query, {"d": today}).fetchall()
                if res:
                    df_live = pd.DataFrame(res)
                    st.dataframe(df_live, use_container_width=True)
                else:
                    st.info("No live signals detected for today yet.")
        else:
            st.error("Database connection not configured.")
    else:
        st.info("Click Refresh to load latest signals from production.")
