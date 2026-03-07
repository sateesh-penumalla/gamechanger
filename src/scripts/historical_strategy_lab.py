import streamlit as st
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, time, date, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Page Configuration
st.set_page_config(page_title="Historical ORB Strategy Lab", layout="wide", page_icon="🏛️")

# Styling
st.markdown("""
    <style>
    .main { background-color: #f6f8fa; }
    .stMetric { background-color: #ffffff; padding: 20px; border-radius: 12px; box-shadow: 0 4px 10px rgba(0,0,0,0.05); border: 1px solid #d0d7de; }
    .stButton>button { background-color: #0969da; color: white; border-radius: 8px; width: 100%; }
    </style>
""", unsafe_allow_html=True)

# Load Environment
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
engine = create_engine(DB_URL)

# --- CONFIG & PERSISTENCE ---
PRESET_FILE = "src/config/strategy_presets.json"
os.makedirs("src/config", exist_ok=True)

def load_presets():
    if os.path.exists(PRESET_FILE):
        with open(PRESET_FILE, "r") as f:
            return json.load(f)
    return {}

def save_preset(name, params):
    presets = load_presets()
    presets[name] = params
    with open(PRESET_FILE, "w") as f:
        json.dump(presets, f, indent=2)

@st.cache_data
def get_available_symbols():
    with engine.connect() as conn:
        res = conn.execute(text("SELECT DISTINCT symbol FROM history_testing ORDER BY symbol"))
        return [r[0] for r in res]

@st.cache_data
def get_date_range():
    with engine.connect() as conn:
        res = conn.execute(text("SELECT MIN(trade_date), MAX(trade_date) FROM history_testing"))
        return res.fetchone()

# --- SIDEBAR FILTERS ---
st.sidebar.header("🏛️ Historical Controls")

# 1. Date Range
min_d, max_d = get_date_range()
st.sidebar.subheader("📅 Backtest Period")
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(max_d - timedelta(days=30), max_d),
    min_value=min_d,
    max_value=max_d
)

# 2. Strategy Presets
st.sidebar.subheader("💾 Strategy Vault")
presets = load_presets()
preset_names = ["-- New / Default --"] + list(presets.keys())
selected_preset = st.sidebar.selectbox("Load Saved Strategy", options=preset_names)

# Initialize session state for filters
if selected_preset != "-- New / Default --" and selected_preset != st.session_state.get('hist_last_preset', ""):
    p = presets[selected_preset]
    st.session_state['hist_time_range_val'] = (time.fromisoformat(p['start']), time.fromisoformat(p['end']))
    st.session_state['hist_range_pct_val'] = tuple(p['range_pct'])
    st.session_state['hist_vol_min_val'] = p['vol_min']
    st.session_state['hist_vol_max_val'] = p['vol_max']
    st.session_state['hist_vol_quality_val'] = p['vol_quality']
    st.session_state['hist_rsi_l_min_val'] = p['rsi_l_min']
    st.session_state['hist_rsi_l_max_val'] = p['rsi_l_max']
    st.session_state['hist_rsi_s_min_val'] = p['rsi_s_min']
    st.session_state['hist_rsi_s_max_val'] = p['rsi_s_max']
    st.session_state['hist_macd_l_min_val'] = p['macd_l_min']
    st.session_state['hist_macd_s_max_val'] = p['macd_s_max']
    st.session_state['hist_trend_l_min_val'] = p['trend_l_min']
    st.session_state['hist_trend_s_max_val'] = p['trend_s_max']
    st.session_state['hist_boundary_val'] = p['boundary']
    st.session_state['hist_enforce_bias_val'] = p['enforce_bias']
    st.session_state['hist_boundary_val'] = p['boundary']
    st.session_state['hist_enforce_bias_val'] = p['enforce_bias']
    st.session_state['hist_sector_filter_val'] = p.get('enable_sector_filter', False)
    st.session_state['hist_sector_strength_val'] = p.get('min_sector_strength', 0.0)
    st.session_state['hist_min_adtv_val'] = p.get('min_adtv', 0.0)
    
    # New Oracle Settings
    st.session_state['hist_rsi_htf_l'] = p.get('rsi_htf_l', 60)
    st.session_state['hist_rsi_htf_s'] = p.get('rsi_htf_s', 40)
    st.session_state['hist_sma_l'] = p.get('sma_l', True)
    st.session_state['hist_sma_s'] = p.get('sma_s', True)
    st.session_state['hist_oracle_l'] = p.get('oracle_l', ["UP_SNIPER"])
    st.session_state['hist_oracle_s'] = p.get('oracle_s', ["DOWN_SNIPER"])
    
    st.session_state['hist_last_preset'] = selected_preset
    st.rerun()
elif selected_preset == "-- New / Default --" and st.session_state.get('hist_last_preset', "") != "-- New / Default --":
    for k in [k for k in st.session_state.keys() if k.startswith('hist_')]:
        if k != 'hist_last_preset': del st.session_state[k]
    st.session_state['hist_last_preset'] = "-- New / Default --"
    st.rerun()

# --- PARAMS UI ---
st.sidebar.subheader("🎯 Strategy Context")
boundary_type = st.sidebar.radio(
    "ORB Boundary Method",
    options=["STANDARD", "CLEAN"],
    index=["STANDARD", "CLEAN"].index(st.session_state.get('hist_boundary_val', "STANDARD"))
)

enforce_bias = st.sidebar.toggle("Enforce Range Alignment", value=st.session_state.get('hist_enforce_bias_val', False))
require_sniper = st.sidebar.toggle("Require SNIPER Status (HTF Guard)", value=st.session_state.get('hist_sniper_val', True), help="Only includes trades where the stock was in an institutional uptrend on that day (Oracle Logic).")

time_range = st.sidebar.slider(
    "Active Signal Window",
    min_value=time(9, 15),
    max_value=time(15, 20),
    value=st.session_state.get('hist_time_range_val', (time(9, 31), time(15, 20))),
    format="HH:mm"
)

st.sidebar.subheader("📐 ORB & Volume")
range_mode = st.sidebar.radio("Range Filter Mode", options=["Standard", "Clean"], help="Standard uses High/Low wicks. Clean uses Body High/Low.")
range_filter = st.sidebar.slider(
    "ORB Range (%)",
    0.0, 10.0,
    st.session_state.get('hist_range_pct_val', (0.2, 5.0))
)
vol_min = st.sidebar.number_input("Min Vol Surge (x)", value=st.session_state.get('hist_vol_min_val', 1.0), step=0.1)
vol_max = st.sidebar.number_input("Max Volume Surge (Avoid Spikes)", value=st.session_state.get('hist_vol_max_val', 25.0), step=1.0)
vol_quality_min = st.sidebar.slider("Min Vol Quality (VQS)", 0.0, 1.0, st.session_state.get('hist_vol_quality_val', 0.2))
st.sidebar.subheader("💰 Liquidity & ADTV")
min_adtv = st.sidebar.slider("Min ADTV (₹ Cr)", 0.0, 1000.0, st.session_state.get('hist_min_adtv_val', 0.0), step=10.0)

st.sidebar.subheader("📈 RSI Thresholds")
col1, col2 = st.sidebar.columns(2)
with col1:
    rsi_l_min = st.number_input("Long Min", value=st.session_state.get('hist_rsi_l_min_val', 60))
    rsi_l_max = st.number_input("Long Max", value=st.session_state.get('hist_rsi_l_max_val', 100))
with col2:
    rsi_s_min = st.number_input("Short Min", value=st.session_state.get('hist_rsi_s_min_val', 30))
    rsi_s_max = st.number_input("Short Max", value=st.session_state.get('hist_rsi_s_max_val', 45))

st.sidebar.subheader("⚡ Momentum & Trend")
macd_l_min = st.sidebar.slider("Long MACD Min", 0.0, 0.5, st.session_state.get('hist_macd_l_min_val', 0.10), step=0.01)
macd_s_max = st.sidebar.slider("Short MACD Max", -0.5, 0.0, st.session_state.get('hist_macd_s_max_val', 0.00), step=0.01)

trend_l_min = st.sidebar.number_input("Long Trend Min", value=st.session_state.get('hist_trend_l_min_val', 0.00), step=0.01)
trend_s_max = st.sidebar.number_input("Short Trend Max", value=st.session_state.get('hist_trend_s_max_val', -0.05), step=0.01)

st.sidebar.subheader("🦉 Oracle (HTF) Alignment")
with st.sidebar.expander("HTF Thresholds", expanded=False):
    rsi_range_l_val = st.slider("Weekly RSI (LONG)", 0, 100, st.session_state.get('hist_rsi_htf_l', 60), step=1)
    rsi_range_s_val = st.slider("Weekly RSI (SHORT)", 0, 100, st.session_state.get('hist_rsi_htf_s', 40), step=1)
    
    sma_align_l = st.toggle("Long: Price > Weekly SMA", value=st.session_state.get('hist_sma_l', True))
    sma_align_s = st.toggle("Short: Price < Weekly SMA", value=st.session_state.get('hist_sma_s', True))

    oracle_status_options = ["UP_SNIPER", "DOWN_SNIPER", "FILTERED", "NEUTRAL"]
    selected_oracle_l = st.multiselect(
        "Oracle Status (LONG)",
        options=oracle_status_options,
        default=st.session_state.get('hist_oracle_l', ["UP_SNIPER"]),
        help="Allowed statuses for Long trades."
    )
    selected_oracle_s = st.multiselect(
        "Oracle Status (SHORT)",
        options=oracle_status_options,
        default=st.session_state.get('hist_oracle_s', ["DOWN_SNIPER"]),
        help="Allowed statuses for Short trades."
    )

st.sidebar.subheader("📊 Sector Trend")
enable_sector_filter = st.sidebar.toggle("Sector Trend Alignment", value=st.session_state.get('hist_sector_filter_val', False), help="Only take LONGs if Sector is Green, SHORTs if Sector is Red.")
min_sector_strength = st.sidebar.slider("Min Sector Strength %", 0.0, 1.0, st.session_state.get('hist_sector_strength_val', 0.0), step=0.1)

# --- SAVE LOGIC ---
st.sidebar.markdown("---")
new_preset_name = st.sidebar.text_input("Name this Strategy", placeholder="e.g. Apex Aggressive", key='hist_new_preset_input')
if st.sidebar.button("💾 Save Current Strategy", key='hist_save_button'):
    if new_preset_name:
        current_params = {
            "start": time_range[0].isoformat(),
            "end": time_range[1].isoformat(),
            "range_pct": list(range_filter),
            "vol_min": vol_min,
            "vol_max": vol_max,
            "vol_quality": vol_quality_min,
            "rsi_l_min": rsi_l_min, "rsi_l_max": rsi_l_max,
            "rsi_s_min": rsi_s_min, "rsi_s_max": rsi_s_max,
            "macd_l_min": macd_l_min, "macd_s_max": macd_s_max,
            "trend_l_min": trend_l_min, "trend_s_max": trend_s_max,
            "boundary": boundary_type,
            "enforce_bias": enforce_bias,
            "enable_sector_filter": enable_sector_filter,
            "min_sector_strength": min_sector_strength,
            "min_adtv": min_adtv,
            "rsi_htf_l": rsi_range_l_val,
            "rsi_htf_s": rsi_range_s_val,
            "sma_l": sma_align_l,
            "sma_s": sma_align_s,
            "oracle_l": selected_oracle_l,
            "oracle_s": selected_oracle_s
        }
        save_preset(new_preset_name, current_params)
        st.sidebar.success(f"Saved: {new_preset_name}")
        st.session_state['hist_last_preset'] = new_preset_name
        st.rerun()
    else:
        st.sidebar.error("Please enter a name")

# Portfolio Selection
st.sidebar.subheader("🏢 Symbols")
all_symbols = get_available_symbols()
selected_symbols = st.sidebar.multiselect("Active Watchlist", options=all_symbols, default=all_symbols)

# --- DATA FETCHING & FILTERING ---
@st.cache_data
def fetch_and_filter_data(start_date, end_date, symbols):
    query = text("""
        SELECT e.*, t.ORB_direction, t.ORB_range_pct, t.ORB_range_pct_clean, t.weekly_rsi, t.weekly_sma, t.oracle_status, t.avg_daily_turnover
        FROM history_events e
        JOIN history_testing t ON e.symbol = t.symbol AND e.trade_date = t.trade_date
        WHERE e.trade_date BETWEEN :start AND :end
        AND e.symbol IN :symbols
        -- force cache clear 2026-02-08
    """)
    params = {
        "start": start_date,
        "end": end_date,
        "symbols": symbols if symbols else ['NONE']
    }
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)

if len(date_range) == 2:
    df_raw = fetch_and_filter_data(date_range[0], date_range[1], selected_symbols)
    
    # Apply Filters in Pandas
    start_td = timedelta(hours=time_range[0].hour, minutes=time_range[0].minute)
    end_td = timedelta(hours=time_range[1].hour, minutes=time_range[1].minute)
    
    range_col = 'ORB_range_pct' if range_mode == "Standard" else 'ORB_range_pct_clean'
    
    mask = (
        (df_raw['boundary_type'] == boundary_type) &
        (df_raw['event_time'] >= start_td) &
        (df_raw['event_time'] <= end_td) &
        (df_raw['vol_surge'] >= vol_min) &
        (df_raw['vol_surge'] <= vol_max) &
        (df_raw['vol_quality'] >= vol_quality_min) &
        (df_raw[range_col] >= range_filter[0]) &
        (df_raw[range_col] <= range_filter[1]) &
        (df_raw['avg_daily_turnover'].fillna(0) >= min_adtv)
    )
    
    # Side Specific Filters
    long_mask = (df_raw['event_type'] == 'BREAKOUT') & \
                (df_raw['rsi'] >= rsi_l_min) & (df_raw['rsi'] <= rsi_l_max) & \
                (df_raw['macd'] >= macd_l_min) & \
                (df_raw['slope'] >= trend_l_min)
                
    short_mask = (df_raw['event_type'] == 'BREAKDOWN') & \
                 (df_raw['rsi'] >= rsi_s_min) & (df_raw['rsi'] <= rsi_s_max) & \
                 (df_raw['macd'] <= macd_s_max) & \
                 (df_raw['slope'] <= trend_s_max)
    
    final_df = df_raw[mask & (long_mask | short_mask)].copy()
    
    if enforce_bias:
        final_df = final_df[
            ((final_df['event_type'] == 'BREAKOUT') & (final_df['ORB_direction'] == 'BULLISH')) |
            ((final_df['event_type'] == 'BREAKDOWN') & (final_df['ORB_direction'] == 'BEARISH'))
        ]

    # --- DYNAMIC ORACLE STATUS CALCULATION ---
    def calc_oracle_status(row):
        # HTF Alignment Logic
        rsi = row['weekly_rsi']
        sma = row['weekly_sma']
        entry = row['entry']
        side = row['event_type']
        
        if pd.isna(rsi) or pd.isna(sma):
            return "NEUTRAL"
            
        if side == 'BREAKOUT':
            # Check for Long alignment
            matches_rsi = rsi >= rsi_range_l_val
            matches_sma = entry >= sma if sma_align_l else True
            if matches_rsi and matches_sma:
                return "UP_SNIPER"
        elif side == 'BREAKDOWN':
            # Check for Short alignment
            matches_rsi = rsi <= rsi_range_s_val
            matches_sma = entry <= sma if sma_align_s else True
            if matches_rsi and matches_sma:
                return "DOWN_SNIPER"
        
        return "FILTERED"

    if not final_df.empty:
        final_df.loc[:, 'oracle_status_calc'] = final_df.apply(calc_oracle_status, axis=1)
        
        # Apply Side-Specific Oracle Filter
        long_oracle_mask = (final_df['event_type'] == 'BREAKOUT') & (final_df['oracle_status_calc'].isin(selected_oracle_l))
        short_oracle_mask = (final_df['event_type'] == 'BREAKDOWN') & (final_df['oracle_status_calc'].isin(selected_oracle_s))
        
        final_df = final_df[long_oracle_mask | short_oracle_mask]

    if enable_sector_filter:
        # Filter based on Sector Direction and Strength
        # sector_change_at_entry is in % (e.g., 0.5, -1.2)
        # We handle NaN by treating it as neutral (or filtering it out? let's keep it safe by filtering out if unknown)
        final_df = final_df[final_df['sector_change_at_entry'].notna()]
        
        final_df = final_df[
            ((final_df['event_type'] == 'BREAKOUT') & (final_df['sector_change_at_entry'] > min_sector_strength)) |
            ((final_df['event_type'] == 'BREAKDOWN') & (final_df['sector_change_at_entry'] < -min_sector_strength))
        ]

    # --- DASHBOARD UI ---
    st.title("🎯 Historical Strategy Lab")
    st.markdown(f"Backtesting results from **{date_range[0]}** to **{date_range[1]}** across **{len(selected_symbols)}** symbols.")

    m1, m2, m3, m4 = st.columns(4)
    total_trades = len(final_df)
    wins = len(final_df[final_df['outcome'] == 'TARGET'])
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    gross_pnl = final_df['pnl'].sum() if total_trades > 0 else 0
    
    # Unique days in the selection
    trading_days = len(final_df['trade_date'].unique()) if not final_df.empty else 1
    net_pnl = gross_pnl - (total_trades * 0.05) # 0.05% brokerage/slip
    daily_avg_pnl = net_pnl / trading_days if trading_days > 0 else 0

    with m1:
        st.metric("Total Trades", total_trades)
    with m2:
        st.metric("Aggregate Win Rate", f"{win_rate:.1f}%")
    with m3:
        st.metric("Net Total ROI", f"{net_pnl:.2f}%")
    with m4:
        monthly_est = (200000 * (daily_avg_pnl / 100)) * 20
        st.metric("Daily Avg PnL", f"{daily_avg_pnl:.2f}%", help=f"Estimated Monthly (20 days): ₹{monthly_est:,.0f}")

    # Detailed Table
    st.subheader("📋 Historical Signal Audit")
    if not final_df.empty:
        # Sort by date and time
        final_df = final_df.sort_values(['trade_date', 'event_time'], ascending=[False, True])
        
        display_cols = [
            'trade_date', 'symbol', 'event_time', 'exit_time', 'event_type', 
            'oracle_status_calc', 'weekly_rsi', 'weekly_sma', 'avg_daily_turnover',
            'ORB_direction', range_col, 'vol_surge', 'vol_quality', 
            'rsi', 'sector_change_at_entry', 'macd', 'sl', 'entry', 'pnl', 'outcome', 'duration'
        ]
        # Rename for clarity
        display_df = final_df[display_cols].rename(columns={
            'event_time': 'Entry Time',
            'exit_time': 'Exit Time',
            'event_type': 'Side',
            'sector_change_at_entry': 'Sector %',
            'sl': 'Stop Loss',
            'pnl': 'PnL %'
        })
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("No trades found matching these constraints. Try broadening your criteria.")

else:
    st.warning("Please select a valid start and end date in the sidebar.")

st.markdown("---")
st.caption("GameChanger AI: Historical Strategy Intelligence | Rooted in Expert ORB Logic")
