import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime, time

# Page Configuration
st.set_page_config(page_title="Sniper ORB Strategy Lab", layout="wide", page_icon="🎯")

# Styling
st.markdown("""
    <style>
    .main { background-color: #f6f8fa; }
    .stMetric { background-color: #ffffff; padding: 20px; border-radius: 12px; box-shadow: 0 4px 10px rgba(0,0,0,0.05); border: 1px solid #d0d7de; }
    .stButton>button { background-color: #0969da; color: white; border-radius: 8px; width: 100%; }
    .css-1kyxreq { justify-content: center; }
    </style>
""", unsafe_allow_html=True)

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

# Load Data
@st.cache_data
def load_data():
    if not os.path.exists('trades_master.json'):
        return pd.DataFrame(columns=['symbol', 'time', 'side', 'range_dir', 'boundary_type', 'range_pct', 'vol_surge', 'rsi', 'macd', 'slope', 'pnl', 'outcome', 'duration'])
    
    df = pd.read_json('trades_master.json')
    # Convert 'time' to minutes for easy comparison
    if not df.empty and 'time' in df.columns:
        df['total_min'] = df['time'].apply(lambda x: int(str(x).split(':')[0])*60 + int(str(x).split(':')[1]) if ':' in str(x) else 0)
    else:
        df['total_min'] = 0
    return df

df_master = load_data()

# --- SIDEBAR FILTERS ---
st.sidebar.header("💾 Strategy Vault")
presets = load_presets()
preset_names = ["-- New / Default --"] + list(presets.keys())
selected_preset = st.sidebar.selectbox("Load Saved Strategy", options=preset_names)

# Initialize session state for filters if switching presets
if selected_preset != "-- New / Default --" and selected_preset != st.session_state.get('last_selected_preset', "-- New / Default --"):
    p = presets[selected_preset]
    st.session_state['time_range_val'] = (time.fromisoformat(p['start']), time.fromisoformat(p['end']))
    st.session_state['range_pct_val'] = tuple(p['range_pct'])
    st.session_state['vol_min_val'] = p['vol_min']
    st.session_state['vol_max_val'] = p['vol_max']
    st.session_state['vol_quality_val'] = p['vol_quality']
    st.session_state['rsi_l_min_val'] = p['rsi_l_min']
    st.session_state['rsi_l_max_val'] = p['rsi_l_max']
    st.session_state['rsi_s_min_val'] = p['rsi_s_min']
    st.session_state['rsi_s_max_val'] = p['rsi_s_max']
    st.session_state['macd_l_min_val'] = p['macd_l_min']
    st.session_state['macd_s_max_val'] = p['macd_s_max']
    st.session_state['trend_l_min_val'] = p['trend_l_min']
    st.session_state['trend_s_max_val'] = p['trend_s_max']
    st.session_state['boundary_type_val'] = p['boundary']
    st.session_state['enforce_bias_val'] = p['enforce_bias']
    st.session_state['last_selected_preset'] = selected_preset
    st.rerun()
elif selected_preset == "-- New / Default --" and st.session_state.get('last_selected_preset', "-- New / Default --") != "-- New / Default --":
    for k in [k for k in st.session_state.keys() if k.endswith('_val')]:
        del st.session_state[k]
    st.session_state['last_selected_preset'] = "-- New / Default --"
    st.rerun()

# 1. Strategy Parameters & Boundaries
st.sidebar.subheader("🎯 Strategy Selection")
boundary_type = st.sidebar.radio(
    "ORB Boundary Method",
    options=["STANDARD", "CLEAN"],
    index=["STANDARD", "CLEAN"].index(st.session_state.get('boundary_type_val', "STANDARD")),
    help="STANDARD uses absolute High/Low wicks. CLEAN uses the highest/lowest of Open or Close (Body)."
)

enforce_bias = st.sidebar.toggle(
    "Enforce Range Alignment",
    value=st.session_state.get('enforce_bias_val', False),
    help="If ON, only Longs are allowed in BULLISH ranges, and only Shorts in BEARISH ranges."
)

time_range = st.sidebar.slider(
    "Active Trading Window",
    min_value=time(9, 15),
    max_value=time(15, 30),
    value=st.session_state.get('time_range_val', (time(9, 30), time(15, 30))),
    format="HH:mm"
)
start_min = time_range[0].hour * 60 + time_range[0].minute
end_min = time_range[1].hour * 60 + time_range[1].minute

# 2. ORB Characteristics
st.sidebar.subheader("📐 ORB Range & Volume")
range_filter = st.sidebar.slider(
    "ORB Range (%)",
    min_value=0.0,
    max_value=10.0,
    value=st.session_state.get('range_pct_val', (2.0, 5.0)),
    help="Filter by the width of the 15-min range. Avoid too narrow (<0.2%) or too wide (>4%) ranges."
)
vol_min = st.sidebar.number_input("Min Volume Surge (x)", value=st.session_state.get('vol_min_val', 0.9), step=0.1)
vol_max = st.sidebar.number_input("Max Volume Surge (Avoid Spikes)", value=st.session_state.get('vol_max_val', 25.0), step=1.0)
vol_quality_min = st.sidebar.slider(
    "Min Volume Quality (VQS)",
    min_value=0.0,
    max_value=1.0,
    value=st.session_state.get('vol_quality_val', 0.15),
    help="Measures candle closing strength. >0.7 means close near extreme (Aggressive Institutional Participation). <0.4 means absorption/wick (Reverse Pressure)."
)

# 3. RSI Zones
st.sidebar.subheader("📈 RSI Thresholds")
col1, col2 = st.sidebar.columns(2)
with col1:
    rsi_l_min = st.number_input("Long Min", value=st.session_state.get('rsi_l_min_val', 60))
    rsi_l_max = st.number_input("Long Max", value=st.session_state.get('rsi_l_max_val', 100))
with col2:
    rsi_s_min = st.number_input("Short Min", value=st.session_state.get('rsi_s_min_val', 30))
    rsi_s_max = st.number_input("Short Max", value=st.session_state.get('rsi_s_max_val', 45))

# 4. Momentum & Trend
st.sidebar.subheader("⚡ Momentum & Trend")
macd_l_min = st.sidebar.slider("Long MACD Min", 0.0, 0.5, st.session_state.get('macd_l_min_val', 0.10))
macd_s_max = st.sidebar.slider("Short MACD Max", -0.5, 0.0, st.session_state.get('macd_s_max_val', 0.00))
trend_l_min = st.sidebar.number_input("Long Trend Min", value=st.session_state.get('trend_l_min_val', -0.0), step=0.01)
trend_s_max = st.sidebar.number_input("Short Trend Max", value=st.session_state.get('trend_s_max_val', -0.05), step=0.01)

# --- SAVE LOGIC ---
st.sidebar.markdown("---")
new_preset_name = st.sidebar.text_input("Name this Strategy", placeholder="e.g. Apex Aggressive")
if st.sidebar.button("💾 Save Current Strategy"):
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
            "enforce_bias": enforce_bias
        }
        save_preset(new_preset_name, current_params)
        st.sidebar.success(f"Saved: {new_preset_name}")
        st.session_state['last_selected_preset'] = new_preset_name
        st.rerun()
    else:
        st.sidebar.error("Please enter a name")

# --- SAVE LOGIC ---
st.sidebar.markdown("---")
new_preset_name = st.sidebar.text_input("Name this Strategy", placeholder="e.g. Apex Aggressive", key='new_preset_name_input')
if st.sidebar.button("💾 Save Current Strategy", key='save_strategy_button'):
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
            "enforce_bias": enforce_bias
        }
        save_preset(new_preset_name, current_params)
        st.sidebar.success(f"Saved: {new_preset_name}")
        st.session_state['last_selected_preset'] = new_preset_name # Set this as the last selected
        st.rerun()
    else:
        st.sidebar.error("Please enter a name")

# 5. Stock Selection / Blacklist
st.sidebar.subheader("🏢 Portfolio Control")
all_symbols = sorted(df_master['symbol'].unique())
selected_symbols = st.sidebar.multiselect(
    "Active Watchlist",
    options=all_symbols,
    default=all_symbols,
    help="Deselect stocks to 'Blacklist' them from the analysis."
)

# --- FILTERING LOGIC ---
mask = (
    (df_master['total_min'] >= start_min) & 
    (df_master['total_min'] <= end_min) &
    (df_master['vol_surge'] >= vol_min) &
    (df_master['vol_surge'] <= vol_max) &
    (df_master['range_pct'] >= range_filter[0]) &
    (df_master['range_pct'] <= range_filter[1]) &
    (df_master.get('vol_quality', 1.0) >= vol_quality_min) &
    (df_master['symbol'].isin(selected_symbols))
)

# Apply Side-Specific RSI/MACD/Trend
long_mask = (df_master['side'] == 'LONG') & \
            (df_master['rsi'] >= rsi_l_min) & (df_master['rsi'] <= rsi_l_max) & \
            (df_master['macd'] >= macd_l_min) & \
            (df_master['slope'] >= trend_l_min)

short_mask = (df_master['side'] == 'SHORT') & \
             (df_master['rsi'] >= rsi_s_min) & (df_master['rsi'] <= rsi_s_max) & \
             (df_master['macd'] <= macd_s_max) & \
             (df_master['slope'] <= trend_s_max)

final_df = df_master[
    mask & 
    (long_mask | short_mask) & 
    (df_master['boundary_type'] == boundary_type)
]

if enforce_bias:
    final_df = final_df[
        ((final_df['side'] == 'LONG') & (final_df['range_dir'] == 'BULLISH')) |
        ((final_df['side'] == 'SHORT') & (final_df['range_dir'] == 'BEARISH'))
    ]

# --- DASHBOARD UI ---
st.title("🎯 Expert ORB Strategy Lab")
st.markdown("Adjust the variables in the sidebar to find the **Golden Configuration** for Feb 6, 2026.")

# Key Metrics
m1, m2, m3, m4 = st.columns(4)

total_trades = len(final_df)
wins = len(final_df[final_df['outcome'] == 'TARGET'])
win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
gross_pnl = final_df['pnl'].sum() if total_trades > 0 else 0
# 0.05% brokerage/slip per trade
net_pnl = gross_pnl - (total_trades * 0.05) if total_trades > 0 else 0

with m1:
    st.metric("Total Trades", total_trades)
with m2:
    st.metric("Win Rate", f"{win_rate:.1f}%")
with m3:
    st.metric("Net Daily ROI", f"{net_pnl:.2f}%", delta=f"{net_pnl:.2f}%")
with m4:
    # Extrapolate for Salary (20 days)
    monthly_est = (200000 * (net_pnl / 100)) * 20 # Assuming 2L capital
    st.metric("Est. Monthly Salary", f"₹{monthly_est:,.0f}", help="Extrapolated over 20 trading days with 2 Lakh capital")

# Results Table
st.subheader("📋 Trade Signal Audit Log")
if not final_df.empty:
    # Color coding results
    def color_outcome(val):
        color = '#dafbe1' if val == 'TARGET' else '#ffebe9'
        return f'background-color: {color}'

    st.dataframe(
        final_df[['symbol', 'time', 'side', 'range_dir', 'boundary_type', 'range_pct', 'vol_surge', 'vol_quality', 'rsi', 'macd', 'slope', 'pnl', 'outcome', 'duration']],
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("No trades match these filters. Try relaxing the Volume or RSI constraints.")

# Strategy Intelligence
with st.expander("ℹ️ Strategy Intelligence & Analysis"):
    st.write("""
    **How to use this to 'Earn your Salary':**
    1. **Aim for Win Rate > 55%**: Your stop losses are technical (ORB boundary), so a 1.0% target with a high win rate is very stable.
    2. **Watch the 'Est. Monthly Salary'**: This assumes you take every signal that flashes 'Expert' on your scanner.
    3. **Volume is Key**: 155 signals exist, but only ~40-50 are 'True institutional moves'. 
    4. **Avoid the Afternoon**: Patterns often fail after 1:30 PM due to square-off volatility.
    """)

st.markdown("---")
st.caption("GameChanger AI: Professional Trading Engineering | Data Session: Feb 6, 2026")
