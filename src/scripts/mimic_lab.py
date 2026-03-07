import streamlit as st
import pandas as pd
import json
import os
import sys
import importlib
import plotly.express as px
from sqlalchemy import create_engine, text
from datetime import datetime, time
from dotenv import load_dotenv

# Add root directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import src.scripts.mimic_engine as mimic_engine

# Page Configuration
st.set_page_config(page_title="Live Mimic Trading Lab", layout="wide", page_icon="🤖")

# Custom CSS for Premium Look
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }
    .trade-card { border-left: 5px solid #007bff; padding: 10px; margin: 10px 0; background: white; border-radius: 5px; }
    
    /* Enhance Dataframe Header visibility */
    [data-testid="stTable"] thead tr th, [data-testid="stDataFrame"] thead tr th {
        background-color: #343a40 !important;
        color: white !important;
        font-weight: 900 !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- UTILS ---
PRESET_FILE = "src/config/strategy_presets.json"

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
engine = create_engine(DB_URL)

def load_presets():
    if os.path.exists(PRESET_FILE):
        with open(PRESET_FILE, "r") as f:
            return json.load(f)
    return {}

@st.cache_data
def get_available_symbols(mode='REPLAY', sniper_only=False, min_turnover=0):
    with engine.connect() as conn:
        if mode == 'LIVE':
            query = "SELECT symbol FROM tickers"
            conditions = []
            if sniper_only:
                conditions.append("oracle_status IN ('UP_SNIPER', 'DOWN_SNIPER')")
            if min_turnover > 0:
                conditions.append(f"COALESCE(avg_daily_turnover, 0.0) >= {float(min_turnover)}")
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY symbol"
            
            # Debugging
            print(f"DEBUG SQL: {query}")
            result = [row[0] for row in conn.execute(text(query))]
            if not result:
                print("DEBUG: Filter returned 0. Fallback to all tickers.")
                return [row[0] for row in conn.execute(text("SELECT symbol FROM tickers ORDER BY symbol"))]
            return result
        else:
            query = "SELECT DISTINCT symbol FROM history_testing ORDER BY symbol"
            return [row[0] for row in conn.execute(text(query))]
            
        res = conn.execute(text(query))
        return [r[0] for r in res]

def play_notification_sound():
    """Injects a hidden HTML5 audio tag to play a notification chime."""
    sound_url = "https://www.soundjay.com/misc/sounds/magic-chime-01.mp3"
    st.markdown(f"""
        <audio id="notif-sound" autoplay>
            <source src="{sound_url}" type="audio/mpeg">
        </audio>
        """, unsafe_allow_html=True)

# --- SIDEBAR: MISSION CONTROL ---
st.sidebar.header("🚀 Mimic Mission Control")

# 1. Select Base Strategy
presets = load_presets()
selected_strategy = st.sidebar.selectbox("Signal Strategy (Preset)", options=list(presets.keys()), index=0 if presets else None)

if not selected_strategy:
    st.warning("Please create a strategy preset in the Strategy Lab first.")
    st.stop()

# 2. Simulation Mode
st.sidebar.subheader("🔬 Simulation Mode")
sim_mode = st.sidebar.radio(
    "Simulation Logic", 
    options=["LIVE", "REPLAY"], 
    index=0, 
    help="LIVE: Real-time detection logic on recent cache. REPLAY: Minute-by-minute replay using historical archive."
)

use_api = False
live_monitor = False
if sim_mode == "LIVE":
    use_api = st.sidebar.toggle("Fetch from Dhan API", value=True, help="If ON, fetches the latest 1m ticks directly from Dhan. If OFF, uses local sniper_cache.")
    live_monitor = st.sidebar.toggle("Live Monitor Mode", value=True, help="If ON, automatically refreshes and monitors for new signals every 60 seconds.")

trade_date = None
if sim_mode == "REPLAY":
    trade_date = st.sidebar.date_input("Historical Date", value=datetime(2026, 2, 6), min_value=datetime(2025, 8, 11), max_value=datetime(2026, 2, 6))

st.sidebar.subheader("🏢 Symbols")
sniper_only = st.sidebar.toggle("Oracle Snipers Only", value=True, help="Oracle Trend Guard: Only shows stocks with confirmed institutional bias (UP_SNIPER or DOWN_SNIPER).")

# Granular Sniper Controls
long_snipers = st.sidebar.multiselect("Long Side Snipers", options=["UP_SNIPER", "DOWN_SNIPER"], default=["UP_SNIPER"], help="Institutional biases allowed for LONG trades.")
short_snipers = st.sidebar.multiselect("Short Side Snipers", options=["UP_SNIPER", "DOWN_SNIPER"], default=["DOWN_SNIPER"], help="Institutional biases allowed for SHORT trades.")

# Liquidity Guard
min_adtv = st.sidebar.slider("Min Avg Daily Turnover (₹ Cr)", 1, 1000, 50, step=1, help="Filters out stocks with average daily turnover below this threshold (calculated over 2 weeks).")

all_symbols = get_available_symbols(mode=sim_mode, sniper_only=sniper_only, min_turnover=min_adtv)
selected_symbols = st.sidebar.multiselect("Active Watchlist", options=all_symbols, default=all_symbols)

st.sidebar.subheader("📐 ORB Strategy")
orb_duration = st.sidebar.select_slider(
    "ORB Window Duration",
    options=[15, 30, 45, 60],
    value=15,
    help="Time window for boundary calculation starting from 09:15. Standard is 15 min (09:30)."
)
range_filter_mode = st.sidebar.radio("Range Filter Mode", options=["STANDARD", "CLEAN"], index=0, help="Determine which boundaries (Standard/Clean) are used for the range percentage filter.")
execution_boundary = st.sidebar.radio("Execution Boundary", options=["STANDARD", "CLEAN"], index=0, help="Determine which boundaries are used for entry detection and stop loss calculations.")

st.sidebar.subheader("🏛️ Institutional HTF Filters")
ignore_htf = st.sidebar.toggle("Bypass HTF Momentum Filters", value=False, help="Debug Mode: If ON, ignores Weekly RSI/SMA filters to see all strategy-compliant trades.")
if ignore_htf:
    st.sidebar.warning("⚠️ HTF Bypass is active. Expect more trades and potentially lower win rates.")
with st.sidebar.expander("HTF Diagnostic Ranges", expanded=False):
    rsi_range_l = st.slider("Weekly RSI (LONG)", 0, 100, (60, 100), step=1)
    rsi_range_s = st.slider("Weekly RSI (SHORT)", 0, 100, (0, 40), step=1)
    
    sma_align_l = st.toggle("Long: Price > Weekly SMA", value=True)
    sma_align_s = st.toggle("Short: Price < Weekly SMA", value=True)

st.sidebar.subheader("🌐 Macro Confirmation")
use_market_filter = st.sidebar.toggle("Market Sentiment Guard", value=True, help="Long only if Nifty/BankNifty are Bullish; Short only if Bearish.")
use_sector_filter = st.sidebar.toggle("Sector Alignment Guard", value=True, help="Ensures the stock's sector index is supportive (Price > VWAP for Long).")
use_news_filter = st.sidebar.toggle("AI News Sentiment Guard", value=False, help="Uses Gemini to analyze real-time news headlines. Vetoes if sentiment is contrary.")


# 3. Trade Management Parameters
st.sidebar.subheader("🛡️ Risk Management")
sl_type = st.sidebar.radio("Stop Loss Method", options=["FIXED_PCT", "ORB_BOUNDARY"], index=1, help="FIXED_PCT uses the slider below. ORB_BOUNDARY uses the opposite extreme of the opening range (Low for Long, High for Short).")
sl_pct = 0.5
if sl_type == "FIXED_PCT":
    sl_pct = st.sidebar.slider("Hard Stop Loss (%)", 0.1, 5.0, 0.5, step=0.1)

st.sidebar.subheader("🎯 Exit Strategy")
exit_method = st.sidebar.radio("Exit Type", options=["FIXED_TARGET", "TREND_RIDER"], index=1, help="FIXED_TARGET exits exactly at the milestone. TREND_RIDER activates Trailing SL only AFTER the milestone is hit.")
tp_milestone = st.sidebar.slider("Profit Milestone (%)", 0.1, 10.0, 1.0, step=0.1)

st.sidebar.subheader("📈 Trailing Stop Loss")
enable_tsl = st.sidebar.toggle("Enable Trailing SL", value=True)
if enable_tsl:
    if exit_method == "TREND_RIDER":
        st.sidebar.info(f"TSL will activate after {tp_milestone}% profit.")
    tsl_step = st.sidebar.slider("TSL Trailing Step (%)", 0.1, 2.0, 0.2, step=0.1)
else:
    tsl_step = 0.0

st.sidebar.subheader("🏢 Portfolio Control")
single_trade = st.sidebar.toggle("One Trade Per Stock", value=True, help="If ON, engine stops looking for signals for a stock after its first executed trade.")
enable_audio = st.sidebar.toggle("🔊 Enable Audio Alerts", value=True, help="If ON, plays a notification chime when a new trading signal is detected.")

st.sidebar.markdown("---")
# Launch Button
launch_mimic = st.sidebar.button("⚡ Launch Mimic Session", use_container_width=True)

# --- MAIN DASHBOARD ---
# Handle Live Monitor sessions
if 'monitoring' not in st.session_state:
    st.session_state.monitoring = False
if 'memoized_watchlist' not in st.session_state:
    st.session_state.memoized_watchlist = None
if 'last_signal_count' not in st.session_state:
    st.session_state.last_signal_count = 0

# Reset Memoization if inputs change
current_preset_key = f"{selected_strategy}_{orb_duration}_{range_filter_mode}"
if 'last_preset_key' not in st.session_state or st.session_state.last_preset_key != current_preset_key:
    st.session_state.memoized_watchlist = None
    st.session_state.last_preset_key = current_preset_key

if launch_mimic:
    st.session_state.monitoring = live_monitor
    st.session_state.memoized_watchlist = None # Force re-scan on manual launch

if launch_mimic or st.session_state.monitoring:
    # Use memoized watchlist if available
    active_symbols = selected_symbols
    if st.session_state.memoized_watchlist:
        active_symbols = st.session_state.memoized_watchlist
        st.info(f"Using Memoized Watchlist: {len(active_symbols)} symbols qualified for strategy.")
        if st.button("🔄 Reset & Re-scan Full List"):
            st.session_state.memoized_watchlist = None
            st.rerun()
    risk_params = {
        "tp_pct": tp_milestone,
        "sl_pct": sl_pct,
        "sl_type": sl_type,
        "exit_method": exit_method,
        "enable_tsl": enable_tsl,
        "tsl_step": tsl_step,
        "single_trade": single_trade,
        "range_filter_mode": range_filter_mode,
        "execution_boundary": execution_boundary,
        "orb_duration": orb_duration,
        "ignore_htf": ignore_htf,
        "htf": {
            "rsi_long": rsi_range_l,
            "rsi_short": rsi_range_s,
            "sma_align_l": sma_align_l,
            "sma_align_s": sma_align_s
        },
        "use_market_filter": use_market_filter,
        "use_sector_filter": use_sector_filter,
        "use_news_filter": use_news_filter,
        "long_sniper_allowance": long_snipers,
        "short_sniper_allowance": short_snipers,
        "min_adtv": min_adtv
    }
    
    with st.spinner("Executing High-Fidelity Market Simulation..."):
        # Force reload of engine to pick up parity fixes
        import src.agents.globalist as globalist_mod
        import src.agents.sector_general as sector_mod
        import src.agents.newsroom as newsroom_mod
        importlib.reload(globalist_mod)
        importlib.reload(sector_mod)
        importlib.reload(newsroom_mod)
        importlib.reload(mimic_engine)
        
        # Blacklist logic (PFOCUS is currently unstable)
        blacklist = ['PFOCUS'] 
        
        df_results, df_skipped, df_orb_summary = mimic_engine.run_mimic_session(
            preset_name=selected_strategy,
            risk_params=risk_params,
            blacklist=blacklist,
            whitelist=active_symbols,
            mode=sim_mode,
            trade_date=trade_date,
            require_sniper=sniper_only,
            use_api=use_api,
            orb_duration=orb_duration,
            ignore_htf=ignore_htf,
            range_filter_mode=range_filter_mode,
            execution_boundary=execution_boundary
        )
        
        # --- AUDIO ALERT LOGIC ---
        current_signal_count = len(df_results)
        if enable_audio and current_signal_count > st.session_state.last_signal_count:
            play_notification_sound()
            st.toast(f"🔔 NEW SIGNAL: {df_results.iloc[-1]['symbol']} ({df_results.iloc[-1]['side']})", icon="📈")
        
        st.session_state.last_signal_count = current_signal_count

        # --- MEMOIZATION LOGIC ---
        if not df_orb_summary.empty and st.session_state.memoized_watchlist is None:
            # Extract symbols that met range requirements from presets
            p = presets[selected_strategy]
            r_min, r_max = p.get('range_pct', [0, 100])
            
            # Use 'Standard' or 'Clean' based on filter mode
            range_col = 'range_pct_clean' if range_filter_mode == 'CLEAN' else 'range_pct'
            
            qualified = df_orb_summary[
                (df_orb_summary[range_col] >= r_min) & 
                (df_orb_summary[range_col] <= r_max)
            ]['symbol'].tolist()
            
            if qualified:
                st.session_state.memoized_watchlist = qualified
                st.success(f"Watchlist Memoized: {len(qualified)} stocks met '{selected_strategy}' volatility criteria.")
        
        if df_results.empty:
            st.warning("No trades triggered with current filters. Try relaxing the RSI or Volume Surge requirements in Strategy Lab.")
        else:
            # 1. METRICS
            total_trades = len(df_results)
            winners = len(df_results[df_results['pnl'] > 0])
            win_rate = (winners / total_trades) * 100
            total_pnl = df_results['pnl'].sum()
            avg_pnl = df_results['pnl'].mean()
            
            # Simulated ROI on 1L Capital (assuming 10k per trade on 10 stocks max)
            net_roi = total_pnl * 0.1 # Simplistic estimate
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Trades", total_trades)
            c2.metric("Win Rate", f"{win_rate:.1f}%")
            c3.metric("Net Day PnL (%)", f"{total_pnl:.2f}%")
            c4.metric("Est. Daily Income", f"₹{net_roi * 1000:,.0f}")
            
            
            # 3. EQUITY CURVE (PnL Timeline)
            st.subheader("📈 Session Equity Curve")
            df_results = df_results.sort_values('exit_ts')
            df_results['cum_pnl'] = df_results['pnl'].cumsum()
            
            fig = px.line(df_results, x='exit_ts', y='cum_pnl', 
                         title="Cumulative PnL (%) over Time",
                         labels={"exit_ts": "Time", "cum_pnl": "Cumulative PnL (%)"},
                         markers=True)
            st.plotly_chart(fig, use_container_width=True)
            
            # --- PREMIUM TABLE RENDERER ---
            def get_audit_table_html(df_subset, table_id, side_label):
                if df_subset.empty:
                    return f"<div style='padding: 20px; color: #666; font-family: sans-serif;'>No trades in this category (Side: {side_label}).</div>"

                audit_cols = ['symbol', 'side', 'oracle_status', 'adtv_cr', 'entry_ts', 'entry_price', 'exit_ts', 'exit_price', 'current_price', 'points', 'pnl', 'reason', 
                              'mkt_entry', 'mkt_live', 'sec_entry', 'sec_live', 'news_score', 'news_summary',
                              'direction', 'rsi', 'macd', 'vol_surge', 
                              'slope', 'vqs', 'weekly_rsi', 'weekly_sma', 'boundary', 'range_pct', 'range_pct_clean', 
                              'orb_h_std', 'orb_l_std', 'orb_h_cln', 'orb_l_cln']
                
                df_rendered = df_subset.reindex(columns=audit_cols).reset_index(drop=True)
                
                # Add Status Column
                status_icon = df_rendered['pnl'].apply(lambda x: "🟢" if float(x) > 0 else ("🔴" if float(x) < 0 else "⚪"))
                df_rendered.insert(0, 'st', status_icon)
                df_rendered['symbol_end'] = df_rendered['symbol']
                
                if 'entry_ts' in df_rendered.columns:
                    df_rendered['entry_ts'] = pd.to_datetime(df_rendered['entry_ts']).dt.strftime('%H:%M')
                if 'exit_ts' in df_rendered.columns:
                    df_rendered['exit_ts'] = pd.to_datetime(df_rendered['exit_ts']).dt.strftime('%H:%M')

                for col in df_rendered.select_dtypes(include=['float64', 'float32']).columns:
                    df_rendered[col] = df_rendered[col].map(lambda x: f"{float(x):.2f}" if pd.notnull(x) else "-")

                def get_row_style(pnl_val, idx):
                    try: pnl = float(pnl_val)
                    except: pnl = 0
                    base_color = "#f8f9fa" if idx % 2 == 0 else "#ffffff"
                    if pnl > 0: return "background-color: #e6ffed;"
                    if pnl < 0: return "background-color: #fff5f5;"
                    return f"background-color: {base_color};"

                html = f"""
                <h4 style="font-family: sans-serif; margin-bottom: 10px; color: #333;">📜 Execution Audit: {side_label} Trades</h4>
                <div style="overflow-x: auto; overflow-y: auto; max-height: 400px; border: 1px solid #ddd; border-radius: 8px; margin-bottom: 30px;">
                    <table id="{table_id}" style="width: 100%; border-collapse: collapse; font-family: sans-serif; font-size: 13px;">
                        <thead style="position: sticky; top: 0; background-color: #212529; color: white; z-index: 10;">
                            <tr>
                """
                
                display_names = {
                    "st": "st", "entry_ts": "Entry", "exit_ts": "Exit", "pnl": "PnL (%)", "current_price": "Live Price", "symbol_end": "Symbol",
                    "mkt_entry": "Mkt (Entry)", "mkt_live": "Mkt (Live)",
                    "sec_entry": "Sec (Entry)", "sec_live": "Sec (Live)",
                    "news_score": "News AI", "news_summary": "News Summary",
                    "adtv_cr": "ADTV (Cr)"
                }
                for i, col in enumerate(df_rendered.columns):
                    name = display_names.get(col, col.replace('_', ' ').title())
                    html += f'<th onclick="sortTable(\'{table_id}\', {i})" style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6; color: white; white-space: nowrap; cursor: pointer; user-select: none;">{name} <small>↕</small></th>'
                
                html += "</tr></thead><tbody>"
                
                for idx, row in df_rendered.iterrows():
                    real_pnl = float(df_subset.iloc[idx]['pnl'])
                    row_style = get_row_style(real_pnl, idx)
                    html += f'<tr style="{row_style}">'
                    for col in df_rendered.columns:
                        val = row[col]
                        cell_style = "padding: 8px; border-bottom: 1px solid #dee2e6; white-space: nowrap;"
                        if col in ['symbol', 'symbol_end', 'reason', 'st']: cell_style += " font-weight: bold;"
                        
                        # Clickable Symbol check
                        if col in ['symbol', 'symbol_end']:
                            clean_sym = str(val).split('.')[0].split(':')[0] # Handle .NS or :NSE
                            link = f"https://www.google.com/finance/quote/{clean_sym}:NSE"
                            val = f'<a href="{link}" target="_blank" style="color: inherit; text-decoration: underline; cursor: pointer;">{val}</a>'

                        if col == 'oracle_status':
                            if val == 'UP_SNIPER': bg_color = "#28a745"
                            elif val == 'DOWN_SNIPER': bg_color = "#dc3545"
                            elif val == 'LOW_LIQUIDITY': bg_color = "#fd7e14" # Orange for liquidity warning
                            else: bg_color = "#6c757d"
                            val = f'<span style="background-color: {bg_color}; color: white; padding: 2px 6px; border-radius: 4px; font-size: 11px;">{val}</span>'

                        if col == 'pnl':
                            color = "#28a745" if real_pnl > 0 else ("#dc3545" if real_pnl < 0 else "#000")
                            cell_style += f" color: {color}; font-weight: bold;"
                        if col == 'current_price' and row['reason'] == 'OPEN':
                            cell_style += " color: #007bff; font-weight: bold;"
                        html += f'<td style="{cell_style}">{val}</td>'
                    html += "</tr>"
                
                html += "</tbody></table></div>"
                return html

            # Consolidate complete HTML payload
            sorting_js = """
            <script>
            function sortTable(tableId, n) {
              var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
              table = document.getElementById(tableId);
              if (!table) return;
              switching = true;
              dir = "asc";
              while (switching) {
                switching = false;
                rows = table.rows;
                for (i = 1; i < (rows.length - 1); i++) {
                  shouldSwitch = false;
                  x = rows[i].getElementsByTagName("TD")[n];
                  y = rows[i + 1].getElementsByTagName("TD")[n];
                  
                  let xVal = x.innerText.replace(/[🟢🔴⚪₹%]/g, '').trim();
                  let yVal = y.innerText.replace(/[🟢🔴⚪₹%]/g, '').trim();
                  
                  let xNum = parseFloat(xVal);
                  let yNum = parseFloat(yVal);
                  
                  if (!isNaN(xNum) && !isNaN(yNum)) {
                    if (dir == "asc") {
                      if (xNum > yNum) { shouldSwitch = true; break; }
                    } else if (dir == "desc") {
                      if (xNum < yNum) { shouldSwitch = true; break; }
                    }
                  } else {
                    if (dir == "asc") {
                      if (xVal.toLowerCase() > yVal.toLowerCase()) { shouldSwitch = true; break; }
                    } else if (dir == "desc") {
                      if (xVal.toLowerCase() < yVal.toLowerCase()) { shouldSwitch = true; break; }
                    }
                  }
                }
                if (shouldSwitch) {
                  rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                  switching = true;
                  switchcount ++;
                } else {
                  if (switchcount == 0 && dir == "asc") {
                    dir = "desc";
                    switching = true;
                  }
                }
              }
            }
            </script>
            """
            
            df_long = df_results[df_results['side'] == 'LONG']
            df_short = df_results[df_results['side'] == 'SHORT']
            
            audit_html = sorting_js + get_audit_table_html(df_long, "long_audit_table", "LONG")
            audit_html += get_audit_table_html(df_short, "short_audit_table", "SHORT")
            
            import streamlit.components.v1 as components
            components.html(audit_html, height=800, scrolling=True)
            
            # 4. SIGNAL DIAGNOSTIC (The "Why no trades?" section)
            if not df_skipped.empty:
                with st.expander("🔍 Signal Diagnostic Audit (Filtered Out Signals)"):
                    st.write("These stocks broke their ORB, but were rejected by your current Strategy Filters:")
                    st.dataframe(
                        df_skipped,
                        use_container_width=True,
                        hide_index=True
                    )
            
            # 5. OPENING RANGE SUMMARY (Bottom of Page)
            st.divider()
            st.subheader("🎯 Opening Range Summary (All Scanned Stocks)")
            if not df_orb_summary.empty:
                st.dataframe(
                    df_orb_summary[['symbol', 'orb_h_std', 'orb_l_std', 'orb_h_cln', 'orb_l_cln', 'range_pct', 'range_pct_clean', 'direction']],
                    use_container_width=True,
                    hide_index=True
                )
            
            st.success("Mimic Simulation Complete.")
            
            if st.session_state.monitoring:
                import time
                st.info("Live Monitoring Active: Refreshing in 60 seconds...")
                time.sleep(60)
                st.rerun()
else:
    st.write("Configure your risk parameters in the sidebar and click **Launch Mimic Session** to see how your strategy manages live trades.")

    # Show active preset details for reference
    st.sidebar.markdown("---")
    st.sidebar.subheader("📖 Strategy Preview")
    st.sidebar.json(presets[selected_strategy])
