import os
import sys
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# --- CONFIGURABLE GOLDEN GUARD FILTERS ---
CONFIG = {
    "TIME": {"START": "09:40", "END": "11:30"},
    "VOL": {"MIN": 2.0, "MAX": 25.0},
    "LONG": {
        "RSI": (45, 70),
        "MACD": (0.05, 0.25),
        "TREND": 0.00
    },
    "SHORT": {
        "RSI": (30, 55),
        "MACD": (-0.25, -0.05),
        "TREND": -0.05
    }
}

def is_expert_trade(e, mode):
    """Filters events based on the Golden Guard criteria."""
    t = e['time']
    v = e.get('vol_surge', 0)
    r = e.get('rsi', 50)
    m = e.get('macd', 0)
    s = e.get('slope', 0)
    
    # 1. Time Check
    if not (CONFIG["TIME"]["START"] <= t <= CONFIG["TIME"]["END"]): return False
    
    # 2. Volume Check
    if not (CONFIG["VOL"]["MIN"] <= v <= CONFIG["VOL"]["MAX"]): return False
    
    # 3. Strategy Specifics
    if mode == 'bo':
        if not (CONFIG["LONG"]["RSI"][0] <= r <= CONFIG["LONG"]["RSI"][1]): return False
        if not (CONFIG["LONG"]["MACD"][0] <= m <= CONFIG["LONG"]["MACD"][1]): return False
        if s < CONFIG["LONG"]["TREND"]: return False
    else:
        if not (CONFIG["SHORT"]["RSI"][0] <= r <= CONFIG["SHORT"]["RSI"][1]): return False
        if not (CONFIG["SHORT"]["MACD"][0] <= m <= CONFIG["SHORT"]["MACD"][1]): return False
        if s > CONFIG["SHORT"]["TREND"]: return False
        
    return True

def format_events(events_json):
    if not events_json: return "-"
    events = events_json if isinstance(events_json, list) else json.loads(events_json)
    if not events: return "-"
    
    formatted = []
    for e in events:
        p = e.get('pnl', 0)
        o = e.get('outcome', '-')
        formatted.append(f"**{e['time']}** ({p}%, {o})")
    return ", ".join(formatted)

def generate_report():
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))
    
    query = text("""
        SELECT symbol, ORB_high, ORB_low, breakout_events, breakdown_events 
        FROM tickers WHERE oracle_status IN ('UP_SNIPER', 'DOWN_SNIPER') ORDER BY symbol ASC
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
    
    # --- METRICS CALCULATION ---
    filtered_trades = []
    for row in rows:
        bo = json.loads(row[3]) if row[3] else []
        bd = json.loads(row[4]) if row[4] else []
        for e in bo:
            if is_expert_trade(e, 'bo'): filtered_trades.append(e)
        for e in bd:
            if is_expert_trade(e, 'bd'): filtered_trades.append(e)
            
    total_f = len(filtered_trades)
    wins_f = len([t for t in filtered_trades if t['outcome'] == 'TARGET'])
    win_rate = (wins_f / total_f * 100) if total_f > 0 else 0
    gross_pnl = sum([t['pnl'] for t in filtered_trades])
    net_pnl = gross_pnl - (total_f * 0.05) # Est taxes/slippage
    
    # --- 1. Markdown ---
    md_content = f"# 💎 Sniper ORB Expert ROI Dashboard (Feb 6, 2026)\n\n"
    md_content += f"## 🚀 Golden Filter Results\n"
    md_content += f"- **Filtered Trades**: {total_f}\n"
    md_content += f"- **Win Rate**: {win_rate:.1f}%\n"
    md_content += f"- **Estimated Net ROI**: {net_pnl:.2f}%\n\n"
    md_content += "| Symbol | Breakout Performance | Breakdown Performance |\n"
    md_content += "| :--- | :--- | :--- |\n"
    
    # --- 2. HTML (Light Mode Premium) ---
    html_content = f"""
    <html>
    <head>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
        <style>
            body {{ font-family: 'Inter', sans-serif; background-color: #f6f8fa; color: #1f2328; padding: 40px; }}
            .dashboard-card {{ background: #ffffff; border-radius: 16px; padding: 30px; box-shadow: 0 8px 30px rgba(0,0,0,0.06); margin-bottom: 40px; border: 1px solid #d0d7de; display: flex; justify-content: space-around; align-items: center; border-top: 6px solid #0969da; }}
            .stat-box {{ text-align: center; }}
            .stat-val {{ font-size: 2.2em; font-weight: 600; color: #0969da; display: block; }}
            .stat-label {{ color: #636c76; text-transform: uppercase; letter-spacing: 1px; font-size: 0.8em; font-weight: bold; }}
            .pnl-plus {{ color: #1a7f37; }}
            .pnl-minus {{ color: #cf222e; }}
            
            table {{ width: 100%; border-collapse: collapse; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 15px rgba(0,0,0,0.04); border: 1px solid #d0d7de; }}
            th {{ background: #f6f8fa; color: #0969da; padding: 18px; text-align: left; border-bottom: 2px solid #d0d7de; font-weight: 600; text-transform: uppercase; font-size: 0.85em; }}
            td {{ padding: 18px; border-bottom: 1px solid #d0d7de; vertical-align: top; }}
            tr:hover {{ background: #f9fafb; }}
            
            .event-card {{ background: #f8f9fa; border-radius: 10px; padding: 12px 16px; margin-bottom: 15px; border: 1px solid #d0d7de; position: relative; }}
            .event-card.expert {{ border: 2px solid #0969da; background: #f0f7ff; box-shadow: 0 4px 12px rgba(9, 105, 218, 0.1); }}
            .expert-badge {{ position: absolute; top: 10px; right: 10px; background: #0969da; color: #fff; font-size: 0.65em; padding: 2px 6px; border-radius: 4px; text-transform: uppercase; font-weight: bold; }}
            
            .time {{ font-weight: bold; color: #1f2328; display: block; margin-bottom: 6px; font-size: 1.1em; }}
            .metrics {{ display: flex; flex-wrap: wrap; gap: 12px; font-size: 0.8em; margin-bottom: 10px; }}
            .metric {{ color: #636c76; }}
            .perf-line {{ display: flex; align-items: center; gap: 10px; border-top: 1px solid #e1e4e8; padding-top: 8px; margin-top: 8px; }}
            .outcome {{ font-weight: bold; padding: 2px 8px; border-radius: 6px; font-size: 0.8em; }}
            .target {{ background: #dafbe1; color: #1a7f37; }}
            .sl {{ background: #ffebe9; color: #cf222e; }}
            .sq {{ background: #fff8c5; color: #9a6700; }}
        </style>
    </head>
    <body>
        <h1 style="text-align: center; margin-bottom: 10px;">🛡️ Sniper ORB Confidence Dashboard</h1>
        <p style="text-align: center; color: #636c76; margin-bottom: 30px;">Applied Filters: Vol ≥ 2.0x | Time 09:40-11:30 | RSI & MACD Aligned</p>

        <div class="dashboard-card">
            <div class="stat-box">
                <span class="stat-val">{total_f}</span>
                <span class="stat-label">Expert Trades</span>
            </div>
            <div class="stat-box">
                <span class="stat-val">{win_rate:.1f}%</span>
                <span class="stat-label">Win Rate</span>
            </div>
            <div class="stat-box">
                <span class="stat-val {'pnl-plus' if net_pnl > 0 else 'pnl-minus'}">{net_pnl:.2f}%</span>
                <span class="stat-label">Net Session ROI*</span>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th width="150">Symbol</th>
                    <th width="120">ORB Range</th>
                    <th>Breakout (Long) Confidence View</th>
                    <th>Breakdown (Short) Confidence View</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for row in rows:
        symbol, orb_h, orb_l, bo_json, bd_json = row
        orb_range = f"<small>H: {orb_h:.2f}<br>L: {orb_l:.2f}</small>" if orb_h else "-"
        
        md_content += f"| {symbol} | {format_events(bo_json)} | {format_events(bd_json)} |\n"
        
        def build_column(json_str, mode):
            if not json_str: return "-"
            events = json.loads(json_str)
            html = ""
            for e in events:
                is_ex = is_expert_trade(e, mode)
                ex_tag = '<span class="expert-badge">Elite Signal</span>' if is_ex else ""
                ex_class = "expert" if is_ex else ""
                o = e.get('outcome', '-')
                o_class = "target" if o == "TARGET" else ("sl" if o == "SL_HIT" else "sq")
                
                html += f"""
                <div class="event-card {ex_class}">
                    {ex_tag}
                    <span class="time">{e['time']} Entry</span>
                    <div class="metrics">
                        <span class="metric">Vol: <strong>{e.get('vol_surge', 0)}x</strong></span>
                        <span class="metric">RSI: {e.get('rsi', 0)}</span>
                        <span class="metric">MACD: {e.get('macd', 0)}</span>
                        <span class="metric">Trnd: {e.get('slope', 0)}</span>
                    </div>
                    <div class="perf-line">
                        <span class="outcome {o_class}">{o}</span>
                        <span class="metric">PnL: <strong>{e.get('pnl', 0)}%</strong></span>
                        <span class="metric">Time: {e.get('duration', 0)}m</span>
                    </div>
                </div>
                """
            return html

        html_content += f"""
            <tr>
                <td><strong>{symbol}</strong></td>
                <td>{orb_range}</td>
                <td>{build_column(bo_json, 'bo')}</td>
                <td>{build_column(bd_json, 'bd')}</td>
            </tr>
        """
        
    html_content += """
            </tbody>
        </table>
        <p style="font-size: 0.8em; color: #636c76; margin-top: 20px;">*Net Session ROI assumes equal lot allocation per signal with 0.05% brokerage/slippage deduction.</p>
    </body>
    </html>
    """
    
    # Save both
    with open("orb_event_report.md", "w") as f: f.write(md_content)
    with open("orb_event_report.html", "w") as f: f.write(html_content)
    print("✅ Expert Confidence Report generated.")

if __name__ == "__main__":
    generate_report()
