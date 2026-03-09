import os
import sys
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add current directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data.yfinance_client import YahooFinanceData
from src.agents.scout import ScoutAgent
from src.agents.globalist import GlobalistAgent
from src.agents.librarian import LibrarianAgent
from src.agents.oracle import OracleAgent
from src.agents.chartist import ChartistAgent
from src.db.schema import init_db

def technical_only_scan():
    logger.info("Starting Technical-Only Scanner (No GenAI)...")
    
    # 1. Load Tickers from CSV
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'sec_list.csv')
    try:
        df = pd.read_csv(csv_path)
        symbols = df['Symbol'].tolist()
        logger.info(f"Loaded {len(symbols)} symbols from {csv_path}")
    except Exception as e:
        logger.error(f"Failed to load {csv_path}: {e}")
        return

    # 2. Setup Database
    db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root@localhost/bharatquant_mas")
    engine = create_engine(db_url)
    init_db(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # 3. Initialize Agents
    data_client = YahooFinanceData()
    oracle = OracleAgent(data_client, session)
    scout = ScoutAgent(data_client, session)
    chartist = ChartistAgent()
    globalist = GlobalistAgent(data_client)
    librarian = LibrarianAgent(session)

    # 4. Global Market Mood Check
    mood_data = globalist.analyze_market_mood()
    logger.info(f"Market Mood: {mood_data['mood']} (Conviction: {mood_data['conviction']}%)")
    
    # NEW STEP: Oracle Warm-up (Weekly Trend Sync)
    logger.info("--- ORACLE WARM-UP (Weekly Trend Sync) ---")
    oracle.sync_ticker_db(symbols)

    # 5. Scan Symbols
    # Note: Scanning 3000+ stocks sequentially might take time. 
    # For a real run, you might want to slice this (e.g. symbols[:100])
    # Remove limit to scan the ENTIRE list as requested
    logger.info(f"Processing all {len(symbols)} symbols from sec_list.csv...")
    
    recommendations = []
    for i, symbol in enumerate(symbols):
        if i % 10 == 0:
            logger.info(f"Progress: {i}/{len(symbols)} stocks processed...")
        # Fetch data for the symbol
        data = data_client.fetch_realtime_data(symbol, period="1d", interval="5m")
        if data is None or len(data) < 2:
            continue

        # Scout check - passing the symbol in a list as before
        setups = scout.scan_for_setups([symbol])
        
        if not setups:
            # Log FETCH FAILURE so user knows why it's missing from analysis
            scan_data = {
                "symbol": symbol,
                "price": 0.0,
                "volume_ratio": 0.0,
                "is_open_low": 0,
                "is_open_high": 0,
                "day_change_pct": 0.0,
                "scout_score": 0.0,
                "mood_score": mood_data["conviction"],
                "final_confidence": 0.0,
                "status": "FETCH_FAILED",
                "signals": ["Unable to retrieve price data from Yahoo Finance"]
            }
            librarian.record_scan(scan_data)
            continue
            
        cand = setups[0]
        
        # Handle Liquidity Veto
        if cand.get("status") == "ILLIQUID":
            scan_data = {
                "symbol": symbol,
                "price": cand["price"],
                "volume_ratio": 0.0,
                "is_open_low": 0,
                "is_open_high": 0,
                "day_change_pct": 0.0,
                "scout_score": 0.0,
                "mood_score": mood_data["conviction"],
                "final_confidence": 0.0,
                "status": "ILLIQUID",
                "signals": [cand["reason"]]
            }
            librarian.record_scan(scan_data)
            logger.info(f"Symbol: {symbol} | Status: ILLIQUID | Reason: {cand['reason']}")
            continue

        # Rule: Past Failure check
        is_vetoed = librarian.check_past_failures(symbol, "Breakout")
        
        # Simplified Technical Confidence (Scout + Mood)
        # Without News, we rely 50/50 on Scout and Global Mood
        confidence = (cand["score"] + mood_data["conviction"]) / 2
        
        # Determine Status
        status = "REJECTED"
        if not is_vetoed:
            if confidence >= 65:
                status = "RECOMMENDED"
            elif cand["score"] >= 40:
                status = "CANDIDATE"

        # Log EVERY successful scan to the database
        scan_data = {
            "symbol": symbol,
            "price": cand["price"],
            "volume_ratio": cand["volume_ratio"],
            "is_open_low": cand["is_open_low"],
            "is_open_high": cand["is_open_high"],
            "day_change_pct": cand["day_change_pct"],
            "scout_score": cand["score"],
            "mood_score": mood_data["conviction"],
            "final_confidence": round(confidence, 2),
            "status": status,
            "signals": cand["signals"]
        }
        librarian.record_scan(scan_data)
        
        logger.info(f"Symbol: {symbol} | Scout: {cand['score']} | Mood: {mood_data['conviction']} | Confidence: {confidence:.2f}% | Status: {status}")
        
        # Add to recommendations list for the final report
        if status == "RECOMMENDED":
            # Fetch levels manually since scout doesn't return them
            levels = chartist.get_execution_levels(data)
            if not levels:
                logger.warning(f"Could not calculate execution levels for {symbol}")
                continue
            
            # Fetch daily data for Swing levels
            daily_data = data_client.fetch_realtime_data(symbol, period="1mo", interval="1d")
            swing_levels = {}
            if daily_data is not None:
                swing_levels = chartist.get_swing_levels(daily_data, levels['entry'], levels['stop_loss'])

            rec = {
                "symbol": symbol,
                "confidence": round(confidence, 2),
                "action": "BUY" if cand["score"] > 50 else "SELL",
                "price": cand["price"],
                "entry": levels["entry"],
                "sl": levels["stop_loss"],
                "t1": levels["target_1"],
                "t2": levels["target_2"],
                "t3": swing_levels.get("target_3"),
                "trailing_sl": swing_levels.get("suggested_trailing_sl")
            }
            recommendations.append(rec)
            
            # Record explicit recommendation
            db_rec = {
                "symbol": rec["symbol"],
                "confidence_score": rec["confidence"],
                "signal_type": "BREAKUP" if rec["action"] == "BUY" else "BREAKDOWN",
                "entry_price": rec["entry"],
                "stop_loss": rec["sl"],
                "target_1": rec["t1"],
                "target_2": rec["t2"],
                "target_3": rec["t3"],
                "hold_type": "INTRADAY",
                "daily_ema_sl": rec["trailing_sl"]
            }
            librarian.record_recommendation(db_rec)

    # 6. Final Report
    try:
        print("\n" + "="*80)
        print("BHARATQUANT PROFESSIONAL WAR ROOM REPORT")
        print("="*80)
        if not recommendations:
            print("No high-conviction trades found at this time.")
        else:
            for r in recommendations:
                print(f"SYMBOL: {r['symbol']} | ACTION: {r['action']} | CONFIDENCE: {r['confidence']}%")
                print(f"   ENTRY: ₹{r['entry']} | EXIT-SL: ₹{r['sl']}")
                print(f"   INTRADAY TARGETS: T1: ₹{r['t1']} | T2: ₹{r['t2']}")
                if r.get("t3"):
                    print(f"   HYBRID SWING (CARRY FORWARD):")
                    print(f"    - Target 3: ₹{r['t3']}")
                    print(f"    - Trailing SL: ₹{r['trailing_sl']} (Daily 9/20 EMA)")
                    print(f"    - RATIONALE: Carry forward if targets not met by 3:10 PM and price > Day Median.")
                print("-" * 40)
        print("="*80 + "\n")
    finally:
        session.close()

if __name__ == "__main__":
    technical_only_scan()
