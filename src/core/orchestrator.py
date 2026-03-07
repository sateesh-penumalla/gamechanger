from typing import List, Dict, TypedDict, Annotated
from loguru import logger
from langgraph.graph import StateGraph, END
from src.db.schema import Ticker, get_ist_now

# Define Agent State
class AgentState(TypedDict):
    symbols: List[str]
    candidates: List[Dict]
    market_mood: Dict
    recommendations: List[Dict]
    logs: List[str]

class BharatQuantOrchestrator:
    def __init__(self, scout, chartist, globalist, newsroom, librarian, oracle=None, sector_general=None, bear_hunter=None, news_panic=None, squeeze_hunter=None):
        self.scout = scout
        self.chartist = chartist
        self.globalist = globalist
        self.newsroom = newsroom
        self.librarian = librarian
        self.oracle = oracle
        self.sector_general = sector_general
        self.bear_hunter = bear_hunter
        self.news_panic = news_panic
        self.squeeze_hunter = squeeze_hunter
        
        # In-memory Cooldown (symbol -> timestamp)
        self.alert_cooldowns = {}
        
        # Inject Sector General into Scout for "Sensitive Scout" points
        if self.sector_general:
            self.scout.sector_agent = self.sector_general
        
        self.workflow = self._create_workflow()

    def _create_workflow(self):
        workflow = StateGraph(AgentState)
        
        # Define Nodes
        workflow.add_node("scout", self._scout_node)
        workflow.add_node("bear_scout", self._bear_scout_node) # Shadow Branch
        workflow.add_node("analyze", self._analyze_node)
        workflow.add_node("judge", self._judge_node)
        
        # Define Edges
        workflow.set_entry_point("scout")
        workflow.add_edge("scout", "bear_scout")
        workflow.add_edge("bear_scout", "analyze")
        workflow.add_edge("analyze", "judge")
        workflow.add_edge("judge", END)
        
        return workflow.compile()

    def _scout_node(self, state: AgentState):
        logger.info("--- SCOUT NODE ---")
        candidates = self.scout.scan_for_setups(state["symbols"])
        # Also check global mood
        mood = self.globalist.analyze_market_mood()
        
        # Progressive Save: Scout & Mood
        if self.scout.db:
            for cand in candidates:
                ticker = self.scout.db.query(Ticker).filter(Ticker.symbol == cand['symbol']).first()
                if ticker:
                    ticker.scout_score = cand.get("score", 0)
                    ticker.mood_score = mood.get("conviction", 0)
                    ticker.last_updated = get_ist_now()
            self.scout.db.commit()

        return {"candidates": candidates, "market_mood": mood}

    def _bear_scout_node(self, state: AgentState):
        """Shadow Branch: Hunts for Panic and Squeezes without touching main Scout."""
        bear_candidates = []
        if self.bear_hunter:
            logger.info("--- BEAR SCOUT: Searching for Panic ---")
            bear_candidates += self.bear_hunter.scan_for_shorts(state["symbols"])
        
        if self.squeeze_hunter:
            logger.info("--- SQUEEZE SCOUT: Searching for Rebounds ---")
            bear_candidates += self.squeeze_hunter.scan_for_rebounds(state["symbols"])
        
        all_candidates = state["candidates"] + bear_candidates
        return {"candidates": all_candidates}

    def _analyze_node(self, state: AgentState):
        logger.info("--- ANALYZE NODE ---")
        analyzed_candidates = []
        for cand in state["candidates"]:
            # --- PERSISTENT SENTIMENT LOGIC ---
            # 1. Start with the existing DB score to prevent "Default 50" misleading values
            ticker = self.scout.db.query(Ticker).filter(Ticker.symbol == cand['symbol']).first()
            existing_score = 50.0
            existing_sent_obj = {"score": 50, "label": "NEUTRAL", "reason": "No previous data"}
            is_cache_fresh = False

            if ticker and ticker.sentiment_score is not None:
                existing_score = ticker.sentiment_score
                existing_sent_obj = {
                    "score": ticker.sentiment_score,
                    "label": "BULLISH" if ticker.sentiment_score > 60 else ("BEARISH" if ticker.sentiment_score < 40 else "NEUTRAL"),
                    "reason": "Retained from DB"
                }
                
                if ticker.sentiment_updated_at:
                    age_mins = (get_ist_now() - ticker.sentiment_updated_at).total_seconds() / 60
                    if age_mins < 30: # 30 Minute Sensitivity
                        is_cache_fresh = True
                        existing_sent_obj["reason"] = f"Cached ({int(age_mins)}m ago)"

            # Default to existing
            sent = existing_sent_obj
            tech_conviction = 0

            # 2. Skip deep analysis for illiquid stocks
            if cand.get("status") != "ILLIQUID":
                # --- STALE DATA GUARD ---
                # Check LTP against candle close to prevent "Hindsight Lag"
                try:
                    quote = self.scout.data_client.get_quote_data(cand['symbol'])
                    if quote:
                        ltp = float(quote.get('ltp', 0))
                        candle_close = cand.get('price', 0)
                        if candle_close > 0 and abs(ltp - candle_close) / candle_close > 0.05:
                            logger.warning(f"Data Lag Detected for {cand['symbol']}: LTP {ltp} vs Candle {candle_close}. Skipping.")
                            continue
                except Exception as e:
                    logger.warning(f"Stale Data check failed for {cand['symbol']}: {e}")

                # --- COOLDOWN GUARD ---
                last_alert = self.alert_cooldowns.get(cand['symbol'])
                if last_alert:
                    mins_since = (get_ist_now().timestamp() - last_alert) / 60
                    if mins_since < 60:
                        logger.info(f"Cooldown: Skipping {cand['symbol']} (Alerted {int(mins_since)}m ago)")
                        continue

                # -- PRECISION PULSE: Confirm 1m setup with 5m technical conviction --
                data_5m = self.scout.data_client.fetch_realtime_data(cand['symbol'], period="5d", interval="5m")
                if data_5m is not None:
                    tech = self.chartist.analyze_technical_setup(data_5m)
                    levels = self.chartist.get_execution_levels(data_5m)
                    cand["tech_analysis"] = tech
                    cand["execution_levels"] = levels
                    tech_conviction = tech.get("conviction", 0)
                    
                    # NEW: Swing levels for promising candidates
                    daily_data = self.scout.data_client.fetch_realtime_data(cand['symbol'], period="1mo", interval="1d")
                    if daily_data is not None and levels:
                        swing = self.chartist.get_swing_levels(daily_data, levels['entry'], levels['stop_loss'])
                        cand["swing_levels"] = swing
                
                # Sector alignment
                if self.sector_general:
                    sec = self.sector_general.validate_trend(cand['symbol'])
                    cand["sector_analysis"] = sec
                    if sec["status"] == "BEARISH":
                        cand["score"] -= 20
                
                # 3. Dynamic Analysis: Only hit Gemini if the setup is "Hot" AND cache is stale
                scout_score = cand.get("score", 0)
                mood_score = state.get("market_mood", {}).get("conviction", 0)
                combined_rank = (scout_score + tech_conviction + mood_score) / 3

                if combined_rank >= 45 and not is_cache_fresh:
                    fresh_sent = self.newsroom.analyze_sentiment(cand['symbol'])
                    if ticker:
                        ticker.sentiment_score = fresh_sent["score"]
                        ticker.sentiment_updated_at = get_ist_now()
                        self.scout.db.commit()
                        logger.info(f"Sentiment Analysis REFRESHED for {cand['symbol']} (Score: {fresh_sent['score']})")
                    sent = fresh_sent
            
            cand["news_sentiment"] = sent
            
            # Progressive Save: Tech & Sentiment (Persistent Heat Map)
            if self.scout.db and ticker:
                ticker.tech_score = tech_conviction
                ticker.sentiment_score = sent.get("score")
                ticker.last_updated = get_ist_now()
                self.scout.db.commit()

            # Logger for User Visibility
            logger.info(f"Analysis: {cand['symbol']:<12} | Scout: {cand.get('score', 0):>2} | Tech: {tech_conviction:>2} | News: {sent.get('score', 50):>2} ({sent.get('reason', 'N/A')})")

            if cand.get("status") != "ILLIQUID":
                analyzed_candidates.append(cand)
            
        return {"candidates": analyzed_candidates}

    def _judge_node(self, state: AgentState):
        logger.info("--- JUDGE NODE ---")
        final_recs = []
        for cand in state["candidates"]:
            # Skip if we don't have execution levels (basic technical prerequisite)
            levels = cand.get("execution_levels")
            if not levels:
                logger.warning(f"Skipping {cand['symbol']}: No execution levels found.")
                continue

            # Rule 4: Librarian check
            if self.librarian.check_past_failures(cand["symbol"], "Breakout"):
                continue
                
            # Rule 5: Sector Veto
            if cand.get("sector_analysis", {}).get("status") == "BEARISH":
                logger.warning(f"Sector Veto: Skipping {cand['symbol']} - Sector trend is bearish.")
                continue

            # Give higher weight to News (Catalyst) and Chartist (Execution Quality)
            scout_score = cand["score"]
            tech_conviction = cand.get("tech_analysis", {}).get("conviction", 0)
            mood_score = state["market_mood"]["conviction"]
            news_score = cand.get("news_sentiment", {}).get("score", 50)
            
            # --- PROGRESSIVE CONFIDENCE (VELOCITY) ---
            history = self.scout.get_score_history(cand['symbol'], limit=5)
            momentum_bonus = 0
            if len(history) >= 2:
                # Check if scores are rising
                if history[-1] > history[0]:
                    momentum_bonus = 10
                    logger.info(f"Momentum Detected for {cand['symbol']}: {history}")
            
            confidence = (scout_score + tech_conviction + mood_score + news_score + momentum_bonus) / 4.1 
            
            # --- CONVERGENCE RULE + SAFETY GUARDS ---
            is_short = cand.get("type") == "SHORT"
            is_rebound = cand.get("type") == "REBOUND"
            is_slingshot = cand.get("is_slingshot") == 1
            
            # Safety Signals from Scout (Deep Dive Guards)
            # We treat MACD and Stoch failures as critical
            safety_signals = [s for s in cand.get("signals", []) if "SAFETY" in s]
            is_panic = len(safety_signals) > 0
            
            if is_panic:
                min_threshold += 30 # Hard Veto
                logger.warning(f"Golden Guard Veto for {cand['symbol']}: {safety_signals}")

            # Update Ticker table
            ticker = self.scout.db.query(Ticker).filter(Ticker.symbol == cand['symbol']).first()
            if ticker:
                ticker.confidence_score = round(confidence, 2)
                ticker.scout_score = scout_score # Update latest scout score
                ticker.last_updated = get_ist_now()
                self.scout.db.commit()

            # RECORD TO SCAN LOG (Librarian) - Every candidate gets logged for history
            try:
                self.librarian.record_scan({
                    "symbol": cand['symbol'],
                    "price": cand['price'],
                    "volume_ratio": cand.get("volume_ratio", 1.0),
                    "is_open_low": cand.get("is_open_low", 0),
                    "is_open_high": cand.get("is_open_high", 0),
                    "day_change_pct": cand.get("day_change_pct", 0),
                    "scout_score": scout_score,
                    "mood_score": mood_score,
                    "final_confidence": round(confidence, 2),
                    "status": "CANDIDATE" if confidence >= 40 else "REJECTED",
                    "signals": cand.get("signals", [])
                })
            except Exception as e:
                logger.error(f"Failed to record scan log for {cand['symbol']}: {e}")

            logger.info(f"Judging {cand['symbol']}: Scout={scout_score}, Tech={tech_conviction}, Velocity=+{momentum_bonus} | Confidence: {confidence:.2f}%")
            
            if confidence >= min_threshold:
                # Add to Cooldown Memory immediately once judged for alert
                self.alert_cooldowns[cand['symbol']] = get_ist_now().timestamp()

                swing = cand.get("swing_levels", {})
                
                # Allocation Logic
                base_slot = 40000
                allocation = round(base_slot * (confidence / 80), 2)
                allocation = min(allocation, 60000)
                
                # Fix: Initialize entry_price from levels
                entry_price = levels.get("entry", 0)
                if is_short or is_rebound:
                    entry_price = cand.get("entry", entry_price)

                qty = int(allocation / entry_price) if entry_price > 0 else 0

                rec = {
                    "symbol": cand["symbol"],
                    "confidence": round(confidence, 2),
                    "action": "BUY" if (not is_short) else "SELL",
                    "signal_type": "REBOUND" if is_rebound else ("PANIC_SELL" if is_short else "BREAKUP"),
                    "price": entry_price,
                    "qty": qty,
                    "allocation": allocation,
                    "sl": cand["sl"] if (is_short or is_rebound) else levels["stop_loss"],
                    "t1": cand["target"] if (is_short or is_rebound) else levels["target_1"],
                    "t2": levels["target_2"] if not (is_short or is_rebound) else None,
                    "t3": swing.get("target_3"),
                    "trailing_sl_ema": swing.get("suggested_trailing_sl")
                }
                
                # Rule 6: Persist to Librarian Memory
                db_rec = {
                    "symbol": rec["symbol"],
                    "confidence_score": rec["confidence"],
                    "signal_type": rec["signal_type"],
                    "entry_price": rec["price"],
                    "stop_loss": rec["sl"],
                    "target_1": rec["t1"],
                    "target_2": rec["t2"],
                    "target_3": rec["t3"],
                    "hold_type": "INTRADAY",
                    "daily_ema_sl": rec["trailing_sl_ema"]
                }
                self.librarian.record_recommendation(db_rec)
                final_recs.append(rec)
        
        return {"recommendations": final_recs}

    def warm_up(self, symbols: List[str]):
        """Runs pre-session syncs (Oracle weekly trend check) only if needed."""
        if not self.oracle:
            logger.warning("No Oracle Agent provided. Skipping warm-up.")
            return

        # Smart Check: Have we already updated today?
        today = get_ist_now().date()
        recent_update = self.scout.db.query(Ticker).filter(
            Ticker.last_updated >= today
        ).first()

        if recent_update:
            logger.info("Warm-up Skipped: Tickers already synced for today. 🚀")
            return

        logger.info("Orchestrator starting fresh warm-up (Oracle Sync)...")
        self.oracle.sync_ticker_db(symbols)

    def run(self, symbols: List[str]):
        initial_state = {
            "symbols": symbols,
            "candidates": [],
            "market_mood": {},
            "recommendations": [],
            "logs": []
        }
        return self.workflow.invoke(initial_state)

    def monitor_active_positions(self, open_positions: List[str]) -> List[Dict]:
        """
        Active Defense Loop (Call every 1m): Checks open positions for Panic Signals.
        Returns list of EXIT commands if guards are breached.
        """
        exits = []
        if not open_positions:
            return []
            
        logger.info(f"--- ACTIVE DEFENSE: Monitoring {len(open_positions)} positions ---")
        
        for symbol in open_positions:
            # check_safety_status is a lightweight 1m check
            safety = self.scout.check_safety_status(symbol)
            
            if safety["is_panic"]:
                logger.critical(f"🚨 PANIC TRIGGERED for {symbol}: {safety['reason']} -> EXITING NOW")
                exits.append({
                    "symbol": symbol,
                    "action": "PANIC_EXIT",
                    "reason": safety["reason"],
                    "timestamp": get_ist_now()
                })
        
        return exits
