from loguru import logger
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from src.db.schema import get_ist_now

class CatalystHunterAgent:
    def __init__(self, data_client: Any, newsroom: Any, db_session: Session = None):
        self.data_client = data_client
        self.newsroom = newsroom
        self.db = db_session
        logger.info("Catalyst Hunter Agent Initialized")

    def find_breakout_candidates(self, symbols: List[str]) -> List[Dict]:
        """
        Scans for stocks hitting multi-year highs with explosive volume.
        """
        candidates = []
        consecutive_errors = 0
        for symbol in symbols:
            try:
                # 1. Fetch long-term daily data (2 years for multi-year breakout)
                data = self.data_client.fetch_realtime_data(symbol, period="2y", interval="1d")
                if data is None:
                    # Increment error counter if data fetch actually failed (not just too short)
                    # We'll be lenient on 'None' if it's a specific symbol issue, but 
                    # 5 in a row usually means a broader API/Network issue.
                    consecutive_errors += 1
                    if consecutive_errors >= 5:
                        raise Exception("Circuit Breaker Tripped: 5 consecutive failures in Catalyst Hunter.")
                    continue
                
                consecutive_errors = 0 # Reset on success
                
                if len(data) < 250:
                    continue

                # 2. Extract Current Metrics
                current_price = data.iloc[-1]['Close']
                current_vol = data.iloc[-1]['Volume']

                # 3. Identify Multi-Year High (52-week and 2-year)
                prev_data_2y = data.iloc[:-5] # Look at high BEFORE this week
                max_high_2y = prev_data_2y['High'].max()
                max_high_52w = data.iloc[-250:-5]['High'].max() if len(data) > 255 else max_high_2y
                
                # Check if we are currently breaking out OR have just broken out in last 2 days
                is_breaking_new_ground = current_price > max_high_2y or current_price > max_high_52w
                
                # 3. Identify Volume Surge (Relative to 20-day average)
                avg_vol_20d = data.iloc[-25:-5]['Volume'].mean()
                current_vol = data.iloc[-1]['Volume']
                volume_multiplier = current_vol / avg_vol_20d if avg_vol_20d > 0 else 0
                
                # Criteria: Current High-Volume Breakout or Just Started (Vol >= 1.5x)
                is_hit = is_breaking_new_ground and volume_multiplier >= 1.5
                
                # Calculate distance to 52w high for NEAR misses
                dist_to_high = (max_high_52w - current_price) / max_high_52w if max_high_52w > 0 else 0
                is_near = (not is_hit) and (volume_multiplier >= 3.0 or (dist_to_high < 0.02 and volume_multiplier > 1.2))

                if is_hit or is_near:
                    logger.info(f"🔥 Potential Catalyst: {symbol} at ₹{current_price} (Vol: {volume_multiplier:.1f}x, Hit: {is_hit})")
                    
                    # 4. Verify Catalyst via Newsroom
                    sentiment = self.newsroom.analyze_sentiment(symbol)
                    
                    # Targets: Using standard momentum extensions
                    target_1 = current_price * 1.08
                    target_2 = current_price * 1.15
                    target_3 = current_price * 1.25
                    
                    days_to_wait = 5 if sentiment['score'] > 75 else 3
                    
                    candidate = {
                        "symbol": symbol,
                        "type": "CATALYST_HIT" if is_hit else "CATALYST_NEAR",
                        "price": round(current_price, 2),
                        "multiplier": round(volume_multiplier, 2),
                        "dist_to_high_pct": round(dist_to_high * 100, 2),
                        "sentiment": sentiment['sentiment'],
                        "sentiment_score": sentiment['score'],
                        "reason": ", ".join(sentiment.get('highlights', [])),
                        "target_1": round(target_1, 2),
                        "target_2": round(target_2, 2),
                        "target_3": round(target_3, 2),
                        "sl": round(max_high_2y * 0.98, 2),
                        "hold_period_days": days_to_wait
                    }
                    candidates.append(candidate)
                    
                    # Incremental Persistence
                    if self.db:
                        from src.db.schema import CatalystScan
                        try:
                            scan = CatalystScan(
                                symbol=candidate['symbol'],
                                scan_type=candidate['type'],
                                current_price=candidate['price'],
                                volume_multiplier=candidate['multiplier'],
                                dist_to_high_pct=candidate['dist_to_high_pct'],
                                sentiment_score=candidate['sentiment_score'],
                                target_1=candidate['target_1'],
                                target_2=candidate['target_2'],
                                target_3=candidate['target_3'],
                                stop_loss=candidate['sl'],
                                timestamp=get_ist_now()
                            )
                            self.db.add(scan)
                            self.db.commit()
                            logger.info(f"✅ Persisted {symbol} to DB.")
                        except Exception as persistence_error:
                            logger.error(f"Failed to persist {symbol}: {persistence_error}")
                            self.db.rollback()
            except Exception as e:
                logger.error(f"Error scanning {symbol} in Catalyst Hunter: {e}")
                
        return candidates
