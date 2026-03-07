import os
import requests
import xml.etree.ElementTree as ET
from loguru import logger
from typing import List, Dict, Optional
import json
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import local utilities
from src.utils.notifications import send_macos_notification
from src.db.schema import get_ist_now, Ticker, DailyFocus
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

try:
    from google import genai
    from google.genai import types
    HAS_GENAI = True
except ImportError:
    logger.warning("google-genai package not found. AI features will be limited.")
    HAS_GENAI = False

class NewsImpactScanner:
    """
    Scans Google News RSS for high-impact stock market catalysts in India.
    Identifies Order Wins, Contracts, and LoAs with recency weighting.
    """
    
    # Tiered RSS URLs for aggressive market-wide discovery
    RSS_FEEDS = {
        "Daily": [
            "https://news.google.com/rss/search?q=NSE+BSE+largest+ever+order+OR+massive+contract+OR+mega+deal+India&hl=en-IN&gl=IN&ceid=IN:en&tbs=qdr:d",
            "https://news.google.com/rss/search?q=Indian+stock+market+major+acquisition+OR+merger+OR+buyback&hl=en-IN&gl=IN&ceid=IN:en&tbs=qdr:d",
            "https://news.google.com/rss/search?q=stocks+to+watch+today+for+massive+breakout+OR+upper+circuit+India&hl=en-IN&gl=IN&ceid=IN:en&tbs=qdr:d",
            "https://news.google.com/rss/search?q=corporate+action+high+impact+NSE+BSE+earnings+beat+significant&hl=en-IN&gl=IN&ceid=IN:en&tbs=qdr:d",
            "https://news.google.com/rss/search?q=NSE+BSE+order+win+OR+contract+OR+LOA+OR+agreement+OR+supply+deal+India&hl=en-IN&gl=IN&ceid=IN:en&tbs=qdr:d"
        ]
    }

    def __init__(self, recency_threshold_days: int = 2):
        self.api_key = os.getenv("GOOGLE_API_KEY")
        self.client = None
        self.recency_threshold = recency_threshold_days
        
        # Setup DB
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            self.engine = create_engine(db_url)
            self.Session = sessionmaker(bind=self.engine)
            logger.info("Database connection established for Newsroom persistence.")
        else:
            self.Session = None
            logger.warning("DATABASE_URL not found. Persistence disabled.")

        if HAS_GENAI and self.api_key:
            try:
                self.client = genai.Client(api_key=self.api_key)
                logger.info("Gemini Client initialized for News Impact Scanner")
            except Exception as e:
                logger.error(f"Failed to initialize Gemini: {e}")
        else:
            logger.warning("Proceeding without Gemini (No API key or package)")

    def _parse_rss_date(self, date_str: str) -> Optional[datetime]:
        """Parses RSS RFC 822 date string to timezone-aware datetime."""
        try:
            return parsedate_to_datetime(date_str)
        except Exception:
            return None

    def fetch_headlines(self) -> List[Dict]:
        """Fetches headlines from tiered feeds to ensure fresh news is prioritized."""
        all_items = []
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=self.recency_threshold)
        
        seen_titles = set()
        
        # Process Daily first, then Weekly, then Monthly
        for tier, urls in self.RSS_FEEDS.items():
            tier_items = []
            for url in urls:
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code != 200:
                        continue
                    
                    root = ET.fromstring(response.content)
                    items = root.findall(".//item")
                    
                    for item in items:
                        title = item.find("title").text if item.find("title") is not None else ""
                        if title in seen_titles:
                            continue
                        
                        pub_date_raw = item.find("pubDate").text if item.find("pubDate") is not None else ""
                        link = item.find("link").text if item.find("link") is not None else ""
                        pub_date = self._parse_rss_date(pub_date_raw)
                        
                        if pub_date:
                            if pub_date > cutoff:
                                item_data = {
                                    "title": title,
                                    "pub_date": pub_date.strftime("%Y-%m-%d %H:%M"),
                                    "pub_date_dt": pub_date,
                                    "link": link,
                                    "tier": tier
                                }
                                tier_items.append(item_data)
                                seen_titles.add(title)
                        else:
                            # Keep if date missing and it's from a fresh tier
                            if tier == "Daily":
                                tier_items.append({
                                    "title": title,
                                    "pub_date": "Recent",
                                    "pub_date_dt": now,
                                    "link": link,
                                    "tier": tier
                                })
                                seen_titles.add(title)

                except Exception as e:
                    logger.error(f"Error parsing RSS {url}: {e}")
            
            logger.info(f"Tier {tier}: Found {len(tier_items)} new headlines.")
            all_items.extend(tier_items)
        
        # Sort by date (newest first)
        return sorted(all_items, key=lambda x: x["pub_date_dt"], reverse=True)

    def analyze_news_batch(self, items: List[Dict]) -> List[Dict]:
        """Uses Gemini to identify symbols and scores in a batch of headlines with recency awareness."""
        if not self.client:
            logger.warning("Skipping AI analysis: Gemini client not available.")
            return []

        # Increase headlines sent to 100 for better coverage
        news_text = "\n".join([f"[{item['pub_date']}] {item['title']}" for item in items[:100]])
        
        prompt = f"""
        Analyze the following Indian stock market headlines. 
        Current Time: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")} UTC

        CRITICAL DATE FILTER:
        - Only news from TODAY or YESTERDAY (Feb 26-27, 2026) is valid.
        - If a news item is older than Feb 26, 2026, YOU MUST GIVE IT A SCORE OF 0 and ignore it.
        - We are ONLY looking for catalysts that can drive an UPPER CIRCUIT today.

        Tasks:
        1. Identify MAJOR discovery catalysts: Largest ever orders, Massive Contract Awards, Mega Deals, Multi-billion Mergers, Earnings Surprises, or Upper Circuit driving news.
        2. Focus on "The Next Big Ride": Look for stocks mentioned in the context of massive business shifts.
        3. IGNORE minor price moves (e.g., "stock up 0.5%", "falls 1%") unless they are caused by a catalyst in task 1.
        4. Recency is THE TOP PRIORITY: Anything older than 48 hours is IRRELEVANT.

        Headlines:
        {news_text}

        Return ONLY a JSON list of objects:
        [
          {{"symbol": "NSE_SYMBOL", "company": "Exact Co Name", "impact": "POSITIVE|NEGATIVE", "score": 0-100, "date": "YYYY-MM-DD", "reason": "Short powerful summary"}}
        ]
        """

        try:
            response = self.client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            
            payload = json.loads(response.text)
            return payload
        except Exception as e:
            logger.error(f"AI Analysis failed: {e}")
            return []

    def _persist_impactful_news(self, news_items: List[Dict]):
        """Saves high-impact news to Ticker and DailyFocus tables."""
        if not self.Session:
            return

        session = self.Session()
        try:
            today_dt = get_ist_now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            count = 0
            for item in news_items:
                score = item.get("score", 0)
                if score < 70: continue
                
                symbol = item.get("symbol")
                if not symbol or symbol == "UNKNOWN": continue
                
                # 1. Update Ticker sentiment
                ticker = session.query(Ticker).filter(Ticker.symbol == symbol).first()
                if ticker:
                    ticker.sentiment_score = score
                    ticker.sentiment_updated_at = get_ist_now()
                    ticker.last_updated = get_ist_now()
                
                # 2. Update DailyFocus
                focus = session.query(DailyFocus).filter(
                    DailyFocus.symbol == symbol,
                    DailyFocus.date == today_dt
                ).first()
                
                if not focus:
                    focus = DailyFocus(symbol=symbol, date=today_dt)
                    session.add(focus)
                
                focus.news_score = score
                focus.news_summary = item.get("reason", "")
                focus.last_updated = get_ist_now()
                count += 1
            
            session.commit()
            logger.info(f"Persisted {count} impactful news items to database.")
        except Exception as e:
            logger.error(f"Persistence failed: {e}")
            session.rollback()
        finally:
            session.close()

    def run_scan(self):
        """Main execution loop."""
        logger.info(f"Starting News Impact Scan (Threshold: {self.recency_threshold} days)...")
        items = self.fetch_headlines()
        logger.info(f"Found {len(items)} items to analyze.")
        
        if not items:
            logger.warning("No recent headlines found.")
            return
            
        # Debug: Log first 20 Headlines to see what's being sent
        logger.debug("First 20 Headlines for analysis:")
        for i, item in enumerate(items[:20]):
            logger.debug(f"{i+1}. [{item['pub_date']}] {item['title']}")

        impactful_news = self.analyze_news_batch(items)
        logger.info(f"AI identified {len(impactful_news)} potential catalysts.")

        for news in impactful_news:
            score = news.get("score", 0)
            symbol = news.get("symbol")
            impact = news.get("impact", "NEUTRAL")
            reason = news.get("reason", "No reason provided")
            news_date = news.get("date", "Unknown Date")
            
            # Defensive check for symbol
            if not symbol or symbol == "UNKNOWN" or any(x in str(symbol).lower() for x in ["nifty", "sensex", "nifty50", "none"]):
                continue

            # Log everything found by AI
            logger.info(f"Found: [{news_date}] {symbol} | Score: {score} | {reason}")

            if score >= 80: # Increased threshold for notifications to filter noise
                logger.success(f"🔥 HIGH IMPACT: {symbol} Score: {score}")
                
                # Send notification with date
                emoji = "🚀" if impact == "POSITIVE" else "⚠️"
                title = f"{emoji} {impact} ({news_date}): {symbol}"
                message = f"Score: {score}\n{reason}"
                send_macos_notification(title, message, sound="Hero" if score > 85 else "Glass")

        # Persist to DB
        self._persist_impactful_news(impactful_news)

if __name__ == "__main__":
    scanner = NewsImpactScanner()
    scanner.run_scan()
