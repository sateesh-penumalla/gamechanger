
import os
import pandas as pd
import pytz
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.db.schema import IntradayTick, DailyFocus, Ticker, SystemJob
from dotenv import load_dotenv

load_dotenv()

class ReplaySimulator:
    def __init__(self, target_date="2026-02-06", speed=0.5):
        self.target_date = target_date
        self.speed = speed # Seconds to wait between minutes
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.history_base_dir = f"data/history/{target_date}"
        self.ist_tz = pytz.timezone('Asia/Kolkata')

    def run(self):
        session = self.Session()
        try:
            # 1. STOP the Intraday Feeder if it's running
            feeder = session.query(SystemJob).filter_by(job_id='intraday_feeder').first()
            if feeder and feeder.status == 'RUNNING':
                print("Sim: Stopping Intraday Feeder to avoid conflicts...")
                feeder.status = 'STOPPED'
                session.commit()

            # 2. Get SNIPER Stocks
            snipers = session.query(Ticker).filter(Ticker.oracle_status.in_(['UP_SNIPER', 'DOWN_SNIPER'])).all()
            symbols = [s.symbol for s in snipers]
            if not symbols:
                print("Sim Error: No SNIPER stocks found in tickers table.")
                return

            print(f"Sim: Starting Replay for {len(symbols)} symbols on {self.target_date}")

            # 3. Cleanup target data for simulation date
            # session.query(IntradayTick).filter(text(f"DATE(timestamp) = '{self.target_date}'")).delete()
            # session.query(DailyFocus).filter(text(f"DATE(date) = '{self.target_date}'")).delete()
            # Using raw text for compatibility with some MySQL drivers if they have issues with DATE() in session
            session.execute(text(f"DELETE FROM intraday_ticks WHERE DATE(timestamp) = '{self.target_date}'"))
            session.execute(text(f"DELETE FROM daily_focus WHERE DATE(date) = '{self.target_date}'"))
            session.commit()

            # 4. Load Data & Initialize Daily Focus
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()
            today_str = today.strftime('%Y-%m-%d')

            print(f"Sim: Replaying {self.target_date} data onto {today_str} database records...")

            focused_data = {} # {symbol: df}
            for sym in symbols:
                csv_path = f"{self.history_base_dir}/{sym}.csv"
                if os.path.exists(csv_path):
                    df = pd.read_csv(csv_path)
                    df['Datetime'] = pd.to_datetime(df['Datetime'])
                    # Map to today
                    df['SimTime'] = df['Datetime'].apply(lambda x: datetime.combine(today, x.time()))
                    focused_data[sym] = df
                    
                    # Calculate ORB (09:15 - 09:30)
                    orb_data = df[(df['Datetime'].dt.time >= datetime.strptime("09:15", "%H:%M").time()) & 
                                  (df['Datetime'].dt.time < datetime.strptime("09:30", "%H:%M").time())]
                    
                    if not orb_data.empty:
                        o_high = orb_data['High'].max()
                        o_low = orb_data['Low'].min()
                        
                        focus = DailyFocus(
                            symbol=sym,
                            date=datetime.combine(today, datetime.min.time()),
                            sector="UNKNOWN",
                            oracle_status="UP_SNIPER",
                            orb_high=o_high,
                            orb_low=o_low,
                            orb_high_clean=o_high + (o_high * 0.001), 
                            orb_low_clean=o_low - (o_low * 0.001),
                            orb_window=15
                        )
                        session.add(focus)
            session.commit()
            print(f"Sim: Daily Focus initialized for {len(focused_data)} symbols.")

            # 5. Start Simulation Loop (Minute-by-Minute)
            sim_times = pd.date_range(start=f"{self.target_date} 09:15:00", end=f"{self.target_date} 15:30:00", freq='1min')
            
            for hist_ts in sim_times:
                current_sim_ts = datetime.combine(today, hist_ts.time())
                print(f"Sim: Streaming Time >> {current_sim_ts.strftime('%H:%M')}")
                
                new_ticks = []
                for sym, df_full in focused_data.items():
                    bar = df_full[df_full['Datetime'] == hist_ts]
                    if not bar.empty:
                        row = bar.iloc[0]
                        tick = IntradayTick(
                            symbol=sym,
                            timestamp=current_sim_ts,
                            open=row['Open'],
                            high=row['High'],
                            low=row['Low'],
                            close=row['Close'],
                            volume=int(row['Volume'])
                        )
                        new_ticks.append(tick)
                
                if new_ticks:
                    session.bulk_save_objects(new_ticks)
                    session.commit()
                
                time.sleep(self.speed)

            print("Sim: Replay Complete.")

        except Exception as e:
            print(f"Sim Error: {e}")
            session.rollback()
        finally:
            session.close()

if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-02-06"
    sim = ReplaySimulator(target_date=date, speed=0.1)
    sim.run()
