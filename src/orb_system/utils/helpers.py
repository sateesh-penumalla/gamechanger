from datetime import datetime, time, timedelta
import pytz

def get_ist_now() -> datetime:
    """Get current time in IST"""
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

def is_market_open() -> bool:
    """Check if Indian market is currently open"""
    now = get_ist_now()
    if now.weekday() >= 5: # Saturday/Sunday
        return False
        
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    return market_open <= now <= market_close

def time_to_datetime(t: time, d: datetime = None) -> datetime:
    """Convert time object to datetime for today or specified date"""
    if d is None:
        d = get_ist_now()
    return datetime.combine(d.date(), t)
