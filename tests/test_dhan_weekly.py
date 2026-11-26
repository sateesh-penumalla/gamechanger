import pandas as pd
import numpy as np
from unittest.mock import MagicMock
import sys
import os
from datetime import datetime, timedelta

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data.dhan_client import DhanDataClient

def test_dhan_weekly_resampling():
    # Mock Dhan API
    mock_dhan = MagicMock()
    
    # Generate 14 days of mock daily data relative to today
    base_date = datetime.now() - timedelta(days=14)
    mock_data = []
    for i in range(14):
        dt = base_date + timedelta(days=i)
        mock_data.append({
            'open': 100 + i,
            'high': 110 + i,
            'low': 90 + i,
            'close': 105 + i,
            'volume': 1000,
            'timestamp': int(dt.timestamp())
        })
    
    mock_dhan.historical_daily_data.return_value = {
        'status': 'success',
        'data': mock_data
    }
    
    client = DhanDataClient("fake_id", "fake_token")
    client.dhan = mock_dhan
    # Skip master load for test
    client._id_map = {"RELIANCE": 2885}
    
    print("Testing fetch_realtime_data with interval='1wk'...")
    try:
        df_weekly = client.fetch_realtime_data("RELIANCE", period="14d", interval="1wk")
        
        if df_weekly is None or df_weekly.empty:
            print("Failed: Returned None or empty DataFrame")
            sys.exit(1)
            
        print(f"Retrieved {len(df_weekly)} weekly bars.")
        print(df_weekly)
        
        # Verify columns
        expected_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in expected_cols:
            if col not in df_weekly.columns:
                print(f"Failed: Missing column {col}")
                sys.exit(1)
        
        # Verify resampling (W-MON)
        # 2026-01-01 (Thu) to 2026-01-14 (Wed)
        # Week 1: 01-01 to 01-05 (Mon)
        # Week 2: 01-06 to 01-12 (Mon)
        # Week 3: 01-13 to 01-14 (Partial)
        
        if len(df_weekly) < 2:
            print(f"Failed: Expected at least 2 weekly bars, got {len(df_weekly)}")
            sys.exit(1)
            
        print("Success! Weekly resampling verified.")
    except Exception as e:
        print(f"Failed! Caught unexpected exception: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    test_dhan_weekly_resampling()
