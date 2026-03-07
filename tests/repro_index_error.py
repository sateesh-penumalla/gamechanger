import pandas as pd
from unittest.mock import MagicMock
import sys
import os

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.agents.sniper_monitor import SniperMonitorAgent

def test_get_dashboard_data_empty_data():
    mock_data_client = MagicMock()
    # Mocking fetch_realtime_data to return an empty DataFrame
    mock_data_client.fetch_realtime_data.return_value = pd.DataFrame()
    
    mock_db = MagicMock()
    
    # Mock active trades
    mock_trade = MagicMock()
    mock_trade.symbol = "NTPC"
    mock_trade.invested_amount = 10000
    mock_trade.avg_price = 100
    mock_trade.target_price = 101
    mock_trade.qty = 100
    
    mock_db.query.return_value.all.side_effect = [[mock_trade], []] # First call for active, second for realized
    
    agent = SniperMonitorAgent(mock_data_client, mock_db)
    
    print("Testing get_dashboard_data with empty DataFrame...")
    try:
        data = agent.get_dashboard_data()
        print("Success! Dashboard data retrieved without IndexError.")
        print(f"Active trades: {data['active_trades']}")
    except IndexError as e:
        print(f"Failed! Caught expected IndexError: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Failed! Caught unexpected exception: {type(e).__name__}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_get_dashboard_data_empty_data()
