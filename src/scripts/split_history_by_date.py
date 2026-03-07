import os
import pandas as pd
from loguru import logger
from datetime import datetime

def split_history():
    # Paths
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    history_dir = os.path.join(base_dir, 'data', 'history')
    
    # Find all 6m files
    source_files = [f for f in os.listdir(history_dir) if f.endswith('_6m_1m.csv')]
    
    if not source_files:
        logger.error(f"No 6m source files found in: {history_dir}")
        return

    logger.info(f"Found {len(source_files)} symbols to reorganize.")

    for source_file_name in source_files:
        symbol = source_file_name.replace('_6m_1m.csv', '')
        source_file = os.path.join(history_dir, source_file_name)
        
        logger.info(f"Processing {symbol}...")
        
        try:
            df = pd.read_csv(source_file)
            
            # Ensure Datetime is correct
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            # Extract Date only for grouping
            df['DateStr'] = df['Datetime'].dt.strftime('%Y-%m-%d')
            
            dates = df['DateStr'].unique()
            # logger.info(f"  - Found {len(dates)} days.")
            
            for date_str in dates:
                # Create folder for the date
                date_folder = os.path.join(history_dir, date_str)
                os.makedirs(date_folder, exist_ok=True)
                
                # Filter data for this day
                daily_df = df[df['DateStr'] == date_str].copy()
                # Drop the helper column
                daily_df = daily_df.drop(columns=['DateStr'])
                
                # Save to date folder
                target_path = os.path.join(date_folder, f'{symbol}.csv')
                daily_df.to_csv(target_path, index=False)
                
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")

    logger.info(f"✅ Reorganization of {len(source_files)} symbols complete.")

if __name__ == "__main__":
    split_history()
