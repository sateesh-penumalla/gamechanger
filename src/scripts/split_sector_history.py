import os
import pandas as pd
from loguru import logger
from datetime import datetime

def split_sector_history():
    # Paths
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    sector_indices_dir = os.path.join(base_dir, 'data', 'history_sectors', 'indices')
    sector_history_root = os.path.join(base_dir, 'data', 'history_sectors')
    
    # 1. Find all 6m files in the indices buffer
    if not os.path.exists(sector_indices_dir):
        logger.error(f"Sector indices directory not found: {sector_indices_dir}")
        return

    source_files = [f for f in os.listdir(sector_indices_dir) if f.endswith('_6m_1m.csv')]
    
    if not source_files:
        logger.error(f"No 6m sector source files found in: {sector_indices_dir}")
        return

    logger.info(f"Found {len(source_files)} sector indices to reorganize.")

    for source_file_name in source_files:
        # e.g., NIFTY_BANK_6m_1m.csv -> NIFTY_BANK
        index_name = source_file_name.replace('_6m_1m.csv', '')
        source_file = os.path.join(sector_indices_dir, source_file_name)
        
        logger.info(f"Processing Sector: {index_name}...")
        
        try:
            df = pd.read_csv(source_file)
            
            # Ensure Datetime is correct
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            # Extract Date only for grouping
            df['DateStr'] = df['Datetime'].dt.strftime('%Y-%m-%d')
            
            dates = df['DateStr'].unique()
            
            for date_str in dates:
                # Create folder for the date: data/history_sectors/<DATE>
                date_folder = os.path.join(sector_history_root, date_str)
                os.makedirs(date_folder, exist_ok=True)
                
                # Filter data for this day
                daily_df = df[df['DateStr'] == date_str].copy()
                # Drop the helper column
                daily_df = daily_df.drop(columns=['DateStr'])
                
                # Save to date folder as <SECTOR_NAME>.csv (e.g., NIFTY_BANK.csv)
                target_path = os.path.join(date_folder, f'{index_name}.csv')
                daily_df.to_csv(target_path, index=False)
                
        except Exception as e:
            logger.error(f"Error processing sector {index_name}: {e}")

    logger.success(f"✅ Reorganization of {len(source_files)} sector indices complete.")

if __name__ == "__main__":
    split_sector_history()
