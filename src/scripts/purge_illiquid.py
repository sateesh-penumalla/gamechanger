import os
import sys
import pandas as pd
from loguru import logger

# Add current directory and src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.data.yfinance_client import YahooFinanceData
from src.agents.scout import ScoutAgent

def purge_sec_list():
    logger.info("Initializing Sector List Purge (Liquidity Filter)...")
    
    csv_path = os.path.join(os.path.dirname(__file__), '../../sec_list.csv')
    try:
        df = pd.read_csv(csv_path)
        symbols = df['Symbol'].tolist()
        logger.info(f"Original list size: {len(symbols)}")
    except Exception as e:
        logger.error(f"Failed to load {csv_path}: {e}")
        return

    data_client = YahooFinanceData()
    scout = ScoutAgent(data_client)
    
    cleaned_symbols = []
    failed_symbols = []
    
    logger.info("Starting liquidity check for all symbols. This will take time...")
    
    for i, symbol in enumerate(symbols):
        if i % 10 == 0:
            logger.info(f"Progress: {i}/{len(symbols)} processed. Found {len(cleaned_symbols)} valid stocks so far.")
        
        # Check liquidity using ScoutAgent
        setups = scout.scan_for_setups([symbol])
        
        if setups and setups[0].get("status") != "ILLIQUID":
            cleaned_symbols.append(symbol)
        else:
            failed_symbols.append(symbol)

    # 4. Save cleaned list back to CSV
    # Filter the original dataframe to only include valid symbols
    cleaned_df = df[df['Symbol'].isin(cleaned_symbols)]
    
    cleaned_csv_path = os.path.join(os.path.dirname(__file__), '../../sec_list_cleaned.csv')
    cleaned_df.to_csv(cleaned_csv_path, index=False)
    
    logger.info("="*50)
    logger.info("PURGE COMPLETE")
    logger.info(f"Original Count: {len(symbols)}")
    logger.info(f"Cleaned Count: {len(cleaned_symbols)}")
    logger.info(f"Removed: {len(failed_symbols)}")
    logger.info(f"Cleaned list saved to: {cleaned_csv_path}")
    logger.info("="*50)
    
    # Optional: Swap files
    # os.replace(cleaned_csv_path, csv_path)
    # logger.info(f"Successfully updated {csv_path}")

if __name__ == "__main__":
    purge_sec_list()
