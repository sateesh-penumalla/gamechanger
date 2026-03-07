import json
import os
from loguru import logger
from typing import List, Dict

class CuratorAgent:
    def __init__(self, config_path: str):
        self.config_path = config_path
        logger.info(f"Curator Agent Initialized with config: {config_path}")

    def get_watchlist(self) -> List[str]:
        """Loads the current watchlist from the JSON config."""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    data = json.load(f)
                    return data.get("watchlist", [])
        except Exception as e:
            logger.error(f"Error loading watchlist: {e}")
        return []

    def update_watchlist(self, market_mood: Dict, trending_sectors: List[str]):
        """
        Dynamically updates the watchlist based on market mood and sector trends.
        In a real scenario, this would poll a news API or sector performance API.
        """
        current_list = self.get_watchlist()
        logger.info(f"Curator evaluating watchlist updates. Current mood: {market_mood.get('mood')}")

        # Simple logic for now: If market is highly bullish, ensure we have top names
        # In the future, this will add high-beta stocks in bullish markets
        # and defensive stocks in bearish markets.
        
        # Placeholder for dynamic addition logic
        # if market_mood.get('mood') == 'Bullish':
        #     current_list.append("ADANIENT") 
        
        # self._save_watchlist(list(set(current_list)))

    def _save_watchlist(self, new_list: List[str]):
        """Saves the updated watchlist back to the JSON config."""
        try:
            with open(self.config_path, 'w') as f:
                json.dump({"watchlist": new_list}, f, indent=2)
            logger.info("Watchlist updated successfully.")
        except Exception as e:
            logger.error(f"Error saving watchlist: {e}")
