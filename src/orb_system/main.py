import sys
import os
from datetime import datetime

# Add root directory to path for imports - MUST BE AT TOP
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import time
import schedule
from loguru import logger
from src.orb_system.config.database import db
from src.orb_system.config.settings import settings
from src.orb_system.services.market_data import MarketDataService
from src.orb_system.core.pre_market import PreMarketAnalyzer
from src.orb_system.core.orb_formation import ORBFormationAnalyzer
from src.orb_system.core.signal_generator import SignalGenerator
from src.orb_system.core.execution_engine import ExecutionEngine
from src.orb_system.core.position_manager import PositionManager
from src.orb_system.utils.helpers import get_ist_now

class ORBOrchestrator:
    """Main orchestrator for the BharatQuant ORB Trading System"""
    
    def __init__(self):
        logger.info("Initializing BharatQuant ORB Orchestrator...")
        self.mds = MarketDataService()
        self.pre_market = PreMarketAnalyzer()
        self.orb_analyzer = ORBFormationAnalyzer(self.mds)
        self.signal_gen = SignalGenerator(self.mds)
        self.execution = ExecutionEngine()
        self.position_manager = PositionManager(self.mds)
        
        self.watchlist = []
        self.setups = {}
        self.active_positions = []
        self.regime = None
        
    def run_pre_market(self):
        """Phase 1: 08:00 - 09:15 Analysis and Setup"""
        logger.info("PHASE 1: Starting Pre-Market Analysis...")
        self.regime = self.pre_market.analyze_market_regime()
        
        if self.regime.trade_orb:
            self.watchlist = self.pre_market.generate_watchlist()
            logger.info(f"Targeted Watchlist: {self.watchlist}")
        else:
            logger.warning(f"Market regime unfavorable: {self.regime.reason}. Trading will be restricted.")

    def run_orb_formation(self):
        """Phase 2: 09:15 - 09:45 Monitor and Score Formation"""
        logger.info("PHASE 2: Starting ORB Formation Analysis...")
        today = get_ist_now().date()
        
        for symbol in self.watchlist:
            # Use defaults from settings for live trading, or custom overrides if needed
            setup = self.orb_analyzer.analyze_orb(
                symbol, 
                today, 
                orb_window_mins=settings.ORB_PERIOD_MINUTES,
                orb_style="STANDARD"
            )
            if setup and setup.is_tradeable:
                setup.market_regime_score = self.regime.regime_score
                self.setups[symbol] = setup
                
        logger.info(f"ORB Formation phase completed. {len(self.setups)} tradeable setups identified.")

    def run_trading_session(self):
        """Phase 3: 09:45 - 14:30 Signal Generation and Execution"""
        logger.info("PHASE 3: Starting Live Trading Session...")
        
        while get_ist_now().time() < settings.TRADING_END:
            # 1. Monitor for Signals
            for symbol, setup in self.setups.items():
                if any(p.symbol == symbol for p in self.active_positions):
                    continue
                    
                if len(self.active_positions) >= settings.MAX_CONCURRENT_POSITIONS:
                    continue
                
                signal = self.signal_gen.monitor_breakouts(setup)
                if signal:
                    logger.info(f"SIGNAL DETECTED: {signal.symbol} {signal.signal_type.value}")
                    pos = self.execution.execute_trade(signal)
                    if pos:
                        self.active_positions.append(pos)
            
            # 2. Manage Active Positions
            if self.active_positions:
                self.position_manager.monitor_positions(self.active_positions)
                # Remove closed positions
                self.active_positions = [p for p in self.active_positions if self._is_active(p.trade_id)]

            time.sleep(10) # 10-second polling for live monitoring

    def _is_active(self, trade_id: int) -> bool:
        """Check if trade is still open in DB"""
        with db.get_cursor() as cursor:
            cursor.execute("SELECT status FROM orb_trades WHERE id = %s", (trade_id,))
            row = cursor.fetchone()
            return row and row['status'] == 'OPEN'

    def run_post_market(self):
        """Phase 4: 15:30+ Performance Analysis"""
        logger.info("PHASE 4: Starting Post-Market Performance Analysis...")
        # Summarize day's performance and update learning engine
        pass

def main():
    orchestrator = ORBOrchestrator()
    
    # Schedule phases
    schedule.every().day.at("08:30").do(orchestrator.run_pre_market)
    schedule.every().day.at("09:46").do(orchestrator.run_orb_formation)
    # schedule.every().day.at("09:50").do(orchestrator.run_trading_session)
    schedule.every().day.at("15:45").do(orchestrator.run_post_market)
    
    logger.info("Orchestrator Scheduler active. Waiting for market timings...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
