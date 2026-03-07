from loguru import logger
from sqlalchemy.orm import Session
from src.db.schema import PerformanceLog, TradeRecommendation, ScanLog, Portfolio
from typing import List

class LibrarianAgent:
    def __init__(self, db_session: Session):
        self.db = db_session
        logger.info("Librarian Agent Initialized")

    def check_past_failures(self, symbol: str, setup_name: str) -> bool:
        """
        Returns True if this setup has failed recently for this stock (Rule 4).
        """
        # Simple check for now: count failures in the last 48h
        # Placeholder for actual DB query logic
        failures = self.db.query(PerformanceLog).filter(
            PerformanceLog.setup_name == setup_name
        ).count()
        
        if failures >= 2:
            logger.warning(f"Librarian VETO: {setup_name} has failed {failures} times recently.")
            return True
        return False

    def record_recommendation(self, rec_data: dict):
        """Logs a new recommendation to the database."""
        new_rec = TradeRecommendation(**rec_data)
        self.db.add(new_rec)
        self.db.commit()
        logger.info(f"Librarian recorded recommendation for {rec_data['symbol']}")

    def add_to_portfolio(self, portfolio_data: dict):
        """Adds a newly entered trade to the active portfolio."""
        new_entry = Portfolio(**portfolio_data)
        self.db.add(new_entry)
        self.db.commit()
        logger.info(f"Librarian added {portfolio_data['symbol']} to active Portfolio.")

    def record_scan(self, scan_data: dict):
        """Logs detailed scanning metrics for any stock analyzed."""
        new_scan = ScanLog(**scan_data)
        self.db.add(new_scan)
        self.db.commit()
        logger.info(f"Librarian logged scan metrics for {scan_data['symbol']} (Status: {scan_data['status']})")
