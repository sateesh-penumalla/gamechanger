from loguru import logger
import pandas as pd
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from src.db.schema import Portfolio, RealizedPnL, get_ist_now
from src.data.yfinance_client import YahooFinanceData

class SniperMonitorAgent:
    def __init__(self, data_client: Any, db_session: Session):
        self.data_client = data_client
        self.db = db_session
        logger.info("Sniper Monitor Agent Initialized")

    def monitor_portfolio(self) -> List[Dict]:
        """
        Scans all active trades in the portfolio and checks for exits.
        Returns a list of actions (SELL/HOLD).
        """
        active_trades = self.db.query(Portfolio).all()
        actions = []

        for trade in active_trades:
            data = self.data_client.fetch_realtime_data(trade.symbol, period="1d", interval="1m")
            if data is None or data.empty:
                continue
            
            current_price = data.iloc[-1]['Close']
            pnl_pct = ((current_price - trade.avg_price) / trade.avg_price) * 100
            
            action = "HOLD"
            # Logic: Exit at 1% Target
            if current_price >= trade.target_price:
                action = "SELL_TARGET"
            elif current_price <= trade.stop_loss:
                action = "SELL_SL"
            # Logic: Breakeven Trailing (if hits 0.5%, move SL to entry)
            elif pnl_pct >= 0.5 and trade.stop_loss < trade.avg_price:
                trade.stop_loss = trade.avg_price
                self.db.commit()
                logger.info(f"SniperMonitor: Moved SL to entry for {trade.symbol} (Profit: {pnl_pct:.2f}%)")
            
            if "SELL" in action:
                self._realize_trade(trade, current_price, pnl_pct)
                actions.append({"symbol": trade.symbol, "action": action, "price": current_price, "pnl_pct": pnl_pct})

        return actions

    def _realize_trade(self, trade: Portfolio, sell_price: float, pnl_pct: float):
        """Moves a trade from Portfolio to RealizedPnL."""
        profit_amount = (sell_price - trade.avg_price) * trade.qty
        
        realized = RealizedPnL(
            symbol=trade.symbol,
            buy_price=trade.avg_price,
            sell_price=sell_price,
            qty=trade.qty,
            profit_amount=profit_amount,
            profit_pct=pnl_pct
        )
        self.db.add(realized)
        self.db.delete(trade)
        self.db.commit()
        logger.info(f"✅ Realized Trade: {trade.symbol} | PnL: {pnl_pct:.2f}% | Profit: ₹{profit_amount:,.2f}")

    def get_dashboard_data(self) -> Dict:
        """Compiles stats for the CLI dashboard."""
        active = self.db.query(Portfolio).all()
        realized = self.db.query(RealizedPnL).all()
        
        total_realized_profit = sum([r.profit_amount for r in realized])
        invested_capital = sum([p.invested_amount for p in active])
        
        # Calculate Current PnL for active positions
        active_details = []
        for p in active:
            data = self.data_client.fetch_realtime_data(p.symbol, period="1d", interval="1m")
            cur_price = data.iloc[-1]['Close'] if data is not None and not data.empty else p.avg_price
            pnl = ((cur_price - p.avg_price) / p.avg_price) * 100
            active_details.append({
                "symbol": p.symbol,
                "invested": p.invested_amount,
                "pnl_pct": pnl,
                "status": "Target: ₹{:.2f}".format(p.target_price)
            })

        return {
            "active_trades": active_details,
            "realized_profit": total_realized_profit,
            "invested_capital": invested_capital,
            "free_cash": 200000 - invested_capital,
            "trade_count": len(realized)
        }
