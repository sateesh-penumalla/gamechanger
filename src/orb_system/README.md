# BharatQuant ORB Trading System

Professional-grade Opening Range Breakout (ORB) signalling and execution system.

## Project Structure
- `main.py`: Main orchestrator for the trading lifecycle.
- `config/`: Database, DhanHQ, and Trading Settings.
- `models/`: Data structures for signals, trades, and positions.
- `core/`:
    - `pre_market.py`: Oracle sync, watchlist generation, and regime analysis.
    - `orb_formation.py`: Breakout boundary calculation and quality scoring.
    - `signal_generator.py`: Live monitoring and adaptive confluence scoring.
    - `execution_engine.py`: DhanHQ order execution.
    - `position_manager.py`: Real-time monitoring and dynamic exits.
- `services/`: Technical indicators and market data wrappers.
- `ui/`: Streamlit-based ORB Lab and Backtesting engine.

## How to Run
1.  **Orchestrator**: `python src/orb_system/main.py`
2.  **ORB Lab UI**: `streamlit run src/orb_system/ui/orb_lab.py`

## Core Features
1.  **Institutional Confluence**: Integrates Oracle (SNIPER), Sector General, and Newsroom agents.
2.  **Adaptive Weighting**: Dynamically adjusts importance of technical indicators during early market hours.
3.  **Active Defense**: Monitors for structural technical failure to trigger early exits.
4.  **Learning Engine**: Tracks stock-specific ORB performance to tier/blacklist symbols.
