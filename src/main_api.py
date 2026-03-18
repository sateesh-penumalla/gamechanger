
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, or_
from sqlalchemy.orm import sessionmaker
import os
from datetime import datetime
from dotenv import load_dotenv

from src.db.schema import DailyFocus, Position, SystemJob, init_db, IntradayTick, Ticker, ORBSignal, ORBSignalNearMiss
from src.scheduler.core import scheduler_service
from src.services.intraday_feeder import IntradayFeeder
from src.scripts.signal_generator import SignalGenerator
from src.services.portfolio_manager import PortfolioManager
from src.agents.oracle import OracleAgent
from src.services.orb_calculator import ORBCalculator
from src.data.yfinance_client import YahooFinanceData

from loguru import logger
load_dotenv()

# Initialize API
app = FastAPI(title="Mimic Lab API", version="4.0.0")

# CORS Configuration
# Allowing all origins for development ease with React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi.responses import JSONResponse

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled server error on {request.url}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"message": "Internal Server Error", "details": str(exc)}
    )

# Database Configuration
db_url = os.getenv("DATABASE_URL")
engine = create_engine(db_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Default Configurations for Services
DEFAULT_JOB_CONFIGS = {
    "intraday_feeder": {
        "interval_minutes": 1,
        "fetch_from_hour": 9,
        "fetch_from_minute": 0
    },
    "signal_generator": {
        "interval_minutes": 1,
        "offset_seconds": 30,
        "rsi_l_min": 55,
        "rsi_l_max": 80,
        "macd_l_min": 0.05,
        "trend_l_min": 0.05,
        "rsi_s_min": 20,
        "rsi_s_max": 45,
        "macd_s_max": -0.05,
        "trend_s_max": -0.05,
        "range_pct_min": 0.5,
        "range_pct_max": 3.0,
        "enforce_bias": 1,
        "min_adtv": 0.5,
        "long_snipers": ["UP_SNIPER"],
        "short_snipers": ["DOWN_SNIPER"]
    },
    "portfolio_manager": {
        "interval_minutes": 1,
        "offset_seconds": 45,
        "tp_pct": 2.0,
        "sl_type": "ORB_BOUNDARY",
        "trailing_sl": 0,
        "max_capital": 50000,
        "min_capital": 5000
    },
    "oracle_sync": {
        "hour": 8,
        "minute": 0
    },
    "orb_calculator": {
        "hour": 9,
        "minute": 31
    }
}

# Dependency for DB Session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Initialize Services
feeder = IntradayFeeder()
sig_gen = SignalGenerator()
port_mgr = PortfolioManager()
orb_calc = ORBCalculator()
oracle_data_client = YahooFinanceData()

# Initialize Oracle (using Yahoo Finance for weekly data)
# --- BRIDGE TRACKING ---
LAST_BRIDGE_HEARTBEAT = None

# We'll initialize OracleAgent inside functions to get it a fresh session if needed, 
# but for the scheduler, we'll need a way to pass one.

@app.get("/system/bridge/status")
async def get_bridge_status():
    """Returns the last heartbeat of the macOS alert bridge."""
    is_active = False
    if LAST_BRIDGE_HEARTBEAT:
        diff = (datetime.now() - LAST_BRIDGE_HEARTBEAT).total_seconds()
        is_active = diff < 15 # Consider active if heartbeat in last 15s
    
    return {
        "is_active": is_active,
        "last_heartbeat": LAST_BRIDGE_HEARTBEAT.isoformat() if LAST_BRIDGE_HEARTBEAT else None
    }

@app.post("/system/bridge/heartbeat")
async def post_bridge_heartbeat():
    """Update the heartbeat for the macOS alert bridge."""
    global LAST_BRIDGE_HEARTBEAT
    LAST_BRIDGE_HEARTBEAT = datetime.now()
    return {"status": "ok"}


# --- STARTUP EVENT ---
@app.on_event("startup")
async def startup_event():
    print("Initializing Database...")
    init_db(engine)
    
    print("Starting Scheduler Service...")
    scheduler_service.start()
    
    # Initialize Default Configs in DB
    db = SessionLocal()
    try:
        for job_id, config in DEFAULT_JOB_CONFIGS.items():
            job = db.query(SystemJob).filter(SystemJob.job_id == job_id).first()
            if not job:
                job = SystemJob(job_id=job_id, status="STOPPED", config=config)
                db.add(job)
            elif not job.config:
                job.config = config
        db.commit()
    finally:
        db.close()
    
    # Schedule Core Service Loops
    # These run periodically to check for data, signals, and manage trades
    
    enable_auto = os.getenv("ENABLE_AUTO_TRADING", "false").lower() == "true"
    
    if enable_auto:
        print("🚀 Automated Trading ENABLED via Environment Variable.")
        # 1. IntradayDataFeeder: Runs every minute to fetch latest candles
        scheduler_service.add_job(
            'intraday_feeder', 
            feeder.run_cycle, 
            'interval', 
            minutes=1, 
            max_instances=1
        )
        
        # 3. PortfolioManager: Runs every minute (offset 45s) to manage execution
        scheduler_service.add_job(
            'portfolio_manager', 
            port_mgr.run_cycle, 
            'interval', 
            minutes=1, 
            seconds=45, 
            max_instances=1
        )
    else:
        print("🛑 Automated Trading DISABLED (ENABLE_AUTO_TRADING is false). Only data ingestion will run if enabled.")

    # 4. Oracle Sync: Runs every day at 8:00 AM IST
    def run_oracle_job():
        db = SessionLocal()
        try:
            agent = OracleAgent(oracle_data_client, db)
            agent.sync_ticker_db()
        finally:
            db.close()

    scheduler_service.add_job(
        'oracle_sync',
        run_oracle_job,
        'cron',
        hour=8,
        minute=0
    )

    # 5. ORB Calculator: Runs every day at 9:31 AM IST
    scheduler_service.add_job(
        'orb_calculator',
        orb_calc.run_cycle,
        'cron',
        hour=9,
        minute=31
    )
    
    print("Core Services Scheduled.")

# --- API ENDPOINTS ---

@app.get("/health")
def health_check():
    return {"status": "ok", "timestamp": datetime.now()}

def _enrich_with_live_prices(db, items):
    """Enriches a list of items (signals/positions) with the latest price from intraday_ticks and daily_focus data."""
    symbols = list(set([item.symbol for item in items]))
    if not symbols:
        return []
    
    # Get latest ticks for these symbols
    # Subquery to find the latest timestamp per symbol
    from sqlalchemy import func
    ticks = []
    try:
        latest_ts = db.query(
            IntradayTick.symbol, 
            func.max(IntradayTick.timestamp).label('max_ts')
        ).filter(IntradayTick.symbol.in_(symbols)).group_by(IntradayTick.symbol).subquery()

        ticks = db.query(IntradayTick).join(
            latest_ts, 
            (IntradayTick.symbol == latest_ts.c.symbol) & (IntradayTick.timestamp == latest_ts.c.max_ts)
        ).all()
    except Exception as e:
        logger.error(f"Failed to fetch live prices from intraday_ticks: {e}")
    
    tick_map = {
        t.symbol: {
            'close': t.close,
            'imbalance': t.mean_imbalance,
            'iceberg_score': t.iceberg_score,
            'buy_volume': t.buy_volume,
            'sell_volume': t.sell_volume,
            'avg_bid_qty': t.avg_bid_qty,
            'avg_ask_qty': t.avg_ask_qty,
            'bid_pct': t.bid_pct,
            'ask_pct': t.ask_pct
        } for t in ticks
    }
    
    # Get daily_focus data for these symbols (today's data)
    focus_items = db.query(DailyFocus).filter(
        DailyFocus.symbol.in_(symbols),
        func.date(DailyFocus.date) == func.date(func.now())
    ).all()
    
    focus_map = {
        f.symbol: {
            'oracle_status': f.oracle_status,
            'orb_direction': f.orb_direction,
            'adtv_cr': f.avg_daily_turnover,
            'range_pct': f.orb_range_pct,
            'orb_high_clean': f.orb_high_clean,
            'orb_low_clean': f.orb_low_clean,
            'orb_high': f.orb_high,
            'orb_low': f.orb_low,
        } for f in focus_items
    }
    
    # Convert to dict and add live_price + order flow + daily_focus data
    results = []
    for item in items:
        # Use __dict__ but remove sqlalchemy state
        d = {k: v for k, v in item.__dict__.items() if k != '_sa_instance_state'}
        
        # Add Live Market Data (Close + Order Flow)
        ld = tick_map.get(item.symbol, {})
        d['live_price'] = ld.get('close')
        d['imbalance'] = ld.get('imbalance')
        d['iceberg_score'] = ld.get('iceberg_score')
        d['buy_volume'] = ld.get('buy_volume')
        d['sell_volume'] = ld.get('sell_volume')
        d['avg_bid_qty'] = ld.get('avg_bid_qty')
        d['avg_ask_qty'] = ld.get('avg_ask_qty')
        d['bid_pct'] = ld.get('bid_pct', 50.0)
        d['ask_pct'] = ld.get('ask_pct', 50.0)
        
        # Add daily_focus fields
        focus_data = focus_map.get(item.symbol, {})
        d.update(focus_data)
        
        # Calculate clean range % if we have the data
        if focus_data.get('orb_high_clean') and focus_data.get('orb_low_clean'):
            orb_high_clean = focus_data['orb_high_clean']
            orb_low_clean = focus_data['orb_low_clean']
            clean_range = orb_high_clean - orb_low_clean
            mid_price = (orb_high_clean + orb_low_clean) / 2
            if mid_price > 0:
                d['range_pct_clean'] = (clean_range / mid_price) * 100
        
        results.append(d)
    return results

@app.get("/dashboard/focus")
def get_daily_focus(db: Session = Depends(get_db)):
    """Returns today's Focus Watchlist with live prices."""
    from sqlalchemy import func
    focus = db.query(DailyFocus).filter(
        func.date(DailyFocus.date) == func.date(func.now()),
        ~DailyFocus.oracle_status.like("REJECTED%")
    ).all()
    return _enrich_with_live_prices(db, focus)

@app.get("/dashboard/signals")
def get_signals(db: Session = Depends(get_db)):
    """Returns signals enriched with live prices. Includes non-focus stocks."""
    from sqlalchemy import func
    # Use outerjoin to include signals even if they aren't in DailyFocus
    signals = db.query(ORBSignal).outerjoin(
        DailyFocus,
        (ORBSignal.symbol == DailyFocus.symbol) & (func.date(ORBSignal.date) == DailyFocus.date)
    ).filter(
        func.date(ORBSignal.date) == func.date(func.now())
    ).order_by(ORBSignal.timestamp.desc()).all()
    return _enrich_with_live_prices(db, signals)

@app.get("/dashboard/signals/nearmiss")
def get_near_miss_signals(db: Session = Depends(get_db)):
    """Returns signals that missed filters enriched with live prices for TODAY only."""
    from sqlalchemy import func
    signals = db.query(ORBSignalNearMiss).join(
        DailyFocus,
        (ORBSignalNearMiss.symbol == DailyFocus.symbol) & (func.date(ORBSignalNearMiss.date) == DailyFocus.date)
    ).filter(
        func.date(ORBSignalNearMiss.date) == func.date(func.now())
    ).order_by(ORBSignalNearMiss.timestamp.desc()).all()
    return _enrich_with_live_prices(db, signals)

@app.post("/dashboard/signals/nearmiss/{nm_id}/promote")
def promote_near_miss(nm_id: int, db: Session = Depends(get_db)):
    """Convert a Near Miss into a real PENDING Signal for execution."""
    nm = db.query(ORBSignalNearMiss).filter(ORBSignalNearMiss.id == nm_id).first()
    if not nm:
        raise HTTPException(status_code=404, detail="Near Miss not found")
    
    # Check if signal already exists to prevent duplicates
    existing = db.query(ORBSignal).filter(
        ORBSignal.symbol == nm.symbol,
        ORBSignal.date == nm.date,
        ORBSignal.side == nm.side
    ).first()
    
    if existing:
        return {"status": "ok", "message": "Signal already exists", "sig_id": existing.id}

    # Create real Signal
    sig = ORBSignal(
        symbol=nm.symbol,
        side=nm.side,
        date=nm.date,
        timestamp=nm.timestamp,
        entry_price=nm.entry_price,
        signal_type=nm.signal_type,
        status="PENDING",
        sl=nm.sl,
        tp=nm.tp,
        metrics=nm.metrics
    )
    db.add(sig)
    # Remove near miss to avoid double counting
    db.delete(nm)
    db.commit()
    db.refresh(sig)
    
    return {"status": "ok", "message": f"Promoted {nm.symbol} to real Signal", "sig_id": sig.id}

@app.post("/dashboard/signals/{sig_id}/demote")
def demote_signal(sig_id: int, db: Session = Depends(get_db)):
    """Convert a Signal back into a Near Miss."""
    sig = db.query(ORBSignal).filter(ORBSignal.id == sig_id).first()
    if not sig:
        raise HTTPException(status_code=404, detail="Signal not found")
    
    # Only allow demoting if not yet executed
    if sig.status == 'EXECUTED' or sig.execution_pos_id:
        raise HTTPException(status_code=400, detail="Cannot demote an executed signal")

    # Create Near Miss
    nm = ORBSignalNearMiss(
        symbol=sig.symbol,
        side=sig.side,
        date=sig.date,
        timestamp=sig.timestamp,
        entry_price=sig.entry_price,
        signal_type=sig.signal_type,
        sl=sig.sl,
        tp=sig.tp,
        fail_reasons=["Manually Demoted"],
        metrics=sig.metrics
    )
    db.add(nm)
    db.delete(sig)
    db.commit()
    return {"status": "ok", "message": f"Demoted {sig.symbol} back to Near Miss"}

@app.get("/dashboard/positions")
def get_positions(db: Session = Depends(get_db)):
    """Returns recent positions enriched with live prices for TODAY only."""
    from sqlalchemy import func
    positions = db.query(Position).filter(
        func.date(Position.entry_time) == func.date(func.now())
    ).order_by(Position.entry_time.desc()).all()
    return _enrich_with_live_prices(db, positions)

@app.get("/system/jobs")
def get_system_jobs(db: Session = Depends(get_db)):
    """Returns status of all background jobs."""
    return db.query(SystemJob).all()

@app.post("/system/jobs/{job_id}/config")
async def update_job_config(job_id: str, request: Request, db: Session = Depends(get_db)):
    """Update configuration for a specific job."""
    try:
        config = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    job = db.query(SystemJob).filter(SystemJob.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Ensure config allows new keys by merging instead of replacing if needed, 
    # but for now replacement is fine as frontend sends full config.
    job.config = config
    db.commit()
    return {"status": "ok", "job_id": job_id, "config": config}

@app.post("/system/jobs/{job_id}/{action}")
def control_job(job_id: str, action: str, db: Session = Depends(get_db)):
    """Start/Stop/Resume background jobs."""
    if action == "start" or action == "resume":
        scheduler_service.resume_job(job_id)
        status = "RUNNING"
    elif action == "stop" or action == "pause":
        scheduler_service.stop_job(job_id)
        status = "STOPPED"
    else:
        raise HTTPException(status_code=400, detail="Invalid action. Use 'start' or 'stop'.")
    
    return {"job_id": job_id, "action": action, "status": status}

@app.get("/system/jobs/{job_id}/config")
def get_job_config(job_id: str, db: Session = Depends(get_db)):
    """Fetch configuration for a specific job."""
    job = db.query(SystemJob).filter(SystemJob.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.config or {}

@app.post("/dashboard/positions/{pos_id}/execute")
def execute_position(pos_id: int, db: Session = Depends(get_db)):
    """Manually trigger order placement for a signaled position."""
    pos = db.query(Position).filter(Position.id == pos_id).first()
    if not pos:
        raise HTTPException(status_code=404, detail="Position not found")
    
    if pos.status not in ['SIGNALED', 'PENDING']:
        raise HTTPException(status_code=400, detail="Only SIGNALED or PENDING positions can be executed.")
    
    # Fetch remote positions for the double-entry guard
    remote_positions = port_mgr.dhan_client.get_positions()
    
    # Trigger execution logic 
    success, message = port_mgr._execute_entry(db, pos, remote_positions)
    db.commit()
    
    if not success:
        # If it's a "Skipped" message, return 200 with the reason
        if "Skipped" in message:
            return {"status": "skipped", "message": message, "new_status": pos.status}
        raise HTTPException(status_code=500, detail=message)
    
    return {"status": "ok", "message": f"Order placed for {pos.symbol}", "new_status": pos.status}

@app.post("/dashboard/signals/{sig_id}/execute")
def execute_signal(sig_id: int, db: Session = Depends(get_db)):
    """Manually execute a staged signal -> Fire Dhan API + Create Position."""
    sig = db.query(ORBSignal).filter(ORBSignal.id == sig_id).first()
    if not sig:
        raise HTTPException(status_code=404, detail="Signal not found")
    
    if sig.status != 'PENDING':
        raise HTTPException(status_code=400, detail=f"Signal status is {sig.status}, cannot execute.")
    
    # 1. Create Position Record in PENDING status
    pos = Position(
        symbol=sig.symbol,
        side=sig.side,
        date=sig.date,
        entry_time=datetime.now(),
        entry_price=sig.entry_price,
        qty=1, 
        signal_type=sig.signal_type,
        status="PENDING", 
        rider_active=0,
        sl=sig.sl,
        tp=sig.tp,
        sl_type="ORB_BOUNDARY",
        entry_metrics=sig.metrics,
        agent_audit_log=f"Manual execution trigger for ORB Signal {sig.id}"
    )
    db.add(pos)
    db.flush() 
    
    # 2. Trigger PortfolioManager Execution Logic
    try:
        # Fetch remote positions for the double-entry guard
        remote_positions = port_mgr.dhan_client.get_positions()
        success, message = port_mgr._execute_entry(db, pos, remote_positions)
        
        if success and pos.status == 'OPEN':
            # 3. Update Signal Status on success
            sig.status = "EXECUTED"
            sig.execution_pos_id = pos.id
            # Also store the super order IDs for reconciliation
            sig.target_order_id = pos.target_order_id
            sig.sl_order_id = pos.sl_order_id
            
            db.commit()
            return {"status": "ok", "message": f"Successfully executed {sig.symbol} {sig.side} via Dhan.", "pos_id": pos.id}
        else:
            db.rollback()
            # If it's a "Skipped" message, return 200 with status: skipped
            if "Skipped" in message:
                return {"status": "skipped", "message": message}
            raise HTTPException(status_code=500, detail=message or "Dhan Execution failed. Check logs.")
            
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Execution error: {str(e)}")


@app.get("/dashboard/dhan/sync")
def sync_dhan_data(db: Session = Depends(get_db)):
    """Fetches live orders/trades/positions from Dhan and reconciles with local DB."""
    try:
        # 1. Fetch live data from Dhan
        orders = port_mgr.dhan_client.get_order_list()
        trades = port_mgr.dhan_client.get_trade_book()
        dhan_positions = port_mgr.dhan_client.get_positions()
        
        # 2. Reconcile with local Positions table
        local_positions = db.query(Position).filter(
            func.date(Position.date) == func.date(func.now())
        ).all()
        
        local_map = {p.symbol: p for p in local_positions if p.status != 'CLOSED'}
        
        sync_count = 0
        for dp in dhan_positions:
            symbol = dp.get('tradingSymbol')
            if not symbol: continue
            
            # Clean symbol (remove .NS if present)
            symbol = symbol.replace(".NS", "")
            
            # Get latest trade for this symbol to find entry price/time
            symbol_trades = [t for t in trades if t.get('tradingSymbol', '').replace(".NS", "") == symbol]
            latest_trade = symbol_trades[-1] if symbol_trades else {}
            
            if symbol in local_map:
                # Update existing position
                pos = local_map[symbol]
                pos.qty = dp.get('netQty', pos.qty)
                sync_count += 1
            else:
                # Create missing position
                create_time = latest_trade.get('createTime', datetime.now().timestamp())
                try:
                    # Dhan can return string or float timestamp
                    parsed_time = datetime.fromtimestamp(float(create_time))
                except:
                    parsed_time = datetime.now()

                new_pos = Position(
                    symbol=symbol,
                    side=dp.get('positionSide', 'LONG').upper(),
                    date=datetime.now(),
                    entry_time=parsed_time,
                    entry_price=latest_trade.get('price', 0.0),
                    qty=dp.get('netQty', 0),
                    status="OPEN",
                    signal_type="MANUAL_DHAN", # Indicate it was found on Dhan
                    agent_audit_log="Reconciled from Dhan HQ (Found existing position)"
                )
                db.add(new_pos)
                sync_count += 1
        
        if sync_count > 0:
            db.commit()
            
        return {
            "status": "ok",
            "synced_count": sync_count,
            "dhan": {
                "orders": orders,
                "trades": trades,
                "positions": dhan_positions
            },
            "local_sync_status": "COMPLETED"
        }
    except Exception as e:
        import traceback
        print(f"Dhan Sync Error: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dashboard/dhan/funds")
def get_available_funds():
    """Fetches available funds from Dhan."""
    try:
        funds = port_mgr.dhan_client.get_fund_limits()
        return {"status": "ok", "available_cash": funds}
    except Exception as e:
        logger.error(f"Error fetching funds: {e}")
        return {"status": "error", "message": str(e), "available_cash": 0.0}

# --- ORACLE INTELLIGENCE ENDPOINTS ---

@app.post("/system/oracle/sync")
def trigger_oracle_sync(db: Session = Depends(get_db)):
    """Manually trigger Oracle Weekly Trend analysis."""
    agent = OracleAgent(oracle_data_client, db)
    agent.sync_ticker_db()
    return {"status": "ok", "message": "Oracle sync completed."}

@app.get("/system/oracle/results")
def get_oracle_results(db: Session = Depends(get_db)):
    """Returns summarized and detailed results of the last Oracle scan."""
    tickers = db.query(Ticker).all()
    
    up_snipers = [t for t in tickers if t.oracle_status == 'UP_SNIPER']
    down_snipers = [t for t in tickers if t.oracle_status == 'DOWN_SNIPER']
    filtered = [t for t in tickers if t.oracle_status == 'FILTERED']
    ignored = [t for t in tickers if t.oracle_status == 'IGNORE']
    
    # Calculate max last_updated
    last_updated = None
    if tickers:
        # Filter out None values
        dates = [t.last_updated for t in tickers if t.last_updated]
        if dates:
            last_updated = max(dates)
    
    return {
        "summary": {
            "total": len(tickers),
            "up_snipers": len(up_snipers),
            "down_snipers": len(down_snipers),
            "filtered": len(filtered),
            "ignored": len(ignored)
        },
        "last_updated": last_updated,
        "details": {
            "up": [{"symbol": t.symbol, "rsi": t.weekly_rsi, "sma": t.weekly_sma} for t in up_snipers],
            "down": [{"symbol": t.symbol, "rsi": t.weekly_rsi, "sma": t.weekly_sma} for t in down_snipers]
        }
    }

@app.post("/system/orb/sync")
def trigger_orb_calculation():
    """Manually trigger ORB calculation for current day."""
    orb_calc.run_cycle()
    return {"status": "ok", "message": "ORB calculation completed."}

# --- DHAN POSTBACK WEBHOOK ---

@app.post("/dhan/postback")
async def dhan_postback(request: Request, db: Session = Depends(get_db)):
    """
    Receives order update webhooks from Dhan.
    Updates the local Position state accordingly.
    """
    try:
        payload = await request.json()
        logger.info(f"Dhan Postback Received: {payload.get('orderId')} - {payload.get('orderStatus')}")
        
        order_id = payload.get('orderId')
        status = payload.get('orderStatus')
        
        if not order_id:
            return {"status": "ignored", "reason": "No orderId"}
            
        # Find position by dedicated ID columns first
        pos = db.query(Position).filter(
            or_(
                Position.entry_order_id == order_id,
                Position.target_order_id == order_id,
                Position.sl_order_id == order_id
            )
        ).first()
        
        if not pos:
            # Fallback 1: Search in audit log (legacy)
            pos = db.query(Position).filter(Position.agent_audit_log.contains(order_id)).first()
        
        if not pos:
            # Fallback 2: find by symbol and status if IDs are missing
            pos = db.query(Position).filter(
                Position.symbol == payload.get('tradingSymbol'),
                Position.status.in_(['PENDING', 'OPEN', 'SIGNALED'])
            ).first()

        if pos:
            # Update status map
            # Dhan Statuses: TRANSIT, PENDING, REJECTED, CANCELLED, TRADED, EXPIRED
            if status == 'TRADED':
                # Is this an internal state transition? (Entry vs Exit)
                if pos.status in ['PENDING', 'SIGNALED']:
                    pos.status = 'OPEN'
                    if not pos.entry_price or pos.entry_price == 0:
                        pos.entry_price = float(payload.get('price', 0))
                    pos.agent_audit_log = (pos.agent_audit_log or "") + f"\n[Webhook] ENTRY Order TRADED at {payload.get('updateTime')}"
                else:
                    # Likely an EXIT order (Target/SL)
                    pos.status = 'CLOSED'
                    pos.exit_time = datetime.now()
                    pos.exit_price = float(payload.get('price', 0))
                    pos.exit_reason = "Webhook Trade"
                    pos.agent_audit_log = (pos.agent_audit_log or "") + f"\n[Webhook] EXIT Order TRADED at {payload.get('updateTime')}"
                    
                    # --- CRITICAL: CLEAR THE ORIGINATING SIGNAL FOR RE-ARMING ---
                    sig = db.query(ORBSignal).filter(ORBSignal.execution_pos_id == pos.id).first()
                    if sig:
                        # Determine if it was TP or SL (rough estimate or use order_id mapping)
                        if pos.target_order_id == order_id:
                            sig.status = "TARGET_HIT"
                        elif pos.sl_order_id == order_id:
                            sig.status = "SL_HIT"
                        else:
                            sig.status = "FINISHED" # Generic finish
                        print(f"Webhook: Cleared Signal {sig.id} for {pos.symbol}. System RE-ARMED.")

            elif status in ['REJECTED', 'CANCELLED', 'EXPIRED']:
                # If it's a PENDING position that got cancelled, mark it closed.
                if pos.status in ['PENDING', 'SIGNALED']:
                    pos.status = 'FAILED'
                    # Also clear signal so it can retry or create new
                    sig = db.query(ORBSignal).filter(ORBSignal.execution_pos_id == pos.id).first()
                    if sig: sig.status = "REJECTED"
                else:
                    # If it's an exit order that failed, we keep status as is for monitoring
                    pos.agent_audit_log = (pos.agent_audit_log or "") + f"\n[Webhook] EXIT Order {status}: {payload.get('omsErrorDescription')}"
            
            db.commit()
            return {"status": "success", "pos_id": pos.id}
            
        return {"status": "ignored", "reason": "Position not found"}
        
    except Exception as e:
        logger.error(f"Error processing Dhan postback: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# --- ANALYSIS ENDPOINTS ---
@app.get("/analysis/daily-opportunities")
def get_daily_opportunities():
    """Replays today's market data to find all missed opportunities."""
    from src.services.analysis_service import DailyAnalysisService
    svc = DailyAnalysisService()
    return svc.run_analysis()

if __name__ == "__main__":
    import uvicorn
    # Run on 0.0.0.0 to be accessible from outside container/VM
    uvicorn.run(app, host="0.0.0.0", port=8000)
