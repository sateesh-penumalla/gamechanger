
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import json
from dotenv import load_dotenv

# Load env
load_dotenv()

# Boilerplate
db_url = os.getenv("DATABASE_URL")
if not db_url:
    print("DATABASE_URL not found!")
    exit(1)

engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
session = Session()

try:
    # Manual Schema Definition to avoid full imports
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, String, JSON

    Base = declarative_base()

    class SystemJob(Base):
        __tablename__ = "system_jobs"
        job_id = Column(String(50), primary_key=True)
        status = Column(String(20), default="STOPPED")
        config = Column(JSON, nullable=True)

    # Fetch Job
    job = session.query(SystemJob).filter_by(job_id='portfolio_manager').first()
    
    if job:
        print(f"Found Job: {job.job_id}")
        current_config = job.config or {}
        print(f"Current Config: {current_config}")
        
        # Update
        updated = False
        if 'max_capital' not in current_config:
            current_config['max_capital'] = 50000
            updated = True
        if 'min_capital' not in current_config:
            current_config['min_capital'] = 5000
            updated = True
            
        if updated:
            # SQLAlchemy JSON type requires re-assignment to detect change sometimes
            job.config = dict(current_config) 
            session.commit()
            print(f"Updated Config: {job.config}")
        else:
            print("Config already has capital keys.")
    else:
        print("Job 'portfolio_manager' not found!")

except Exception as e:
    print(f"Error: {e}")
    session.rollback()
finally:
    session.close()
