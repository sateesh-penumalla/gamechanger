
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import os
from datetime import datetime
from src.db.schema import SystemJob, get_ist_now
from dotenv import load_dotenv
from pytz import timezone
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

load_dotenv()

class SchedulerService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SchedulerService, cls).__new__(cls)
            cls._instance.scheduler = None
            cls._instance.db_url = os.getenv("DATABASE_URL")
            cls._instance.engine = create_engine(cls._instance.db_url)
            cls._instance.Session = sessionmaker(bind=cls._instance.engine)
        return cls._instance

    def start(self):
        if self.scheduler and self.scheduler.running:
            return

        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': ThreadPoolExecutor(20)
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 1
        }

        from pytz import timezone
        self.scheduler = AsyncIOScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=timezone('Asia/Kolkata'))
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        self.scheduler.start()
        print("SchedulerService: Started with listeners.")

    def _job_listener(self, event):
        job_id = event.job_id
        if event.exception:
            print(f"Job {job_id} failed!")
            self._update_job_status(job_id, "FAILED", error=str(event.exception))
        else:
            self._update_job_status(job_id, "RUNNING")

    def add_job(self, job_id, func, trigger, **kwargs):
        """Adds a job and updates SystemJob table."""
        # Remove existing if any to avoid duplicates on restart
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
            
        self.scheduler.add_job(func, trigger, id=job_id, replace_existing=True, **kwargs)
        self._update_job_status(job_id, "RUNNING")

    def stop_job(self, job_id):
        if self.scheduler.get_job(job_id):
            self.scheduler.pause_job(job_id)
            self._update_job_status(job_id, "STOPPED")

    def resume_job(self, job_id):
        if self.scheduler.get_job(job_id):
            self.scheduler.resume_job(job_id)
            self._update_job_status(job_id, "RUNNING")

    def _update_job_status(self, job_id, status, error=None):
        session = self.Session()
        try:
            job = session.query(SystemJob).filter_by(job_id=job_id).first()
            if not job:
                job = SystemJob(job_id=job_id)
                session.add(job)
            
            job.status = status
            job.last_run = get_ist_now()
            
            # --- NEW: Get Next Run Time ---
            if self.scheduler:
                aps_job = self.scheduler.get_job(job_id)
                if aps_job and aps_job.next_run_time:
                    # Convert to naive datetime if needed, or keep timezone
                    # DB expects naive or compatible string. aps_job.next_run_time is timezone aware (IST)
                    job.next_run = aps_job.next_run_time.replace(tzinfo=None)
            
            if error:
                job.error_log = error
            session.commit()
        except Exception as e:
            print(f"Scheduler DB Sync Error: {e}")
            session.rollback()
        finally:
            session.close()

scheduler_service = SchedulerService()
