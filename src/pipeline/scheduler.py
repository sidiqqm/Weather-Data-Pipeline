import schedule
import time
import logging
from datetime import datetime
from src.pipeline.orchestrator import PipelineOrchestrator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PipelineScheduler:
    """
    Schedule and run pipeline at regular intervals
    """
    
    def __init__(self):
        self.orchestrator = PipelineOrchestrator()
        logger.info("Pipeline Scheduler initialized")
    
    def run_scheduled_pipeline(self):
        """Wrapper for scheduled pipeline run"""
        logger.info(f"\n{'='*70}")
        logger.info(f"SCHEDULED PIPELINE RUN - {datetime.now()}")
        logger.info(f"{'='*70}")
        
        try:
            result = self.orchestrator.run_pipeline()
            
            if result['status'] == 'success':
                logger.info("Scheduled pipeline run completed successfully")
            else:
                logger.error(f"Scheduled pipeline run failed: {result.get('error')}")
        
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
    
    def start_hourly_schedule(self):
        """Run pipeline every hour"""
        logger.info("=" * 70)
        logger.info("STARTING HOURLY SCHEDULER")
        logger.info("=" * 70)
        logger.info("Pipeline will run every hour")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 70)
        
        # Schedule job
        schedule.every().hour.do(self.run_scheduled_pipeline)
        
        # Run immediately on start
        logger.info("\n Running initial pipeline...")
        self.run_scheduled_pipeline()
        
        # Keep running
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("\n  Scheduler stopped by user")
    
    def start_daily_schedule(self, run_time="08:00"):
        """Run pipeline once per day at specific time"""
        logger.info("=" * 70)
        logger.info(f"STARTING DAILY SCHEDULER (Run time: {run_time})")
        logger.info("=" * 70)
        logger.info(f"Pipeline will run daily at {run_time}")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 70)
        
        # Schedule job
        schedule.every().day.at(run_time).do(self.run_scheduled_pipeline)
        
        # Keep running
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("\n  Scheduler stopped by user")
    
    def start_interval_schedule(self, minutes=30):
        """Run pipeline every N minutes"""
        logger.info("=" * 70)
        logger.info(f"STARTING INTERVAL SCHEDULER (Every {minutes} minutes)")
        logger.info("=" * 70)
        logger.info(f"Pipeline will run every {minutes} minutes")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 70)
        
        # Schedule job
        schedule.every(minutes).minutes.do(self.run_scheduled_pipeline)
        
        # Run immediately
        logger.info("\n Running initial pipeline...")
        self.run_scheduled_pipeline()
        
        # Keep running
        try:
            while True:
                schedule.run_pending()
                time.sleep(30)
        except KeyboardInterrupt:
            logger.info("\n  Scheduler stopped by user")


def start_scheduler(mode='interval', interval_minutes=30, daily_time="08:00"):
    """
    Start scheduler in fferent modes
    
    Args:
        mode: 'hourly', 'daily', or 'interval'
        interval_minutes: minutes between runs (for interval mode)
        daily_time: time to run (for daily mode, format: "HH:MM")
    """
    scheduler = PipelineScheduler()
    
    if mode == 'hourly':
        scheduler.start_hourly_schedule()
    elif mode == 'daily':
        scheduler.start_daily_schedule(run_time=daily_time)
    elif mode == 'interval':
        scheduler.start_interval_schedule(minutes=interval_minutes)
    else:
        raise ValueError("Mode must be 'hourly', 'daily', or 'interval'")