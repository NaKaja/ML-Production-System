from apscheduler.schedulers.blocking import BlockingScheduler
from batch_pipeline import batch_pipeline
import time

scheduler = BlockingScheduler()
scheduler.add_job(batch_pipeline, 'interval', hours=6)
batch_pipeline() # Initial run
scheduler.start()