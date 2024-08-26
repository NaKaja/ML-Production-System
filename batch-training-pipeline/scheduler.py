from apscheduler.schedulers.blocking import BlockingScheduler
from batch_training_pipeline import batch_training_pipeline
import time

scheduler = BlockingScheduler()
scheduler.add_job(batch_training_pipeline, 'interval', hours=6)
batch_training_pipeline() # Initial run
scheduler.start()