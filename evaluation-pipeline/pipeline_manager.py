from apscheduler.schedulers.background import BackgroundScheduler
from evaluation_pipeline import evaluation_pipeline
from minio import Minio
import logging
import datetime
import time
import os

MINIO_URL = os.getenv("MINIO_URL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_MODEL_BUCKET = os.getenv("MINIO_MODEL_BUCKET")
MINIO_MODEL_DIRECTORY = os.getenv("MINIO_MODEL_DIRECTORY")

current_model = ''
current_job = None
scheduler = BackgroundScheduler()
scheduler.start()

def get_newest_model():
    minio_client = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    models = list(minio_client.list_objects(MINIO_MODEL_BUCKET, prefix=MINIO_MODEL_DIRECTORY+"/"))
    models.sort(key=lambda obj: obj.last_modified, reverse=True)
    newest_model = models[0].object_name.split("/")[-1]
    return newest_model
    
    
while True:
    newest_model = get_newest_model()
    if newest_model != current_model:
        current_model = newest_model
        if current_job:
            current_job.remove()
        current_job = scheduler.add_job(evaluation_pipeline, 'interval', [newest_model], minutes=30, next_run_time=datetime.datetime.now())
    time.sleep(10)
          