import os
import time
import json
import pickle
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
from minio import Minio
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.metrics import mean_squared_error
from sklearn.base import clone
from io import BytesIO
from prometheus_client import Gauge, Counter, push_to_gateway, CollectorRegistry

registry = CollectorRegistry()
rmse_gauge = Gauge('rmse', 'Root Mean Squared Error', registry=registry)
mape_gauge = Gauge('mape', 'Mean Absolute Percentage Error', registry=registry)
num_records_gauge = Gauge('num_records', 'Number of Records Used for Training', registry=registry)
elapsed_time_gauge = Gauge('elapsed_time', 'Elapsed Batch Job Run Time', registry=registry)
num_models_counter = Counter('num_models', 'Number of Trained Models', registry=registry)

def batch_training_pipeline():
    start = time.time()
    
    MINIO_URL = os.getenv('MINIO_URL')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
    MINIO_BATCH_BUCKET = os.getenv('MINIO_BATCH_BUCKET')
    MINIO_BATCH_DIRECTORY = os.getenv('MINIO_BATCH_DIRECTORY')
    MINIO_MODEL_BUCKET = os.getenv('MINIO_MODEL_BUCKET')
    MINIO_CONFIG_DIRECTORY = os.getenv('MINIO_CONFIG_DIRECTORY')
    MINIO_MODEL_DIRECTORY = os.getenv('MINIO_MODEL_DIRECTORY')
    PUSHGATEWAY_URL = os.getenv('PUSHGATEWAY_URL')
    
    minio_client = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    
    # Fetch most recent batch
    batches = list(minio_client.list_objects(MINIO_BATCH_BUCKET, prefix=MINIO_BATCH_DIRECTORY+"/"))
    batches.sort(key=lambda obj: obj.last_modified, reverse=True)
    batch_obj = minio_client.get_object(MINIO_BATCH_BUCKET, batches[0].object_name)
    reader = pa.BufferReader(batch_obj.read())
    df = feather.read_feather(reader)
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df = df.sort_values(by='date').reset_index(drop=True)

    # Fetch most recent model configuration
    configs = list(minio_client.list_objects(MINIO_MODEL_BUCKET, prefix=MINIO_CONFIG_DIRECTORY+"/"))
    configs.sort(key=lambda obj: obj.last_modified, reverse=True)
    config_obj = minio_client.get_object(MINIO_MODEL_BUCKET, configs[0].object_name)
    pipeline = pickle.loads(config_obj.read())
    
    # Train and evaluate with TimeSeriesSplit
    tss = TimeSeriesSplit(n_splits=5)
    rmses = []
    mapes = []

    for i, (train_index, test_index) in enumerate(tss.split(df)):
        X_train = df.loc[train_index].drop('price', axis=1)
        y_train = df.loc[train_index]['price']
        X_test = df.loc[test_index].drop('price', axis=1)
        y_test = df.loc[test_index]['price']
        
        pipe = clone(pipeline)
        pipe.fit(X_train, y_train)
        
        y_pred = pipe.predict(X_test)
        rmses.append(mean_squared_error(y_test, y_pred, squared=False))
        mapes.append(mean_absolute_percentage_error(y_test, y_pred))
        
    RMSE = sum(rmses)/len(rmses)
    MAPE = sum(mapes)/len(mapes)
    
    # Fit on all training data and upload to Minio
    pipeline.fit(df.drop('price', axis=1), df['price'])
    pipeline_bytes = pickle.dumps(pipeline)
    pipeline_stream = BytesIO(pipeline_bytes)
    
    batch_timestamp = batches[0].last_modified.strftime('%Y-%m-%d_%H:%M:%S')
    config_timestamp = configs[0].last_modified.strftime('%Y-%m-%d_%H:%M:%S') 
    filename = f"model_batch_{batch_timestamp}_config_{config_timestamp}.pkl"
    minio_client.put_object(MINIO_MODEL_BUCKET, f"{MINIO_MODEL_DIRECTORY}/{filename}", pipeline_stream, len(pipeline_bytes))
    
    end = time.time()
    elapsed = end - start
    
    # Send metrics to push-gateway
    report_metrics(RMSE, MAPE, len(df), elapsed, PUSHGATEWAY_URL)

 
def report_metrics(rmse_values, mape_values, num_records, elapsed_time, url):
    global registry, rmse_gauge, mape_gauge, num_records_gauge, elapsed_time_gauge, num_models_counter
 
    # Set gauge and counter values
    rmse_gauge.set(rmse_values)
    mape_gauge.set(mape_values)
    num_records_gauge.set(num_records)
    elapsed_time_gauge.set(elapsed_time)
    num_models_counter.inc()
 
    # Push metrics to Prometheus Pushgateway
    push_to_gateway(url, job='batch_training', registry=registry)
        
if __name__ == "__main__":
    batch_training_pipeline()