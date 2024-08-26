import os
import json
import pickle
import datetime
import pandas as pd
from kafka import KafkaConsumer
from minio import Minio
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.metrics import mean_squared_error
from prometheus_client import Gauge, Counter, push_to_gateway, CollectorRegistry

registry = CollectorRegistry()
rmse_gauge = Gauge('eval_rmse', 'Root Mean Squared Error', ['model_datestamp'], registry=registry)
mape_gauge = Gauge('eval_mape', 'Mean Absolute Percentage Error', ['model_datestamp'], registry=registry)

def evaluation_pipeline(model_name):
    KAFKA_BROKER = os.getenv("KAFKA_BROKER")
    SOURCE_TOPIC = os.getenv("SOURCE_TOPIC")
    MINIO_URL = os.getenv("MINIO_URL")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_MODEL_BUCKET = os.getenv("MINIO_MODEL_BUCKET")
    MINIO_MODEL_DIRECTORY = os.getenv("MINIO_MODEL_DIRECTORY")
    PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")
    
    # Download specified model from Minio
    minio_client = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    model_timestamp = minio_client.stat_object(MINIO_MODEL_BUCKET, MINIO_MODEL_DIRECTORY+"/"+model_name).last_modified
    timestamp_str = model_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    model_obj = minio_client.get_object(MINIO_MODEL_BUCKET, MINIO_MODEL_DIRECTORY+"/"+model_name)
    model = pickle.loads(model_obj.read())
    
    # Download records from Kafka
    consumer = KafkaConsumer(SOURCE_TOPIC,
                             bootstrap_servers=KAFKA_BROKER,
                             auto_offset_reset='earliest',
                             value_deserializer=json.loads)    
                             
    records = []
    current_timestamp_ms = int(datetime.datetime.now().timestamp() * 1000)
    model_timestamp_ms = int(model_timestamp.timestamp() * 1000)
    for msg in consumer:
        if msg.timestamp > current_timestamp_ms:
            break
        elif msg.timestamp > model_timestamp_ms:
            records.append(next(consumer))
    consumer.close()
    
    columns = ['topic', 'partition', 'offset', 'timestamp', 'col4', 'key', 'data', 'col7', 'col8', 'col9', 'col10', 'col11']
    df = pd.DataFrame.from_records(records, columns=columns).sort_values(by='timestamp', ascending=False).drop_duplicates(subset='key', keep='first').reset_index(drop=True)   
    df = pd.DataFrame.from_records(df['data'])
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    
    # Generate predictions on records
    y_pred = model.predict(df.drop('price', axis=1))
    y_true = df['price']
    
    # Calculate metrics
    rmse = mean_squared_error(y_true, y_pred, squared=False)
    mape = mean_absolute_percentage_error(y_true, y_pred)
    
    # Push to gateway with labels
    rmse_gauge.labels(model_datestamp=timestamp_str).set(rmse)
    mape_gauge.labels(model_datestamp=timestamp_str).set(mape)
    push_to_gateway(PUSHGATEWAY_URL, job='evaluation', registry=registry)
    