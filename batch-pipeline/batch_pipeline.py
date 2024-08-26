import os
import json
import datetime
import pandas as pd
from kafka import KafkaConsumer
from minio import Minio

def batch_pipeline():
    KAFKA_BROKER = os.getenv("KAFKA_BROKER")
    SOURCE_TOPIC = os.getenv("SOURCE_TOPIC")
    MINIO_URL = os.getenv("MINIO_URL")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET")
    MINIO_DIRECTORY = os.getenv("MINIO_DIRECTORY")
    
    consumer = KafkaConsumer(SOURCE_TOPIC,
                             bootstrap_servers=KAFKA_BROKER,
                             auto_offset_reset='earliest',
                             value_deserializer=json.loads)
                             
    records = []
    current_timestamp_ms = int(datetime.datetime.now().timestamp() * 1000)
    for msg in consumer:
        if msg.timestamp > current_timestamp_ms:
            break
        records.append(next(consumer))
    consumer.close()
        
    columns = ['topic', 'partition', 'offset', 'timestamp', 'col4', 'key', 'data', 'col7', 'col8', 'col9', 'col10', 'col11']
    df = pd.DataFrame.from_records(records, columns=columns).sort_values(by='timestamp', ascending=False).drop_duplicates(subset='key', keep='first').reset_index(drop=True)   
    data = pd.DataFrame.from_records(df['data'])
    filename = f"{datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.feather"
    data.to_feather(filename)
    
    minio_client = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    minio_client.fput_object(MINIO_BUCKET, MINIO_DIRECTORY+"/"+filename, filename)
    os.remove(filename)


if __name__ == "__main__":
    batch_pipeline()
    