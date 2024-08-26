import os
import json
from pymongo import MongoClient
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaInput, KafkaOutput
from bytewax.connectors.stdio import StdOutput

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC")
DESTINATION_TOPIC = os.getenv("DESTINATION_TOPIC")
MONGODB_HOST = os.getenv("MONGODB_HOST")
MONGODB_USERNAME = os.getenv("MONGODB_USERNAME")
MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
REPLAY_MESSAGES = os.getenv("REPLAY_MESSAGES") # Not used here

print(f"Connecting to MongoClient at {MONGODB_HOST}")
client = MongoClient(f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_HOST}:27017/")
db = client[MONGODB_DATABASE]

collection_pop = db["zipcode-populations"]
collection_school = db["zipcode-schools"]

print(f"Defining DataFlow from '{SOURCE_TOPIC}' to '{DESTINATION_TOPIC}'")
def enrich(line):
    zip_population = collection_pop.find_one({'zipcode': line['zipcode']})
    zip_schools = collection_school.find_one({'zipcode': line['zipcode']})
    
    line['zip_population'] = zip_population['population'] if zip_population else None
    line['zip_schools'] = zip_schools['TYPE'] if zip_schools else None
    return line

input = KafkaInput(brokers=[KAFKA_BROKER], topics=[SOURCE_TOPIC])
output = KafkaOutput(brokers=[KAFKA_BROKER], topic=DESTINATION_TOPIC)
flow = Dataflow()

flow.input("kafka-in", input)
flow.map(lambda kv: json.loads(kv[1].decode("utf-8")))
flow.filter(lambda msg: 0 < msg['bedrooms'] <= 11)
flow.filter(lambda msg: msg['bathrooms'] > 0)
flow.map(enrich)
flow.filter(lambda msg: msg['zip_population'] != None and msg['zip_schools'] != None)
flow.map(lambda obj: (str(obj['id']).encode("utf-8"), json.dumps(obj).encode("utf-8")))
flow.output("kafka-out", output)

print("Setup complete.")
