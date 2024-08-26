import os
import sys
import subprocess

required_vars = ['KAFKA_BROKER', 'SOURCE_TOPIC', 'DESTINATION_TOPIC', 'MONGODB_HOST', 'MONGODB_USERNAME', 'MONGODB_PASSWORD', 'MONGODB_DATABASE', 'REPLAY_MESSAGES']
missing_vars = [var for var in required_vars if var not in os.environ]

if missing_vars:
    print(f"Missing environment variables: {', '.join(missing_vars)}")
    sys.exit(1)
    
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC")
DESTINATION_TOPIC = os.getenv("DESTINATION_TOPIC")
MONGODB_HOST = os.getenv("MONGODB_HOST")
MONGODB_USERNAME = os.getenv("MONGODB_USERNAME")
MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
REPLAY_MESSAGES = os.getenv("REPLAY_MESSAGES")

batch_command = f"KAFKA_BROKER={KAFKA_BROKER} SOURCE_TOPIC={SOURCE_TOPIC} DESTINATION_TOPIC={DESTINATION_TOPIC} MONGODB_DATABASE={MONGODB_DATABASE} MONGODB_USERNAME={MONGODB_USERNAME} MONGODB_PASSWORD={MONGODB_PASSWORD} MONGODB_HOST={MONGODB_HOST} REPLAY_MESSAGES={REPLAY_MESSAGES} python3 -m bytewax.run "

if REPLAY_MESSAGES == '1':
    # If we want message replay, we run the bytewax module in recovery mode with the -r flag
    batch_command += "-r recovery/ etl_pipeline:flow"
else:
    # If we don't want message replay, we can simply run the bytewax module
    batch_command += 'etl_pipeline:flow'

try:
    subprocess.run(batch_command, shell=True, check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running etl_pipeline: {e}")
    sys.exit(1)