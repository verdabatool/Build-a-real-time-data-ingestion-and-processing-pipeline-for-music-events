# producer.py
import time
import random
import json
from kafka import KafkaProducer

# pip install kafka-python

TOPIC = "music_events"
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

songs = [101, 202, 303, 404, 505]   # sample song IDs
regions = ["US", "EU", "APAC"]

actions = ["play", "skip", "like"]  # New actions added

while True:
    event = {
        "song_id": random.choice(songs),
        "timestamp": time.time(),
        "region": random.choice(regions),
        "action": random.choice(actions)  # Randomly pick an action
    }
    producer.send(TOPIC, event)
    print(f"Sent event: {event}")
    time.sleep(random.uniform(0.5, 2.0))  # random interval

