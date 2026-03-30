import json
import time

import pandas as pd
from kafka import KafkaProducer

FILE_PATH = "green_tripdata_2025-10.parquet"
TOPIC_NAME = "green-trips"
BOOTSTRAP_SERVERS = "localhost:9092"

COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]

def json_serializer(record):
    return json.dumps(record).encode("utf-8")

df = pd.read_parquet(FILE_PATH, columns=COLUMNS)

# convert timestamps to strings so JSON can handle them cleanly
for col in ["lpep_pickup_datetime", "lpep_dropoff_datetime"]:
    df[col] = df[col].astype(str)

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=json_serializer
)

t0 = time.time()

for _, row in df.iterrows():
    producer.send(TOPIC_NAME, value=row.to_dict())

producer.flush()

t1 = time.time()
print(f"sent {len(df)} records")
print(f"took {t1 - t0:.2f} seconds")