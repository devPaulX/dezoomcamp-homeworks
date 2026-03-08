"""@bruin

name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: fare_amount
    type: double
  - name: payment_type
    type: integer
  - name: taxi_type
    type: string

@bruin"""

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def generate_months(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    months = []
    current = start

    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    return months


def materialize():
    import os
    import json

    start = os.environ["BRUIN_START_DATE"]
    end = os.environ["BRUIN_END_DATE"]

    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    months = generate_months(start, end)

    frames = []

    for taxi in taxi_types:
        for year, month in months:

            file = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
            url = f"{BASE_URL}/{file}"

            try:
                print("Downloading:", url)

                df = pd.read_parquet(url)

                df = df.rename(columns={
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                    "PULocationID": "pickup_location_id",
                    "DOLocationID": "dropoff_location_id"
                })

                df["taxi_type"] = taxi

                frames.append(df)

                print(f"Successfully ingested: {file}")

            except Exception as e:
                print(f"SKIPPING: Could not download {file}. Error: {e}")

    return pd.concat(frames) if frames else pd.DataFrame()