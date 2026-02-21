import os
import requests
import duckdb
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden

# GCS setup
BUCKET_NAME = "dezoomcamp-hw3-subhamay-2026"
CREDENTIALS_FILE = "gcs.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# Schema contracts
SCHEMA_YELLOW = {
    "VendorID": "INT",
    "tpep_pickup_datetime": "TIMESTAMP",
    "tpep_dropoff_datetime": "TIMESTAMP",
    "passenger_count": "INT",
    "trip_distance": "DOUBLE",
    "RatecodeID": "INT",
    "store_and_fwd_flag": "STRING",
    "PULocationID": "INT",
    "DOLocationID": "INT",
    "payment_type": "INT",
    "fare_amount": "DOUBLE",
    "extra": "DOUBLE",
    "mta_tax": "DOUBLE",
    "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "improvement_surcharge": "DOUBLE",
    "total_amount": "DOUBLE",
    "congestion_surcharge": "DOUBLE"
}

SCHEMA_GREEN = {
    "VendorID": "INT",
    "lpep_pickup_datetime": "TIMESTAMP",
    "lpep_dropoff_datetime": "TIMESTAMP",
    "store_and_fwd_flag": "STRING",
    "RatecodeID": "INT",
    "PULocationID": "INT",
    "DOLocationID": "INT",
    "passenger_count": "INT",
    "trip_distance": "DOUBLE",
    "fare_amount": "DOUBLE",
    "extra": "DOUBLE",
    "mta_tax": "DOUBLE",
    "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "ehail_fee": "STRING",   # forced to STRING
    "improvement_surcharge": "DOUBLE",
    "total_amount": "DOUBLE",
    "payment_type": "INT",
    "trip_type": "INT",
    "congestion_surcharge": "DOUBLE"
}

SCHEMA_FHV = {
    "dispatching_base_num": "STRING",
    "pickup_datetime": "TIMESTAMP",
    "dropoff_datetime": "TIMESTAMP",
    "PUlocationID": "INT",
    "DOlocationID": "INT",
    "SR_Flag": "INT",
    "affiliated_base_number": "STRING"
}

SCHEMAS = {
    "yellow": SCHEMA_YELLOW,
    "green": SCHEMA_GREEN,
    "fhv": SCHEMA_FHV
}

def create_bucket(bucket_name):
    try:
        client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' exists. Proceeding...")
    except NotFound:
        client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket '{bucket_name}' exists but not accessible.")
        raise

def convert_and_upload(dataset, year, month, out_dir="data"):
    Path(out_dir).mkdir(exist_ok=True, parents=True)
    csv_gz = Path(out_dir) / f"{dataset}_tripdata_{year}-{month:02d}.csv.gz"
    parquet = Path(out_dir) / f"{dataset}_tripdata_{year}-{month:02d}.parquet"

    # Download CSV.gz
    url = f"{BASE_URL}/{dataset}/{csv_gz.name}"
    print(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(csv_gz, "wb") as f:
        for chunk in response.iter_content(8192):
            f.write(chunk)

    # Convert to Parquet with enforced schema
    schema = SCHEMAS[dataset]
    con = duckdb.connect()
    cols = ", ".join([f"CAST({col} AS {dtype}) AS {col}" for col, dtype in schema.items()])
    query = f"""
        COPY (
            SELECT {cols}
            FROM read_csv_auto('{csv_gz}', compression='gzip')
        ) TO '{parquet}' (FORMAT PARQUET)
    """
    con.execute(query)
    con.close()
    print(f"Converted {csv_gz.name} → {parquet.name}")

    # Upload to GCS
    blob = bucket.blob(parquet.name)
    print(f"Uploading {parquet} to GCS...")
    blob.upload_from_filename(parquet)
    print(f"Uploaded: gs://{BUCKET_NAME}/{parquet.name}")

    # Cleanup local files
    csv_gz.unlink()
    parquet.unlink()
    print(f"Cleaned up local files for {dataset} {year}-{month:02d}")

if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    # Dynamic input
    datasets = input("Enter datasets (comma-separated, e.g. yellow,green,fhv or leave empty for all): ").split(",")
    datasets = [d.strip() for d in datasets if d.strip()] or ["yellow", "green", "fhv"]

    years = input("Enter years (comma-separated, e.g. 2019,2020): ").split(",")
    years = [int(y.strip()) for y in years if y.strip()]

    months = input("Enter months (comma-separated, e.g. 1,2,3 or leave empty for all): ").split(",")
    months = [int(m.strip()) for m in months if m.strip()] or list(range(1, 13))

    filename = input("Enter specific filename (optional, e.g. green_tripdata_2019-01.csv.gz): ").strip()

    with ThreadPoolExecutor(max_workers=8) as executor:
        if filename:
            # Parse dataset/year/month from filename
            parts = filename.split("_")
            dataset = parts[0]
            year, month = parts[2].split("-")
            executor.submit(convert_and_upload, dataset, int(year), int(month))
        else:
            for dataset in datasets:
                for year in years:
                    for month in months:
                        executor.submit(convert_and_upload, dataset, year, month)
