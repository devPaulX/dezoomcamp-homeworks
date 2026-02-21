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

# Base URL for taxi data
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# Datasets to process
DATASETS = {
    "yellow": {"years": [2019, 2020]},
    "green": {"years": [2019, 2020]},
    "fhv": {"years": [2019]}   # ✅ included now
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

def convert_to_parquet(dataset, year, month):
    csv_gz = f"{dataset}_tripdata_{year}-{month:02d}.csv.gz"
    parquet = f"{dataset}_tripdata_{year}-{month:02d}.parquet"

    # Download CSV.gz
    url = f"{BASE_URL}/{dataset}/{csv_gz}"
    print(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(csv_gz, "wb") as f:
        for chunk in response.iter_content(8192):
            f.write(chunk)

    # Convert to Parquet using DuckDB (strict parsing)
    con = duckdb.connect()
    con.execute(f"""
        COPY (SELECT * FROM read_csv_auto('{csv_gz}'))
        TO '{parquet}' (FORMAT PARQUET)
    """)
    con.close()

    os.remove(csv_gz)  # save space
    print(f"Converted {csv_gz} → {parquet}")
    return parquet

def upload_to_gcs(file_path):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    print(f"Uploading {file_path} to GCS...")
    blob.upload_from_filename(file_path)
    print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

if __name__ == "__main__":
    create_bucket(BUCKET_NAME)
    months = [f"{i:02d}" for i in range(1, 13)]

    parquet_files = []
    for dataset, cfg in DATASETS.items():
        for year in cfg["years"]:
            for month in months:
                parquet = convert_to_parquet(dataset, year, int(month))
                parquet_files.append(parquet)

    # Upload all Parquet files to GCS
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, parquet_files)

    print("All Parquet files processed and uploaded.")
