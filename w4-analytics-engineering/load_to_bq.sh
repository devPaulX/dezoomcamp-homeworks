#!/bin/bash
BUCKET=dezoomcamp-hw3-subhamay-2026
DATASET=zoomcamp

# Yellow + Green (2019–2020)
for year in 2019 2020; do
  for month in $(seq -w 1 12); do
    bq load --source_format=PARQUET \
      $DATASET.yellow_tripdata_${year}_${month} \
      gs://$BUCKET/yellow_tripdata_${year}-${month}.parquet

    bq load --source_format=PARQUET \
      $DATASET.green_tripdata_${year}_${month} \
      gs://$BUCKET/green_tripdata_${year}-${month}.parquet
  done
done

# FHV (2019)
for month in $(seq -w 1 12); do
  bq load --source_format=PARQUET \
    $DATASET.fhv_tripdata_2019_${month} \
    gs://$BUCKET/fhv_tripdata_2019-${month}.parquet
done
