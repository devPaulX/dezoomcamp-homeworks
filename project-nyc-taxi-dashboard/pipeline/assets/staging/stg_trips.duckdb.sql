/* @bruin
name: staging.stg_trips
type: duckdb.sql
materialization:
  type: table
depends:
  - ingestion.trips
@bruin */

SELECT
    VendorID AS vendor_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID AS ratecode_id,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee AS airport_fee
FROM ingestion.trips
WHERE
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND PULocationID IS NOT NULL
    AND DOLocationID IS NOT NULL
    AND trip_distance > 0
    AND total_amount > 0;