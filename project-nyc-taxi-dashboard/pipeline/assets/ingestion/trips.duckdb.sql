/* @bruin
name: ingestion.trips
type: duckdb.sql
materialization:
  type: table
@bruin */

SELECT *
FROM read_parquet('data/raw/{{ var.taxi_type }}_tripdata_{{ var.year }}-{{ var.month }}.parquet');