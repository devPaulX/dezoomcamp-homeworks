/* @bruin
name: ingestion.zones
type: duckdb.sql
materialization:
  type: table
@bruin */

SELECT *
FROM read_csv_auto('data/raw/taxi_zone_lookup.csv', header = true);