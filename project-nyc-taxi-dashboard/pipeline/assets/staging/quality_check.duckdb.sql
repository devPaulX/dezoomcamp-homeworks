/* @bruin
name: staging.quality_check
type: duckdb.sql
materialization:
  type: table
depends:
  - staging.stg_trips
@bruin */

SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN pickup_datetime IS NULL THEN 1 ELSE 0 END) AS null_pickups,
    SUM(CASE WHEN dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS null_dropoffs,
    SUM(CASE WHEN trip_distance <= 0 THEN 1 ELSE 0 END) AS invalid_distance,
    SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) AS invalid_amount
FROM staging.stg_trips;