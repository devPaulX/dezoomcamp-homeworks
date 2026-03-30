/* @bruin
name: marts.revenue_by_pickup_zone
type: duckdb.sql
materialization:
  type: table
depends:
  - staging.stg_trips
  - ingestion.zones
@bruin */

SELECT
    z.Zone AS pickup_zone,
    z.Borough AS pickup_borough,
    ROUND(SUM(t.total_amount), 2) AS total_revenue
FROM staging.stg_trips t
JOIN ingestion.zones z
    ON t.pickup_location_id = z.LocationID
GROUP BY 1, 2
ORDER BY total_revenue DESC;