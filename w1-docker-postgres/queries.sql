-- Q3: Count of trips with distance <= 1 mile in Nov 2025
SELECT COUNT(*)
FROM green_tripdata
WHERE trip_distance <= 1
  AND lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01';

-- Q4: Longest trip day (<100 miles)
SELECT lpep_pickup_datetime::date AS day, MAX(trip_distance) AS max_distance
FROM green_tripdata
WHERE trip_distance < 100
GROUP BY day
ORDER BY max_distance DESC
LIMIT 1;

-- Q5: Pickup zone with largest total_amount on Nov 18
SELECT z.zone, SUM(t.total_amount) AS total
FROM green_tripdata t
JOIN taxi_zone_lookup z ON t.pulocationid = z.locationid
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z.zone
ORDER BY total DESC
LIMIT 1;

-- Q6: Drop-off zone with largest tip for pickups in East Harlem North
SELECT z2.zone, MAX(t.tip_amount) AS max_tip
FROM green_tripdata t
JOIN taxi_zone_lookup z1 ON t.pulocationid = z1.locationid
JOIN taxi_zone_lookup z2 ON t.dolocationid = z2.locationid
WHERE z1.zone = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime < '2025-12-01'
GROUP BY z2.zone
ORDER BY max_tip DESC
LIMIT 1;
