CREATE OR REPLACE TABLE `zoomcamp-project-486310.zoomcamp.yellow_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `zoomcamp-project-486310.zoomcamp.yellow_external`;
