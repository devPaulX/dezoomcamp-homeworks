CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-project-486310.zoomcamp.yellow_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp-hw3-subhamay-2026/*.parquet']
);