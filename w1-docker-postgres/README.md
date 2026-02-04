# 🧱 Data Engineering Zoomcamp – Homework 1: Docker & Postgres

This README contains answers to the homework questions for Week 1 of the Data Engineering Zoomcamp.  
Evidence is provided via screenshots stored in the `screenshots/` folder.

## ✅ Setup Summary

- Docker Compose used to spin up:
  - PostgreSQL 13
  - pgAdmin 4
- Dataset: `green_tripdata_2025-11.parquet` → converted to CSV
- Table: `green_tripdata` with 21 columns

# 🚀 Module 1 Homework: Docker & SQL + Terraform

This repository contains my solutions for **Data Engineering Zoomcamp – Module 1 Homework**.  
It covers Docker basics, SQL queries on NYC Taxi data, and Terraform workflow.

---

## 🐳 Q1. Understanding Docker Images
What's the version of pip in the python:3.13 image?

Run Python 3.13 image with bash:
```bash
docker run -it --entrypoint bash python:3.13
pip --version
Answer: pip 25.3
```

🐳 Q2. Docker Networking & docker-compose
Given the docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?

Hostname: db
Port: 5432
Answer: db:5432

🚖 Q3. Counting Short Trips
For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile?
SQL:

```sql
SELECT COUNT(*)
FROM green_tripdata
WHERE trip_distance <= 1
  AND lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01';
Answer: 8,007
```

🚖 Q4. Longest Trip for Each Day
Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles.
SQL:

```sql
SELECT lpep_pickup_datetime::date AS day, MAX(trip_distance) AS max_distance
FROM green_tripdata
WHERE trip_distance < 100
GROUP BY day
ORDER BY max_distance DESC
LIMIT 1;
Answer: 2025-11-14
```

🚖 Q5. Biggest Pickup Zone (Nov 18, 2025)
Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?
SQL:

```sql
SELECT z.zone, SUM(t.total_amount) AS total
FROM green_tripdata t
JOIN taxi_zone_lookup z ON t.pulocationid = z.locationid
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z.zone
ORDER BY total DESC
LIMIT 1;
Answer: East Harlem North (Total ≈ 9281.92)
```

🚖 Q6. Largest Tip (East Harlem North pickups)
For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip? 
SQL:

```sql
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
Answer: Yorkville West (Max Tip ≈ 81.89)
```

☁️ Q7. Terraform Workflow
Which of the following sequences describes the Terraform workflow for: 1) Downloading plugins and setting up backend, 2) Generating and executing changes, 3) Removing all resources?

Correct sequence:

```bash
terraform init
terraform apply -auto-approve
terraform destroy

Answer: terraform init, terraform apply -auto-approve, terraform destroy
```

📂 Repo Structure

Module-1-Docker-SQL-Terraform/
├── docker-compose.yaml
├── datasets/
│   ├── green_tripdata_2025-11.csv
│   └── taxi_zone_lookup.csv
├── queries.sql
├── README.md
└── screenshots/

📝 Notes
All SQL queries are included in queries.sql.

Screenshots of pgAdmin query results are in screenshots/.

Terraform configs are adapted from course repo to create a GCP bucket and BigQuery dataset.

