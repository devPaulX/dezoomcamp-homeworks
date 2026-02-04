# 🧱 Data Engineering Zoomcamp – Homework 2: Workflow Orchestration

This README contains answers to the homework questions for Week 2 of the Data Engineering Zoomcamp.  
Evidence is provided via screenshots stored in the `screenshots/` folder.


# Week 2 Homework – Workflow Orchestration

This README contains answers to the homework questions for Week 2 of the Data Engineering Zoomcamp.  
Evidence is provided via screenshots stored in the `screenshots/` folder, and SQL queries are stored in `queries/week2_queries.sql`.

---

## Q1. Uncompressed File Size
**Question:** Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size?  
**Answer:** 128.3 MiB
**Evidence:** ![Q1 Screenshot](screenshots/q1_uncompressed_size.png)

---

## Q2. Rendered Variable Value
**Question:** What is the rendered value of the variable `file` when taxi=green, year=2020, month=04?  
**Answer:** `green_tripdata_2020-04.csv`  
**Evidence:** ![Q2 Screenshot](screenshots/q2_rendered_variable.png)

---

## Q3. Yellow Taxi 2020 Row Count
**Question:** How many rows are there for the Yellow Taxi data for all CSV files in 2020?  
**Answer:** 24,648,499 rows  
**Query:** See `queries/week2_queries.sql` → `yellow_tripdata_2020_count.sql`  
**Evidence:** ![Q3 Screenshot](screenshots/q3_yellow_2020_count.png)

---

## Q4. Green Taxi 2020 Row Count
**Question:** How many rows are there for the Green Taxi data for all CSV files in 2020?  
**Answer:** 1,734,051 rows  
**Query:** See `queries/week2_queries.sql` → `green_tripdata_2020_count.sql`  
**Evidence:** ![Q4 Screenshot](screenshots/q4_green_2020_count.png)

---

## Q5. Yellow Taxi March 2021 Row Count
**Question:** How many rows are there for the Yellow Taxi data for March 2021?  
**Answer:** 1,925,152 rows  
**Query:** See `queries/week2_queries.sql` → `yellow_tripdata_2021_03_count.sql`  
**Evidence:** ![Q5 Screenshot](screenshots/q5_yellow_march2021_count.png)

---

## Q6. Schedule Trigger Timezone
**Question:** How would you configure the timezone to New York in a Schedule trigger?  
**Answer:** Add a `timezone` property set to `America/New_York` in the Schedule trigger configuration.  
**Snippet:**
```yaml
triggers:
  - id: daily_schedule
    type: io.kestra.core.models.triggers.types.Schedule
    cron: "0 10 * * *"
    timezone: America/New_York

