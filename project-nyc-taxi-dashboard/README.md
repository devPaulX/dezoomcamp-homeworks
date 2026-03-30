
⭐ Built using Bruin for ingestion, transformation, and orchestration

# NYC Taxi Data Pipeline & Dashboard 🚕📊

## Problem Statement

Build an end-to-end batch data pipeline to analyze NYC taxi trip patterns and revenue distribution using January 2024 data.

---

## Architecture

```
Raw Data (Parquet + CSV)
        ↓
Ingestion (Bruin)
        ↓
DuckDB Warehouse
        ↓
Staging (cleaned trips)
        ↓
Marts (aggregations)
        ↓
Streamlit Dashboard (2 tiles)
```

---

## Dataset

* Yellow Taxi: January 2024
* Taxi Zone Lookup

---

## Pipeline

### Ingestion

* Load raw parquet and CSV into DuckDB

### Transformation

* Clean and filter invalid trips
* Standardize schema

### Marts

* Trips by pickup zone
* Revenue by pickup zone

---

## Dashboard

### Tile 1 — Trip Volume

Top 10 pickup zones by total trips

### Tile 2 — Revenue

Top 10 pickup zones by total revenue

---

## Results

* Raw trips: **2,964,624**
* After filtering: **2,872,021**

---

## How to Run

```bash
cd project-nyc-taxi-dashboard

bruin run pipeline --config-file .bruin.yml
python -m streamlit run dashboard.py
```

---

## Screenshots

### Pipeline Run

![pipeline](screenshots/pipeline.png)

### Dashboard

![dashboard](screenshots/dashboard.png)

---

## Tech Stack

* Bruin (orchestration + transformations)
* DuckDB (warehouse)
* Streamlit (dashboard)
* Python

---

## Key Learnings

* Built a complete batch data pipeline
* Used DuckDB for fast local analytics
* Integrated orchestration + dashboarding
* Designed a reproducible data workflow
