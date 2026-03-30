import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")

DB_PATH = "data/warehouse/duckdb.db"

@st.cache_data
def load_data():
    con = duckdb.connect(DB_PATH)

    trips_df = con.execute("""
        SELECT pickup_zone, pickup_borough, total_trips
        FROM marts.trips_by_pickup_zone
        LIMIT 10
    """).fetchdf()

    revenue_df = con.execute("""
        SELECT pickup_zone, pickup_borough, total_revenue
        FROM marts.revenue_by_pickup_zone
        LIMIT 10
    """).fetchdf()

    raw_count = con.execute("SELECT COUNT(*) AS cnt FROM ingestion.trips").fetchone()[0]
    staged_count = con.execute("SELECT COUNT(*) AS cnt FROM staging.stg_trips").fetchone()[0]

    con.close()
    return trips_df, revenue_df, raw_count, staged_count

trips_df, revenue_df, raw_count, staged_count = load_data()

trips_df = trips_df.sort_values("total_trips", ascending=False)
revenue_df = revenue_df.sort_values("total_revenue", ascending=False)

st.title("NYC Taxi Trips Dashboard")
st.caption("Batch pipeline built with Bruin + DuckDB on NYC Yellow Taxi January 2024 data")

c1, c2 = st.columns(2)

with c1:
    st.subheader("Top 10 Pickup Zones by Total Trips")
    st.bar_chart(
        trips_df.set_index("pickup_zone")["total_trips"]
    )
    st.dataframe(trips_df, use_container_width=True)

with c2:
    st.subheader("Top 10 Pickup Zones by Total Revenue")
    st.bar_chart(
        revenue_df.set_index("pickup_zone")["total_revenue"]
    )
    st.dataframe(revenue_df, use_container_width=True)

st.divider()

m1, m2 = st.columns(2)
with m1:
    st.metric("Raw Trips Loaded", f"{raw_count:,}")
with m2:
    st.metric("Trips After Staging Filters", f"{staged_count:,}")