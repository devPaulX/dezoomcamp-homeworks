from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
PARQUET_PATH = str(DATA_DIR / "yellow_tripdata_2025-11.parquet")
ZONE_PATH = str(DATA_DIR / "taxi_zone_lookup.csv")
REPARTITIONED_PATH = str(OUTPUT_DIR / "yellow_2025_11_repartitioned")

def bytes_to_mb(num_bytes: int) -> float:
    return num_bytes / (1024 * 1024)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("zoomcamp-module6-homework")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Q1. Spark Version")
print("=" * 80)
print("Spark version:", spark.version)

print("\n" + "=" * 80)
print("Load Yellow Taxi November 2025")
print("=" * 80)
df = spark.read.parquet(PARQUET_PATH)
df.printSchema()
print("Initial partitions:", df.rdd.getNumPartitions())

print("\n" + "=" * 80)
print("Q2. Repartition to 4 and save parquet")
print("=" * 80)
df_4 = df.repartition(4)
print("Repartitioned partitions:", df_4.rdd.getNumPartitions())

# overwrite if rerunning
df_4.write.mode("overwrite").parquet(REPARTITIONED_PATH)

parquet_files = list(Path(REPARTITIONED_PATH).glob("*.parquet"))
sizes_mb = [bytes_to_mb(p.stat().st_size) for p in parquet_files]

print("Created parquet files:")
for p, s in zip(parquet_files, sizes_mb):
    print(f"{p.name}: {s:.2f} MB")

avg_size_mb = sum(sizes_mb) / len(sizes_mb)
print(f"Average parquet file size: {avg_size_mb:.2f} MB")

print("\n" + "=" * 80)
print("Q3. Count trips starting on 2025-11-15")
print("=" * 80)

# Adjust column names if needed after printSchema()
pickup_col = "tpep_pickup_datetime"
dropoff_col = "tpep_dropoff_datetime"

q3_count = (
    df.filter(F.to_date(F.col(pickup_col)) == F.lit("2025-11-15"))
      .count()
)
print("Trips with pickup date 2025-11-15:", q3_count)

print("\n" + "=" * 80)
print("Q4. Longest trip in hours")
print("=" * 80)

duration_df = (
    df.filter(F.col(dropoff_col).isNotNull() & F.col(pickup_col).isNotNull())
      .filter(F.col(dropoff_col) > F.col(pickup_col))
      .withColumn(
          "trip_duration_hours",
          (
              F.unix_timestamp(F.col(dropoff_col)) -
              F.unix_timestamp(F.col(pickup_col))
          ) / 3600.0
      )
)

# duration_df = df.withColumn(
#     "trip_duration_hours",
#     (F.col(dropoff_col).cast("long") - F.col(pickup_col).cast("long")) / 3600.0
# )

q4_max = (
    duration_df
    .agg(F.max("trip_duration_hours").alias("max_hours"))
    .collect()[0]["max_hours"]
)
# q4_max = duration_df.select(F.max("trip_duration_hours")).first()[0] # type: ignore

print(f"Longest trip duration: {q4_max:.2f} hours")

print("\n" + "=" * 80)
print("Q5. Spark UI")
print("=" * 80)
print("Spark UI URL:", spark.sparkContext.uiWebUrl)

print("\n" + "=" * 80)
print("Q6. Least frequent pickup location zone")
print("=" * 80)

zones = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(ZONE_PATH)
)

zones.printSchema()

pickup_counts = (
    df.groupBy("PULocationID")
      .count()
      .join(zones, F.col("PULocationID") == F.col("LocationID"), "left")
      .select("PULocationID", "Zone", "count")
      .orderBy(F.col("count").asc(), F.col("Zone").asc())
)

pickup_counts.show(20, truncate=False)

least_row = pickup_counts.first()
print("Least frequent pickup zone:", least_row["Zone"]) # type: ignore
print("Pickup count:", least_row["count"]) # type: ignore

spark.stop()