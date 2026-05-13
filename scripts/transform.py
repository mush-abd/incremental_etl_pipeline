import json
import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth

from config.settings import RAW_DIR, CURATED_DIR, WATERMARK_FILE, DEFAULT_WATERMARK


import platform

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if platform.system() == "Windows":
    # Project-local JDK -- no system Java needed on Windows
    JAVA_HOME = os.path.join(BASE_DIR, "jdk17")
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = (
        os.path.join(JAVA_HOME, "bin") + ";"
        + r"C:\hadoop\bin" + ";"
        + os.environ["PATH"]
    )

# ── Watermark helpers ────────────────────────────────────────────────────────

def read_watermark() -> str:
    """Reads last processed date from watermark file."""
    if not os.path.exists(WATERMARK_FILE):
        print(f"[WATERMARK] No watermark found. Using default: {DEFAULT_WATERMARK}")
        return DEFAULT_WATERMARK

    with open(WATERMARK_FILE, "r") as f:
        wm = json.load(f)
    print(f"[WATERMARK] Last processed date: {wm['last_processed_date']}")
    return wm["last_processed_date"]


def update_watermark(new_date: str):
    """Updates watermark file with the latest processed date."""
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_processed_date": new_date}, f, indent=2)
    print(f"[WATERMARK] Updated watermark to: {new_date}")


def get_next_date_range(watermark_date: str):
    """
    Returns (start_date, end_date) for the next batch.
    Loads 7 days ahead of the watermark.
    """
    start = datetime.strptime(watermark_date, "%Y-%m-%d") + timedelta(days=1)
    end = start + timedelta(days=6)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


# ── Data quality checks ──────────────────────────────────────────────────────

def run_quality_checks(df, stage=""):
    """
    Runs basic DQ checks and prints a report.
    Does NOT silently drop bad data — flags it so you can decide.
    """
    total = df.count()
    print(f"\n[DQ:{stage}] Total records: {total}")

    # Null checks
    for col_name in ["timestamp", "temperature_2m", "relative_humidity_2m",
                     "wind_speed_10m", "precipitation"]:
        null_count = df.filter(col(col_name).isNull()).count()
        status = "OK" if null_count == 0 else "WARN"
        print(f"  [{status}] Nulls in '{col_name}': {null_count}")

    # Range checks
    temp_violations = df.filter(
        (col("temperature_2m") < -90) | (col("temperature_2m") > 60)
    ).count()
    humidity_violations = df.filter(
        (col("relative_humidity_2m") < 0) | (col("relative_humidity_2m") > 100)
    ).count()
    wind_violations = df.filter(col("wind_speed_10m") < 0).count()

    print(f"  [{'OK' if temp_violations == 0 else 'WARN'}] Temperature out of range (-90 to 60 C): {temp_violations}")
    print(f"  [{'OK' if humidity_violations == 0 else 'WARN'}] Humidity out of range (0-100%): {humidity_violations}")
    print(f"  [{'OK' if wind_violations == 0 else 'WARN'}] Negative wind speed: {wind_violations}")

    return df  # returns unchanged — DQ is observational here, not filtering


# ── Core transform ───────────────────────────────────────────────────────────

def flatten_raw_json(spark, filepath: str):
    """
    Open-Meteo returns parallel lists under 'hourly'.
    This flattens them into one row per timestamp.
    """
    with open(filepath, "r") as f:
        raw = json.load(f)

    hourly = raw["hourly"]
    records = []

    for i, ts in enumerate(hourly["time"]):
        records.append({
            "timestamp": ts,
            "temperature_2m": hourly["temperature_2m"][i],
            "relative_humidity_2m": hourly["relative_humidity_2m"][i],
            "wind_speed_10m": hourly["wind_speed_10m"][i],
            "precipitation": hourly["precipitation"][i],
            "latitude": raw["latitude"],
            "longitude": raw["longitude"],
            "timezone": raw["timezone"]
        })

    df = spark.createDataFrame(records)
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm"))
    return df


def run_transform(start_date: str, end_date: str):
    spark = SparkSession.builder \
        .appName("IncrementalETL") \
        .master("local[1]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Find raw files for this batch ────────────────────────────────────
    raw_files = [
        os.path.join(RAW_DIR, f)
        for f in os.listdir(RAW_DIR)
        if f.startswith("weather_") and f.endswith(".json")
    ]

    if not raw_files:
        print(f"[TRANSFORM] No raw files found for {start_date}. Skipping.")
        spark.stop()
        return

    print(f"[TRANSFORM] Processing {len(raw_files)} file(s) for batch: {start_date} -> {end_date}")

    # ── 2. Flatten & union all files for this batch ──────────────────────────
    df = flatten_raw_json(spark, raw_files[0])
    for f in raw_files[1:]:
        df = df.union(flatten_raw_json(spark, f))

    # -- 3. Watermark filter -- only keep records within the batch window ------
    df = df.filter(
        (col("timestamp") >= start_date) & (col("timestamp") <= end_date)
    )
    print(f"[TRANSFORM] Records after watermark filter: {df.count()}")

    # ── 4. Deduplication ─────────────────────────────────────────────────────
    before_dedup = df.count()
    df = df.dropDuplicates(["timestamp", "latitude", "longitude"])
    after_dedup = df.count()
    print(f"[TRANSFORM] Dedup: {before_dedup} -> {after_dedup} records "
          f"({before_dedup - after_dedup} duplicates removed)")

    # ── 5. Data quality checks ───────────────────────────────────────────────
    df = run_quality_checks(df, stage="post-dedup")

    # ── 6. Add partition columns ─────────────────────────────────────────────
    df = df.withColumn("year", year(col("timestamp"))) \
           .withColumn("month", month(col("timestamp"))) \
           .withColumn("day", dayofmonth(col("timestamp")))

    # ── 7. Write curated Parquet partitioned by date ─────────────────────────
    os.makedirs(CURATED_DIR, exist_ok=True)
    output_path = os.path.join(CURATED_DIR, "weather")

    df.write \
      .mode("append") \
      .partitionBy("year", "month", "day") \
      .parquet(output_path)

    print(f"[TRANSFORM] Curated data written to: {output_path}")

    spark.stop()
    return end_date  # return so Airflow / caller can update watermark


if __name__ == "__main__":
    watermark = read_watermark()
    start_date, end_date = get_next_date_range(watermark)
    result = run_transform(start_date, end_date)
    if result:
        update_watermark(end_date)