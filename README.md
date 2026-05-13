# Incremental Weather ETL Pipeline

A production-grade, containerized Data Engineering ETL pipeline that incrementally ingests, transforms, and loads weather data using **Apache Airflow**, **Apache Spark (PySpark)**, and **Docker**.

## 🏗️ Architecture & Technologies

* **Orchestration:** Apache Airflow (Dockerized)
* **Processing Framework:** Apache Spark / PySpark
* **Data Source:** Open-Meteo Historical Weather API (Free, no auth required)
* **Storage Format:** Parquet (Partitioned by Year, Month, Day)
* **Environment:** Fully Containerized (Custom Airflow + Java/PySpark image)

## ⚙️ How It Works

The pipeline is designed to run on a daily schedule and process data incrementally to optimize performance and avoid reprocessing historical data.

1. **Watermark Management (`watermark.json`)**: The pipeline tracks the `last_processed_date`. 
2. **Ingestion (`scripts/ingest.py`)**: Reads the watermark, calculates the next 7-day batch window, and makes API calls to Open-Meteo to fetch the raw hourly weather data. The raw data is saved locally as `.json`.
3. **Transformation (`scripts/transform.py`)**: 
   * A PySpark job reads the new raw JSON data.
   * Flattens the parallel array structures into relational rows.
   * Filters the data strictly to the current 7-day batch window.
   * Deduplicates records.
   * Performs Data Quality (DQ) checks (e.g., Temperature bounds, Humidity bounds, negative wind speeds).
   * Writes the curated data as partitioned Parquet files.
   * Updates the watermark.
4. **Orchestration (`dags/weather_etl_dag.py`)**: Airflow DAG handles the dependencies ensuring ingestion completes successfully before transformation begins.

## 🚀 Getting Started

Because this project is fully containerized, you do not need to install Spark, Java, or Airflow on your local machine. You only need **Docker**.

### Prerequisites
* Docker Desktop installed and running.

### 1. Start the Environment
Open your terminal in the root of the project and run:
```bash
docker compose up -d --build
```
*Note: The first run will take a few minutes as it downloads the Airflow image and installs OpenJDK and PySpark.*

### 2. Access Airflow
Once the containers are running, navigate to:
* **URL:** http://localhost:8081
* **Username:** `admin`
* **Password:** `admin`

### 3. Run the Pipeline
1. In the Airflow UI, locate the `incremental_weather_etl` DAG.
2. Toggle the switch to "Unpause" the DAG.
3. Click the "Trigger DAG" (Play) button to start a run.
4. Watch the `ingest_raw_data` and `transform_and_load_data` tasks execute successfully!

## 📁 Directory Structure
```text
├── config/              # Configuration files (API URLs, paths)
├── dags/                # Airflow DAGs (weather_etl_dag.py)
├── data/
│   ├── raw/             # Landing zone for raw JSON files
│   └── curated/         # Cleaned, partitioned Parquet files
├── scripts/             # Python scripts for Ingest and PySpark Transform
├── watermark/           # JSON file tracking incremental load state
├── Dockerfile           # Custom Airflow image with Java/PySpark
├── docker-compose.yaml  # Docker services configuration
└── requirements.txt     # Python dependencies
```
