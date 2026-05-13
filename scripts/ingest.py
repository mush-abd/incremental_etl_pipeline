import requests
import json
import os
from datetime import datetime
from config.settings import API_BASE_URL, LATITUDE, LONGITUDE, TIMEZONE, RAW_DIR


def fetch_weather_data(start_date: str, end_date: str) -> dict:
    """
    Fetches hourly weather data from Open-Meteo API.
    start_date / end_date format: 'YYYY-MM-DD'
    """
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
        "timezone": TIMEZONE
    }

    print(f"[INGEST] Fetching data from {start_date} to {end_date}...")
    response = requests.get(API_BASE_URL, params=params, timeout=30)

    if response.status_code != 200:
        raise Exception(f"API call failed: {response.status_code} — {response.text}")

    data = response.json()
    print(f"[INGEST] Received {len(data['hourly']['time'])} records.")
    return data


def save_raw_data(data: dict, start_date: str) -> str:
    """
    Saves raw JSON to data/raw/ with a timestamped filename.
    Returns the saved file path.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    filename = f"weather_{start_date}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    filepath = os.path.join(RAW_DIR, filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"[INGEST] Raw data saved to: {filepath}")
    return filepath


def run_ingestion(start_date: str, end_date: str) -> str:
    data = fetch_weather_data(start_date, end_date)
    filepath = save_raw_data(data, start_date)
    return filepath


if __name__ == "__main__":
    from scripts.transform import read_watermark, get_next_date_range
    watermark = read_watermark()
    start_date, end_date = get_next_date_range(watermark)
    run_ingestion(start_date, end_date)