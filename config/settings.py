import os

# API
API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
LATITUDE = 28.67   
LONGITUDE = 77.22
TIMEZONE = "Asia/Kolkata"

# Local paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CURATED_DIR = os.path.join(BASE_DIR, "data", "curated")
WATERMARK_FILE = os.path.join(BASE_DIR, "watermark", "watermark.json")

# Watermark default
DEFAULT_WATERMARK = "2024-01-01"