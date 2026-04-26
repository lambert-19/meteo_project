"""Configuration constants for the weather pipeline."""

import os
from typing import Dict

# French cities with their coordinates
CITIES: Dict[str, Dict[str, float]] = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Lyon": {"lat": 45.7640, "lon": 4.8357},
    "Marseille": {"lat": 43.2965, "lon": 5.3698}
}

# API configuration - OpenWeatherMap
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5"
OPENWEATHER_API_KEY = os.getenv("API_KEY")

# Weather fields to retrieve from OpenWeatherMap
# Current weather API provides: temp, temp_min, temp_max, pressure, humidity, etc.
WEATHER_FIELDS = [
    "temp",           # Current temperature
    "temp_min",       # Minimum temperature 
    "temp_max",       # Maximum temperature
    "pressure",       # Atmospheric pressure
    "humidity"        # Humidity percentage
]

# For historical data, we'll need to use One Call API 3.0 (paid) or collect current data over time
# For MVP, we'll start with current weather data and build historical archive

# Database configuration
DATABASE_PATH = "data/meteo.duckdb"

# Table names
DIM_CITIES_TABLE = "dim_cities"
DIM_CALENDAR_TABLE = "dim_calendar"
FCT_WEATHER_TABLE = "fct_weather_history"

# Pipeline configuration
DEFAULT_DAYS_BACK = 30
SCHEDULE_TIME = "09:00"

# Data collection strategy
COLLECTION_STRATEGY = "current_weather"
