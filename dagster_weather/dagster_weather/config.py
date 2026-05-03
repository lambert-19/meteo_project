"""Configuration constants for the weather pipeline."""

import os
from typing import Dict

CITIES: Dict[str, Dict[str, float]] = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Lyon": {"lat": 45.7640, "lon": 4.8357},
    "Marseille": {"lat": 43.2965, "lon": 5.3698}
}

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5"
OPENWEATHER_API_KEY = os.getenv("API_KEY")

WEATHER_FIELDS = [
    "temp",           
    "temp_min",        
    "temp_max",       
    "pressure",       
    "humidity"        
]


DATABASE_PATH = "data/meteo.duckdb"

DIM_CITIES_TABLE = "dim_cities"
DIM_CALENDAR_TABLE = "dim_calendar"
FCT_WEATHER_TABLE = "fct_weather_history"

DEFAULT_DAYS_BACK = 30
SCHEDULE_TIME = "09:00"

COLLECTION_STRATEGY = "current_weather"
