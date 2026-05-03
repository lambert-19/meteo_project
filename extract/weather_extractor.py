"""Weather data extractor using OpenWeatherMap API."""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import os
from pathlib import Path
import logging
from dotenv import load_dotenv

_env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(_env_path)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherExtractor:
    """Extract weather data from OpenWeatherMap API."""
    
    def __init__(self):
        """Initialize the extractor for Open-Meteo."""
        self.base_url: str = "https://archive-api.open-meteo.com/v1/archive"
        
        
        self.cities: Dict[str, Dict[str, Any]] = {
            "Paris": {"lat": 48.8566, "lon": 2.3522, "elevation": 35, "region": "Île-de-France"},
            "Lyon": {"lat": 45.7640, "lon": 4.8357, "elevation": 173, "region": "Auvergne-Rhône-Alpes"},
            "Marseille": {"lat": 43.2965, "lon": 5.3698, "elevation": 12, "region": "Provence-Alpes-Côte d'Azur"}
        }
        
        
        self.weather_codes = {
            0: "Ciel dégagé",
            1: "Principalement dégagé", 2: "Partiellement nuageux", 3: "Couvert",
            45: "Brouillard", 48: "Givre",
            51: "Bruine légère", 61: "Pluie légère", 71: "Neige légère",
            80: "Averses de pluie", 95: "Orage"
        }
    
    def get_historical_weather(self, city_name: str, target_date: str) -> Optional[List[Dict[str, Any]]]:
        """Get historical weather data for a specific city and date using Open-Meteo."""
        if city_name not in self.cities:
            logger.error(f"City {city_name} not found in configuration")
            return None
            
        coords: Dict[str, Any] = self.cities[city_name]
        
        params: Dict[str, Any] = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": target_date,
            "end_date": target_date,
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "weather_code"],
            "timezone": "Europe/Berlin"
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data: Dict[str, Any] = response.json()
            daily = data.get("daily", {})
            
            records: List[Dict[str, Any]] = []
            for i in range(len(daily.get("time", []))):
                w_code = daily.get("weather_code", [0])[i]
                description = self.weather_codes.get(w_code, f"Code {w_code}")
                
                records.append({
                    "city": city_name,
                    "latitude": coords["lat"],
                    "longitude": coords["lon"],
                    "elevation": coords["elevation"],
                    "region": coords["region"],
                    "date": daily["time"][i],
                    "temperature": (daily["temperature_2m_max"][i] + daily["temperature_2m_min"][i]) / 2,
                    "temp_min": daily["temperature_2m_min"][i],
                    "temp_max": daily["temperature_2m_max"][i],
                    "precipitation_sum": daily["precipitation_sum"][i],
                    "weather_description": description,
                    "wind_speed": 0, # Not in basic daily archive
                    "extracted_at": datetime.now().isoformat()
                })
            
            logger.info(f"Extracted {len(records)} records for {city_name}")
            return records
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data for {city_name}: {e}")
            return None
        except KeyError as e:
            logger.error(f"Unexpected API response format for {city_name}: {e}")
            return None
    
    def extract_all_cities(self, target_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Extract current weather data for all configured cities."""
        all_weather_data: List[Dict[str, Any]] = []
        
        date_to_fetch = target_date or (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        
        logger.info(f"Starting weather data extraction for {date_to_fetch}...")
        
        for city_name in self.cities.keys():
            weather_data = self.get_historical_weather(city_name, date_to_fetch)
            if weather_data:
                all_weather_data.extend(weather_data)
        
        logger.info(f"Extracted data for {len(all_weather_data)} cities")
        return all_weather_data
    
    def to_dataframe(self, weather_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Convert weather data to pandas DataFrame."""
        if not weather_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(weather_data)
        
        # Convert date columns
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        if 'extracted_at' in df.columns:
            df['extracted_at'] = pd.to_datetime(df['extracted_at'])
        
        return df

    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> bool:
        """Save weather data to CSV file."""
        try:
            path = Path(filepath)
          
            path.parent.mkdir(parents=True, exist_ok=True)
            
            df.to_csv(path, index=False, encoding='utf-8')
            logger.info(f"Data saved to {path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save data to {filepath}: {e}")
            return False


def main():
    """Main function for testing the extractor."""
   
    
    
    extractor = WeatherExtractor()
    
    
    weather_data = extractor.extract_all_cities()
    
    if weather_data:
        
        df = extractor.to_dataframe(weather_data)
        
       
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        
        project_root = Path(__file__).resolve().parent.parent
        data_dir = project_root / "data"
        filepath = data_dir / f"weather_data_{timestamp}.csv"
        
        extractor.save_to_csv(df, str(filepath))
        
        
        print("\n=== Weather Data Summary ===")
        print(f"Records extracted: {len(df)}")
        print(f"Cities: {', '.join(df['city'].unique())}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        print("\nSample data:")
        print(df[['city', 'temperature', 'temp_min', 'temp_max', 'precipitation_sum']].head())
        
    else:
        logger.error("No weather data extracted")


if __name__ == "__main__":
    main()
