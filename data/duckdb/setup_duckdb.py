"""Simple setup script for DuckDB database with only necessary tables for Dagster pipeline."""

import os
import duckdb
from datetime import datetime, date
from dotenv import load_dotenv
from typing import List, Tuple

def setup_database_simple():
    """Setup database with only necessary tables for Dagster pipeline."""

    load_dotenv("../../.env")
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")

    if not motherduck_token:
        print("MOTHERDUCK_TOKEN environment variable not set")
        return

    try:
        conn = duckdb.connect(f'md:?motherduck_token={motherduck_token}')
        conn.execute("USE meteo_weather")
        print("Connected to MotherDuck meteo_weather database")
    except Exception as e:
        print(f"Failed to connect to MotherDuck: {e}")
        return

    try:
        print("Creating dim_cities table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_cities (
                city_id INTEGER PRIMARY KEY,
                city_name VARCHAR(100) NOT NULL UNIQUE,
                country VARCHAR(100),
                region VARCHAR(100),
                elevation DECIMAL(10, 2),
                latitude DECIMAL(10, 8),
                longitude DECIMAL(10, 8)
            )
        """)
        print("✓ Created dim_cities table")

        cities_data = [
            (1, 'Paris', 'France', 'Île-de-France', 35, 48.8566, 2.3522),
            (2, 'Lyon', 'France', 'Auvergne-Rhône-Alpes', 173, 45.7640, 4.8357),
            (3, 'Marseille', 'France', "Provence-Alpes-Côte d'Azur", 12, 43.2965, 5.3698),
        ]

        conn.executemany("""
            INSERT INTO dim_cities 
            (city_id, city_name, country, region, elevation, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (city_id) DO UPDATE SET 
                city_name = EXCLUDED.city_name,
                country = EXCLUDED.country,
                region = EXCLUDED.region,
                elevation = EXCLUDED.elevation,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude
        """, cities_data)
        print(f"✓ Inserted {len(cities_data)} cities into dim_cities")

        print("Creating dim_calendar table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_calendar (
                date_id INTEGER PRIMARY KEY,
                date DATE NOT NULL UNIQUE,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                day_of_week INTEGER NOT NULL,
                day_of_year INTEGER NOT NULL,
                week_of_year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                is_weekend BOOLEAN NOT NULL,
                season VARCHAR(20)
            )
        """)
        print("✓ Created dim_calendar table")

        current_year = datetime.now().year
        calendar_data: List[Tuple[int, date, int, int, int, int, int, int, int, bool, str]] = []

        for month in range(1, 13):
            for day in range(1, 32):
                try:
                    date_obj = date(current_year, month, day)
                    date_id = int(date_obj.strftime("%Y%m%d"))

                    day_of_week = date_obj.weekday() + 1  
                    day_of_year = date_obj.timetuple().tm_yday
                    week_of_year = (day_of_year - 1) // 7 + 1
                    quarter = (month - 1) // 3 + 1
                    is_weekend = day_of_week >= 6  

                    if month in [12, 1, 2]:
                        season = "Winter"
                    elif month in [3, 4, 5]:
                        season = "Spring"
                    elif month in [6, 7, 8]:
                        season = "Summer"
                    else:
                        season = "Autumn"

                    calendar_data.append((
                        date_id, date_obj, current_year, month, day,
                        day_of_week, day_of_year, week_of_year, quarter,
                        is_weekend, season
                    ))
                except ValueError:
                    continue

        conn.executemany("""
            INSERT INTO dim_calendar 
            (date_id, date, year, month, day, day_of_week, day_of_year, 
             week_of_year, quarter, is_weekend, season)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
            ON CONFLICT DO NOTHING
        """, calendar_data)
        print(f"✓ Inserted {len(calendar_data)} dates into dim_calendar")

        print("Creating fct_weather_history table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fct_weather_history (
                weather_id INTEGER PRIMARY KEY,
                city_id INTEGER NOT NULL,
                date_id INTEGER NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                temperature DECIMAL(5,2) NOT NULL,
                temp_min DECIMAL(5,2) NOT NULL,
                temp_max DECIMAL(5,2) NOT NULL,
                precipitation_sum DECIMAL(5,2),
                pressure DECIMAL(7,2),
                humidity INTEGER,
                weather_description VARCHAR(100),
                wind_speed DECIMAL(5,2),
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (city_id) REFERENCES dim_cities(city_id),
                FOREIGN KEY (date_id) REFERENCES dim_calendar(date_id)
            )
        """)
        print("✓ Created fct_weather_history table")

        print("\n=== Verification ===")
        res_city = conn.execute("SELECT COUNT(*) FROM dim_cities").fetchone()
        city_count = res_city[0] if res_city is not None else 0
        
        res_date = conn.execute("SELECT COUNT(*) FROM dim_calendar").fetchone()
        date_count = res_date[0] if res_date is not None else 0
        
        res_weather = conn.execute("SELECT COUNT(*) FROM fct_weather_history").fetchone()
        weather_count = res_weather[0] if res_weather is not None else 0


        print(f"Cities in database: {city_count}")
        print(f"Calendar dates in database: {date_count}")
        print(f"Weather records in database: {weather_count}")

        if city_count > 0 and date_count > 0:
            print("✓ Database setup verified successfully!")
            print("✓ Ready for Dagster pipeline!")
        else:
            print("✗ Database setup verification failed!")

    except Exception as e:
        print(f"Error during database setup: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        print("\nDatabase connection closed.")
        print("Simple database setup completed!")

if __name__ == "__main__":
    setup_database_simple()
