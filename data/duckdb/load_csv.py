"""CSV loading utilities for DuckDB local database."""

import os
import pandas as pd
import duckdb
from datetime import datetime, date
from typing import Dict, Any, cast, Optional


def get_db_path() -> str:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    return os.path.join(project_root, 'data', 'meteo.duckdb')


def load_csv_to_duckdb(target_date: Optional[str] = None) -> Dict[str, Any]:
    """Load weather data from CSV files into local DuckDB database."""

    print("Starting weather data loading to local DuckDB...")

    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    try:
        conn = duckdb.connect(db_path)
        print(f"Connected to local DuckDB: {db_path}")
    except Exception as e:
        return {"error": f"DuckDB connection failed: {e}"}
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_cities (
            city_id INTEGER PRIMARY KEY,
            city_name VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            elevation DOUBLE,
            region VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fct_weather_history (
            weather_id INTEGER,
            city_id INTEGER,
            date_id INTEGER,
            timestamp VARCHAR,
            temperature DOUBLE,
            temp_min DOUBLE,
            temp_max DOUBLE,
            precipitation_sum DOUBLE,
            weather_description VARCHAR,
            wind_speed DOUBLE,
            extracted_at TIMESTAMP
        )
    """)

    # Insérer les villes si elles n'existent pas
    cities = [
        (1, "Paris", 48.8566, 2.3522, 35, "Île-de-France"),
        (2, "Lyon", 45.7640, 4.8357, 173, "Auvergne-Rhône-Alpes"),
        (3, "Marseille", 43.2965, 5.3698, 12, "Provence-Alpes-Côte d'Azur"),
    ]
    for city in cities:
        conn.execute("""
            INSERT INTO dim_cities SELECT ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM dim_cities WHERE city_id = ?)
        """, (*city, city[0]))

    city_mapping = conn.execute("SELECT city_id, city_name FROM dim_cities").fetchall()
    city_dict: Dict[str, int] = {name: city_id for city_id, name in city_mapping}

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    data_dir = os.path.join(project_root, 'data')

    if target_date:
        date_clean = target_date.replace('-', '')
        csv_files = [f for f in os.listdir(data_dir) if f"weather_data_{date_clean}" in f and f.endswith('.csv')]
    else:
        csv_files = [f for f in os.listdir(data_dir) if f.startswith('weather_data_') and f.endswith('.csv')]

    if not csv_files:
        msg = f"No weather CSV files found for date: {target_date if target_date else 'any'}"
        return {"warning": msg}

    csv_files = sorted(csv_files)
    latest_file = csv_files[-1]

    total_records_loaded = 0

    try:
        csv_path = os.path.join(data_dir, latest_file)
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} records from {latest_file}")

        res = conn.execute("SELECT MAX(weather_id) FROM fct_weather_history").fetchone()
        weather_id: int = 1
        if res and res[0] is not None:
            weather_id = int(res[0]) + 1

        for _, row in df.iterrows():
            city_name = row['city']
            date_str = row['date']

            if city_name not in city_dict:
                continue

            city_id = city_dict[city_name]
            date_obj: date = cast(Any, pd.to_datetime(date_str)).date()
            date_id = int(date_obj.strftime("%Y%m%d"))

            conn.execute("""
                INSERT INTO fct_weather_history
                (weather_id, city_id, date_id, timestamp, temperature, temp_min, temp_max,
                 precipitation_sum, weather_description, wind_speed, extracted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                int(weather_id),
                int(city_id),
                int(date_id),
                str(date_str),
                float(row['temperature']),
                float(row['temp_min']),
                float(row['temp_max']),
                float(row['precipitation_sum']) if 'precipitation_sum' in row and pd.notna(row['precipitation_sum']) else 0.0,
                str(row['weather_description']) if 'weather_description' in row else 'Historical',
                float(row['wind_speed']) if 'wind_speed' in row and pd.notna(row['wind_speed']) else 0.0,
                pd.Timestamp.now()
            ))
            weather_id += 1

        total_records_loaded = len(df)
        count_res = conn.execute("SELECT COUNT(*) FROM fct_weather_history").fetchone()
        final_count = count_res[0] if count_res else 0
        conn.close()

        return {
            "total_records_loaded": total_records_loaded,
            "final_database_count": final_count,
            "processing_timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.close()
        return {"error": f"Loading failed: {e}"}


if __name__ == "__main__":
    result: Dict[str, Any] = load_csv_to_duckdb()
    print(result)