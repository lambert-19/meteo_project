"""CSV loading utilities for DuckDB database."""

import os
import pandas as pd
import duckdb
from dotenv import load_dotenv
from datetime import datetime, date
from typing import Dict, Any, cast, Optional

def load_csv_to_duckdb(target_date: Optional[str] = None) -> Dict[str, Any]:
    """Load weather data from CSV files into DuckDB database."""
    
    print("Starting weather data loading to DuckDB...")
    
    # Load environment variables
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    env_path = os.path.join(project_root, '.env')
    load_dotenv(env_path)
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    
    if not motherduck_token:
        print("MOTHERDUCK_TOKEN environment variable not set")
        return {"error": "MOTHERDUCK_TOKEN environment variable not set"}
    
    # Connect to MotherDuck
    try:
        conn = duckdb.connect(f'md:?motherduck_token={motherduck_token}')
        conn.execute("USE meteo_weather")
        print("Connected to MotherDuck meteo_weather database")
    except Exception as e:
        print(f"Failed to connect to MotherDuck: {e}")
        return {"error": f"MotherDuck connection failed: {e}"}
    
    # Get city mappings
    try:
        city_mapping = conn.execute("""
            SELECT city_id, city_name FROM meteo_weather.main.dim_cities
        """).fetchall()
        city_dict: Dict[str, int] = {name: city_id for city_id, name in city_mapping}
        print(f"Found {len(city_dict)} cities in database")
    except Exception as e:
        print(f"Failed to get city mappings: {e}")
        return {"error": f"City mapping failed: {e}"}
    
    # Find CSV files
    data_dir = os.path.join(project_root, 'data')
    
    if target_date:
        # Chercher les fichiers correspondant à la date de la partition (YYYY-MM-DD -> YYYYMMDD)
        date_clean = target_date.replace('-', '')
        csv_files = [f for f in os.listdir(data_dir) if f"weather_data_{date_clean}" in f and f.endswith('.csv')]
    else:
        csv_files = [f for f in os.listdir(data_dir) if f.startswith('weather_data_') and f.endswith('.csv')]
    
    if not csv_files:
        msg = f"No weather CSV files found for date: {target_date if target_date else 'any'}"
        print(msg)
        return {"warning": msg}
    
    # Prendre le plus récent parmi ceux trouvés
    csv_files = sorted(csv_files)
    latest_file = csv_files[-1]
    
    print(f"Latest CSV file identified: {latest_file}")
    
    total_records_loaded = 0
    processed_files = 0
    
    try:
        for csv_file in [latest_file]:
            csv_path = os.path.join(data_dir, csv_file)
            print(f"Processing {csv_file}")
            
            # Read CSV data
            df = pd.read_csv(csv_path)
            print(f"Loaded {len(df)} records from CSV")
            
            # Get next weather_id from database
            res = conn.execute("SELECT MAX(weather_id) FROM meteo_weather.main.fct_weather_history").fetchone()
            weather_id: int = 1
            if res and res[0] is not None:
                weather_id = int(res[0]) + 1
            
            # Process each record
            for _, row in df.iterrows():
                city_name = row['city']
                date_str = row['date']
                
                # Get city_id from dimension
                if city_name not in city_dict:
                    print(f"Warning: City {city_name} not found in dim_cities")
                    continue
                
                city_id = city_dict[city_name]
                
                # Create date_id from date
                date_obj: date = cast(Any, pd.to_datetime(date_str)).date()
                date_id = int(date_obj.strftime("%Y%m%d"))
                
                # INSERT with exactly 11 columns and 11 values
                conn.execute("""
                    INSERT INTO meteo_weather.main.fct_weather_history 
                    (weather_id, city_id, date_id, timestamp, temperature, temp_min, temp_max, precipitation_sum,
                     pressure, humidity, weather_description, wind_speed, extracted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    int(weather_id),  # weather_id
                    int(city_id),    # city_id
                    int(date_id),     # date_id
                    str(date_str),    # timestamp
                    float(row['temperature']),  # temperature
                    float(row['temp_min']),     # temp_min
                    float(row['temp_max']),     # temp_max
                    float(row['precipitation_sum']) if 'precipitation_sum' in row and pd.notna(row['precipitation_sum']) else 0.0,
                    float(row['pressure']) if 'pressure' in row and pd.notna(row['pressure']) else None,
                    int(row['humidity']) if 'humidity' in row and pd.notna(row['humidity']) else None,
                    str(row['weather_description']) if 'weather_description' in row else 'Historical',
                    float(row['wind_speed']) if 'wind_speed' in row and pd.notna(row['wind_speed']) else 0.0,
                    pd.Timestamp.now()  # extracted_at (current timestamp)
                ))
                
                weather_id += 1
            
            records_loaded = len(df)
            total_records_loaded += records_loaded
            processed_files += 1
            print(f"Successfully loaded {records_loaded} records from {csv_file}")
        
        # Verify final count
        count_res = conn.execute("SELECT COUNT(*) FROM meteo_weather.main.fct_weather_history").fetchone()
        final_count = count_res[0] if count_res else 0
        
        # Create metadata
        metadata: Dict[str, Any] = {
            "csv_files_processed": processed_files,
            "total_records_loaded": total_records_loaded,
            "final_database_count": final_count,
            "cities_processed": list(city_dict.keys()),
            "processing_timestamp": datetime.now().isoformat()
        }
        
        print(f"Successfully loaded {total_records_loaded} records from {processed_files} CSV files")
        print(f"Total weather records in database: {final_count}")
        
        conn.close()
        
        return metadata
        
    except Exception as e:
        print(f"Error loading weather data: {e}")
        import traceback
        traceback.print_exc()
        
        if 'conn' in locals():
            conn.close()
        
        return {"error": f"Loading failed: {e}"}


if __name__ == "__main__":
    result: Dict[str, Any] = load_csv_to_duckdb()
    if "error" in result:
        print(f"Error: {result['error']}")
    elif "warning" in result:
        print(f"Warning: {result['warning']}")
    else:
        print(f"Success: {result}")
