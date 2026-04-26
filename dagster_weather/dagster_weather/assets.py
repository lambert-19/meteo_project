"""Dagster assets for weather data pipeline with MotherDuck integration."""

import os
import sys
from typing import Any
import pandas as pd
from datetime import datetime
from pathlib import Path

# Add project root to path for proper imports
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    DailyPartitionsDefinition,
)

# Import from project-level modules
from extract import WeatherExtractor
from data import load_csv_to_duckdb

# Définition d'une partition quotidienne
daily_partition = DailyPartitionsDefinition(
    start_date="2024-01-01"
)

@asset(
    key="raw_weather_data",
    description="Extract historical weather data from Open-Meteo",
    compute_kind="python",
    partitions_def=daily_partition,
)
def raw_weather_data(context: AssetExecutionContext) -> MaterializeResult[Any]:
    """Extract historical weather data from Open-Meteo API."""
    
    partition_date = context.partition_key
    context.log.info(f"Starting weather data extraction for partition: {partition_date}...")
    
    extractor: WeatherExtractor = WeatherExtractor()
    weather_data: Any = extractor.extract_all_cities(target_date=partition_date)
    df: pd.DataFrame = extractor.to_dataframe(weather_data)
    
    # Sauvegarde effective du CSV pour que l'asset de chargement puisse le trouver
    timestamp = f"{partition_date}_{datetime.now().strftime('%H%M%S')}"
    project_root = Path(__file__).resolve().parent.parent.parent
    data_dir = project_root / "data"
    filepath = data_dir / f"weather_data_{timestamp}.csv"
    extractor.save_to_csv(df, str(filepath))
    
    if df.empty:
        context.log.warning("No weather data extracted")
        return MaterializeResult(
            metadata={"error": "No data extracted"}
        )
    
    # Add metadata
    metadata: dict[str, Any] = {
        "records_count": len(df),
        "cities": list(df['city'].unique()),
        "date": str(df['date'].iloc[0]) if not df.empty else None,
        "columns": list(df.columns),
        "preview": MetadataValue.md(df.head().to_markdown())
    }
    
    context.log.info(f"Successfully extracted {len(df)} weather records")
    
    return MaterializeResult(metadata=metadata)


@asset(
    key="load_weather_to_duckdb",
    description="Load weather data from CSV files into DuckDB database",
    compute_kind="python",
    deps=["raw_weather_data"]
)
def load_weather_to_duckdb(context: AssetExecutionContext) -> MaterializeResult[Any]:
    """Load weather data from CSV files into DuckDB database."""
    
    context.log.info("Starting weather data loading to DuckDB...")
    
    # Use the imported CSV loading function
    result: dict[str, Any] = load_csv_to_duckdb()
    
    # Convert result to Dagster MaterializeResult
    if "error" in result:
        context.log.error(f"Error: {result['error']}")
        return MaterializeResult(metadata=result)
    elif "warning" in result:
        context.log.warning(f"Warning: {result['warning']}")
        return MaterializeResult(metadata=result)
    else:
        context.log.info(f"Success: {result}")
        return MaterializeResult(metadata=result)
