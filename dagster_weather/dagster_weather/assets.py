"""Dagster assets for weather data pipeline with MotherDuck integration."""

import os
import sys
from typing import Any, Mapping, Iterator, cast, Optional, List
import pandas as pd
from datetime import datetime
from pathlib import Path

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dagster import (
    AssetKey,
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    DailyPartitionsDefinition,
    Config,
)
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator, DbtProject

from extract import WeatherExtractor
from data import load_csv_to_duckdb

DBT_PROJECT_DIR = Path(_project_root) / "dbt_meteo"
dbt_project = DbtProject(project_dir=os.fspath(DBT_PROJECT_DIR))
dbt_project.prepare_if_dev()

class MeteoDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if resource_type == "source" and name == "fct_weather_history":
            return AssetKey("load_weather_to_duckdb")
        return super().get_asset_key(dbt_resource_props)

daily_partition = DailyPartitionsDefinition(
    start_date="2024-01-01"
)

class RawWeatherDataConfig(Config):
    cities: List[str] = ["Paris", "Lyon", "Marseille"]
    batch_id: Optional[str] = None
    execution_date: Optional[str] = None
    source: str = "manual"
    trigger: str = "manual"
    file: Optional[str] = None
    timestamp: Optional[str] = None

@asset(
    key="raw_weather_data",
    description="Extract historical weather data from Open-Meteo",
    compute_kind="python",
    partitions_def=daily_partition,
)
def raw_weather_data(context: AssetExecutionContext, config: RawWeatherDataConfig) -> MaterializeResult[Any]:
    """Extract historical weather data from Open-Meteo API."""
    
    partition_date = context.partition_key
    target_date = config.execution_date or partition_date
    context.log.info(f"Starting weather data extraction for: {target_date}...")
    
    extractor: WeatherExtractor = WeatherExtractor()
    weather_data: Any = extractor.extract_all_cities(target_date=target_date)
    df: pd.DataFrame = extractor.to_dataframe(weather_data)
    
    date_clean = partition_date.replace("-", "")
    timestamp = f"{date_clean}_{datetime.now().strftime('%H%M%S')}"
    data_dir = Path(_project_root) / "data"
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
    partitions_def=daily_partition,
    deps=["raw_weather_data"]
)
def load_weather_to_duckdb(context: AssetExecutionContext) -> MaterializeResult[Any]:
    """Load weather data from CSV files into DuckDB database."""
    
    partition_date = context.partition_key
    context.log.info(f"Starting weather data loading to DuckDB for partition: {partition_date}...")
    
    result: dict[str, Any] = load_csv_to_duckdb(target_date=partition_date)

    if "error" in result:
        context.log.error(f"Error: {result['error']}")
        return MaterializeResult(metadata=result)
    elif "warning" in result:
        context.log.warning(f"Warning: {result['warning']}")
        return MaterializeResult(metadata=result)
    else:
        context.log.info(f"Success: {result}")
        return MaterializeResult(metadata=result)

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=MeteoDbtTranslator()
)
def dbt_meteo_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    """Exécute les transformations dbt (staging et marts)."""
    yield from cast(Any, dbt.cli(["build"], context=context)).stream()

@asset(
    key="streamlit_dashboard",
    description="Dashboard Streamlit de visualisation météo",
    compute_kind="streamlit",
    deps=[
        AssetKey("mart_city_frost_days"),
        AssetKey("mart_city_heatwaves"),
        AssetKey("mart_city_thermal_amplitude"),
        AssetKey("mart_city_weather_records"),
        AssetKey("mart_regional_precipitation_comparison"),
    ],
)
def streamlit_dashboard(context: AssetExecutionContext) -> MaterializeResult:
    """Lance le dashboard Streamlit et vérifie que les données sont disponibles."""
    
    import subprocess
    import sys
    
    data_dir = Path(_project_root) / "data"
    csv_files = list(data_dir.glob("weather_data_*.csv"))
    
    if not csv_files:
        context.log.warning("Aucun fichier CSV trouvé pour le dashboard.")
        return MaterializeResult(metadata={"status": "no data"})
    
    latest_csv = max(csv_files, key=lambda f: f.stat().st_mtime)
    
    dashboard_path = Path(_project_root) / "dashboard" / "app.py"
    
    try:
        subprocess.Popen(
            [sys.executable, "-m", "streamlit", "run", str(dashboard_path),
             "--server.port", "8501",
             "--server.headless", "true"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        context.log.info("Dashboard Streamlit démarré sur http://localhost:8501")
    except Exception as e:
        context.log.warning(f"Streamlit déjà en cours ou erreur : {e}")
    
    return MaterializeResult(
        metadata={
            "url": MetadataValue.url("http://localhost:8501"),
            "latest_data_file": MetadataValue.text(latest_csv.name),
            "csv_files_count": MetadataValue.int(len(csv_files)),
            "status": MetadataValue.text("Dashboard opérationnel"),
        }
    )