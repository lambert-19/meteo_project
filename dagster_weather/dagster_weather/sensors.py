"""Dagster sensors for weather data pipeline."""

from dagster import (
    sensor,  # type: ignore
    SensorEvaluationContext,
    SensorResult,
    RunRequest,
    DefaultSensorStatus,
    run_failure_sensor,  # type: ignore
    RunFailureSensorContext
)
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from .jobs import weather_job
import re

@sensor(
    job=weather_job,
    description="Trigger weather pipeline when new CSV files are detected",
    minimum_interval_seconds=120,  # Check every 2 minutes
    default_status=DefaultSensorStatus.RUNNING
)
def csv_file_sensor(context: SensorEvaluationContext):
    """Sensor that triggers the weather pipeline when new CSV files are detected."""
    
    # Get the data directory
    data_dir = Path(__file__).resolve().parent.parent.parent / "data"
    
    if not data_dir.exists():
        context.log.warning(f"Data directory {data_dir} does not exist.")
        return SensorResult(cursor=context.cursor)

    # Find all weather CSV files
    csv_files = [f for f in os.listdir(str(data_dir)) if f.startswith('weather_data_') and f.endswith('.csv')]
    
    # Get the latest file timestamp
    latest_file = None
    latest_timestamp = None
    
    for csv_file in csv_files:
        file_path = data_dir / csv_file
        file_mtime = file_path.stat().st_mtime
        
        if latest_timestamp is None or file_mtime > latest_timestamp:
            latest_timestamp = file_mtime
            latest_file = csv_file
    
    # Check if we have a new file since last check
    last_seen_timestamp = float(context.cursor) if context.cursor else 0.0
    
    if latest_file and latest_timestamp and latest_timestamp > last_seen_timestamp:
        # New file detected, trigger the pipeline
        run_key = f"csv_trigger_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        
        # Extraire la date du nom de fichier (format: weather_data_YYYYMMDD_...)
        # Exemple: weather_data_20260426_021709.csv -> 2026-04-26
        date_match = re.search(r'weather_data_(\d{4})(\d{2})(\d{2})', latest_file)
        if date_match:
            partition_key = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
        else:
            # Fallback sur la date du jour - 5 (délai Archive API) si le nom ne correspond pas
            partition_key = datetime.fromtimestamp(latest_timestamp).strftime('%Y-%m-%d')

        context.log.info(f"New CSV file detected: {latest_file}. Targeting job: weather_pipeline_job")
        context.log.info(f"Triggering weather pipeline with run key: {run_key}")
        
        return SensorResult(
            run_requests=[
                RunRequest(
                    run_key=run_key,
                    run_config={
                        "ops": {
                            "raw_weather_data": {
                                "config": {
                                    "trigger": "csv_sensor",
                                    "file": latest_file,
                                    "timestamp": datetime.fromtimestamp(latest_timestamp).isoformat()
                                }
                            }
                        }
                    },
                    tags={"trigger": "csv_sensor", "file": latest_file},
                    partition_key=partition_key
                )
            ],
            cursor=str(latest_timestamp)
        )
    
    return SensorResult(cursor=str(last_seen_timestamp))

@sensor(
    job=weather_job,
    description="Trigger weather pipeline based on API rate limits and optimal timing",
    minimum_interval_seconds=3600,  # Check every hour
    default_status=DefaultSensorStatus.RUNNING
)
def api_rate_limit_sensor(context: SensorEvaluationContext):
    """Sensor that triggers the pipeline based on API rate limits and optimal conditions."""
    
    current_time = datetime.now(timezone.utc)
    current_hour = current_time.hour
    
    # Define optimal hours for weather data extraction (avoiding peak API usage times)
    optimal_hours = [6, 7, 8, 9, 10, 14, 15, 16, 17, 18, 19, 20]
    
    last_run_time = float(context.cursor) if context.cursor else 0.0
    
    # Check if it's an optimal hour and we haven't run recently
    if current_hour in optimal_hours:
        time_since_last_run = current_time.timestamp() - last_run_time
        
        # Run at most once every 3 hours during optimal times
        if time_since_last_run >= 3 * 3600:  # 3 hours in seconds
            # On cible la partition d'il y a 5 jours (délai de consolidation API Archive)
            partition_key = (current_time - timedelta(days=5)).strftime("%Y-%m-%d")
            run_key = f"api_optimal_{current_time.strftime('%Y%m%d_%H%M%S')}"
            
            context.log.info(f"Optimal API time detected: {current_hour}:00")
            context.log.info(f"Triggering weather pipeline with run key: {run_key}")
            
            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=run_key,
                        run_config={
                            "ops": {
                                "raw_weather_data": {
                                    "config": {
                                        "trigger": "api_rate_sensor",
                                        "optimal_hour": current_hour,
                                        "execution_time": current_time.isoformat()
                                    }
                                }
                            }
                        },
                        tags={"trigger": "api_rate_sensor", "hour": str(current_hour)},
                        partition_key=partition_key
                    )
                ],
                cursor=str(current_time.timestamp())
            )
    
    return SensorResult(cursor=str(last_run_time))

@sensor(
    job=weather_job,
    description="Trigger weather pipeline for specific cities based on weather conditions",
    minimum_interval_seconds=1800  # Check every 30 minutes
    
)
def weather_condition_sensor(context: SensorEvaluationContext):
    """Sensor that triggers the pipeline when specific weather conditions are met."""
    
    # This is a placeholder for a more sophisticated sensor
    # In a real implementation, you might check external weather APIs
    # or other conditions that should trigger additional data collection
    
    current_time = datetime.now(timezone.utc)
    
    # Example: Trigger more frequently during severe weather events
    # This would require access to real-time weather alerts
    
    last_severe_check = float(context.cursor) if context.cursor else 0.0
    
    # For demonstration, we'll just run every 4 hours
    time_since_check = current_time.timestamp() - last_severe_check
    
    if time_since_check >= 4 * 3600:  # 4 hours
        # On cible la partition d'il y a 5 jours
        partition_key = (current_time - timedelta(days=5)).strftime("%Y-%m-%d")
        
        run_key = f"condition_check_{current_time.strftime('%Y%m%d_%H%M%S')}"
        
        context.log.info("Running weather condition check")
        context.log.info(f"Triggering pipeline with run key: {run_key}")
        
        return SensorResult(
            run_requests=[
                RunRequest(
                    run_key=run_key,
                    run_config={
                        "ops": {
                                "raw_weather_data": {
                                "config": {
                                    "trigger": "condition_sensor",
                                    "check_type": "severe_weather",
                                    "execution_time": current_time.isoformat()
                                }
                            }
                        }
                    },
                    tags={"trigger": "condition_sensor", "check_type": "severe_weather"},
                    partition_key=partition_key
                )
            ],
            cursor=str(current_time.timestamp())
        )
    
    return SensorResult(cursor=str(last_severe_check))

@sensor(
    job=weather_job,
    description="Manual trigger sensor for ad-hoc weather data collection",
    minimum_interval_seconds=60,  # Check every minute
    default_status=DefaultSensorStatus.RUNNING
)
def manual_trigger_sensor(context: SensorEvaluationContext):
    """Sensor that checks for manual trigger requests (e.g., from a flag file)."""
    
    # Check for a trigger file in the data directory
    project_root = Path(__file__).resolve().parent.parent.parent
    data_dir = project_root / "data"
    trigger_file = data_dir / ".trigger_weather_pipeline"
    
    last_trigger_check = float(context.cursor) if context.cursor else 0.0
    
    current_time = datetime.now(timezone.utc)
    
    if os.path.exists(trigger_file):
        file_mtime = os.path.getmtime(trigger_file)
        
        # Check if the trigger file is newer than our last check
        if file_mtime > last_trigger_check:
            run_key = f"manual_trigger_{current_time.strftime('%Y%m%d_%H%M%S')}"
            
            context.log.info("Manual trigger detected")
            context.log.info(f"Triggering pipeline with run key: {run_key}")
            
            # Read trigger file for additional config if needed
            # Le fichier peut contenir une date spécifique (YYYY-MM-DD)
            trigger_config = {}
            partition_key: str = (current_time - timedelta(days=5)).strftime("%Y-%m-%d") # Default
            
            try:
                with open(trigger_file, 'r') as f:
                    content = f.read().strip()
                    if content:
                        # Si le contenu ressemble à une date, on l'utilise comme partition
                        if re.match(r'\d{4}-\d{2}-\d{2}', content):
                            partition_key = content
                        trigger_config = {"manual_info": content}
            except:
                pass
            
            # Remove the trigger file after processing
            try:
                os.remove(trigger_file)
                context.log.info("Removed trigger file")
            except OSError as e:
                context.log.warning(f"Could not remove trigger file: {e}")
            
            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=run_key,
                        run_config={
                            "ops": {
                            "raw_weather_data": {
                                    "config": {
                                        "trigger": "manual_sensor",
                                        "execution_time": current_time.isoformat(),
                                        **trigger_config
                                    }
                                }
                            }
                        },
                    tags={"trigger": "manual_sensor"},
                    partition_key=partition_key
                    )
                ],
                cursor=str(current_time.timestamp())
            )
    
    return SensorResult(cursor=str(last_trigger_check))

@run_failure_sensor(
    name="weather_pipeline_failure_sensor",
    job_selection=[weather_job],
    default_status=DefaultSensorStatus.RUNNING
)
def weather_pipeline_failure_sensor(context: RunFailureSensorContext):
    """Alerte déclenchée en cas d'échec du job (incluant les tests dbt via dbt build)."""
    job_name = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    error_msg = context.failure_event.message
    
    # Ici, vous pouvez intégrer un client Slack, Email ou MS Teams
    context.log.error(f"🚨 ALERTE MÉTÉO : Échec du Job {job_name} (Run: {run_id}). Erreur : {error_msg}")

# All sensors to be registered
all_sensors = [
    csv_file_sensor,
    api_rate_limit_sensor,
    weather_condition_sensor,
    manual_trigger_sensor,
    weather_pipeline_failure_sensor
]
