"""Dagster jobs and schedules for weather data pipeline."""

from typing import Any, List
from dagster import (
    schedule,  # type: ignore
    ScheduleDefinition,
    DefaultScheduleStatus,
    RunRequest,
    AssetSelection,
    define_asset_job,  # type: ignore
    ScheduleEvaluationContext
)

# Define asset-based job for weather pipeline
weather_job: Any = define_asset_job(
    name="weather_pipeline_job",
    description="Complete weather data ELT pipeline",
    selection=AssetSelection.all(), # Select all assets
)

# Daily schedule for weather data extraction (every day at 9 AM)
daily_weather_schedule = ScheduleDefinition(
    job=weather_job,
    cron_schedule="0 9 * * *",  # Every day at 9:00 AM
    description="Weather data extraction and loading (daily at 9:00 AM)",
    default_status=DefaultScheduleStatus.RUNNING,
    execution_timezone="Europe/Paris"
)


# Hourly schedule for testing (optional)
hourly_weather_schedule = ScheduleDefinition(
    job=weather_job,
    cron_schedule="0 * * * *",  # Every hour at minute 0
    description="Hourly weather data extraction (for testing)",
    default_status=DefaultScheduleStatus.STOPPED,  # Disabled by default
    execution_timezone="Europe/Paris"
)

# Weekly schedule for comprehensive data refresh
weekly_weather_schedule = ScheduleDefinition(
    job=weather_job,
    cron_schedule="0 2 * * 1",  # Every Monday at 2:00 AM
    description="Weekly comprehensive weather data refresh",
    default_status=DefaultScheduleStatus.STOPPED,  # Disabled by default
    execution_timezone="Europe/Paris"
)

# Custom schedule with context for dynamic execution
@schedule(
    job=weather_job,
    cron_schedule="*/2 * * * *",  # Every 2 minutes for testing
    description="Weather pipeline with dynamic context (testing - every 2 minutes)",
    execution_timezone="Europe/Paris"
)
def daily_weather_with_context(context: ScheduleEvaluationContext) -> RunRequest:
    """Daily schedule with dynamic run configuration."""
    date_str: str = context.scheduled_execution_time.strftime("%Y-%m-%d")
    
    run_config = {
        "ops": {
            "raw_weather_data": {
                "config": {
                    "execution_date": date_str,
                    "batch_id": f"weather_{date_str}",
                    "source": "scheduled_run"
                }
            },
            "load_weather_to_duckdb": {
                "config": {
                    "execution_date": date_str,
                    "batch_id": f"weather_{date_str}",
                    "target": "motherduck"
                }
            }
        }
    }
    
    return RunRequest(
        run_key=f"daily_weather_{date_str}",
        run_config=run_config,
        tags={"date": date_str, "schedule": "daily"}
    )

# All schedules to be registered (simplified for testing)
all_schedules: List[Any] = [
    daily_weather_schedule,
    daily_weather_with_context
]

# All jobs to be registered
all_jobs: List[Any] = [
    weather_job
]
