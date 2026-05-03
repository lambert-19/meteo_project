"""Configuration for Dagster jobs and schedules."""

import os
from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any, cast

class WeatherExtractionConfig(BaseModel):
    """Configuration for weather data extraction."""
    
    cities: list[str] = Field(default=["Paris", "Lyon", "Marseille"], description="Cities to extract weather data for")
    batch_id: Optional[str] = Field(default=None, description="Batch identifier for tracking")
    execution_date: Optional[str] = Field(default=None, description="Execution date in YYYY-MM-DD format")
    source: str = Field(default="manual", description="Source of the extraction trigger")
    trigger: str = Field(default="manual", description="Type of trigger")
    
    class Config:
        extra = "allow"

class WeatherLoadingConfig(BaseModel):
    """Configuration for weather data loading."""
    
    target: str = Field(default="motherduck", description="Target database system")
    batch_id: Optional[str] = Field(default=None, description="Batch identifier for tracking")
    execution_date: Optional[str] = Field(default=None, description="Execution date in YYYY-MM-DD format")
    backup_enabled: bool = Field(default=True, description="Whether to create backup copies")
    
    class Config:
        extra = "allow"

class ScheduleConfig(BaseModel):
    """Configuration for schedule execution."""
    
    timezone: str = Field(default="Europe/Paris", description="Timezone for schedule execution")
    retry_policy: Dict[str, Any] = Field(
        default={
            "max_retries": 3,
            "retry_delay_seconds": 300,
            "retry_on_failure": True
        },
        description="Retry policy for failed runs"
    )
    notification_config: Dict[str, Any] = Field(
        default={
            "on_success": False,
            "on_failure": True,
            "channels": ["email"]
        },
        description="Notification configuration"
    )
    
    class Config:
        extra = "allow"

class JobConfig(BaseModel):
    """Complete job configuration."""
    
    name: str = Field(..., description="Job name")
    description: str = Field(..., description="Job description")
    tags: Dict[str, str] = Field(default_factory=dict, description="Job tags")
    execution_config: Dict[str, Any] = Field(default_factory=dict, description="Execution configuration")
    
    class Config:
        extra = "allow"

DEFAULT_EXTRACTION_CONFIG = WeatherExtractionConfig(
    cities=["Paris", "Lyon", "Marseille"],
    source="scheduled_run"
)

DEFAULT_LOADING_CONFIG = WeatherLoadingConfig(
    target="motherduck",
    backup_enabled=True
)

DEFAULT_SCHEDULE_CONFIG = ScheduleConfig(
    timezone="Europe/Paris",
    retry_policy={
        "max_retries": 3,
        "retry_delay_seconds": 300,
        "retry_on_failure": True
    }
)

WEATHER_JOB_CONFIG = JobConfig(
    name="weather_pipeline_job",
    description="Complete weather data ELT pipeline",
    tags={
        "pipeline": "weather",
        "frequency": "daily",
        "environment": "production"
    },
    execution_config={
        "log_level": "INFO",
        "resource_requirements": {
            "memory_mb": 512,
            "cpu_count": 1
        }
    }
)


DAILY_SCHEDULE_CONFIG = {
    "cron_schedule": "0 6 * * *", 
    "description": "Daily weather data extraction and loading",
    "execution_timezone": "Europe/Paris",
    "default_status": "RUNNING"
}

HOURLY_SCHEDULE_CONFIG = {
    "cron_schedule": "0 * * * *", 
    "description": "Hourly weather data extraction (for testing)",
    "execution_timezone": "Europe/Paris",
    "default_status": "STOPPED"
}

WEEKLY_SCHEDULE_CONFIG = {
    "cron_schedule": "0 2 * * 1",  
    "description": "Weekly comprehensive weather data refresh",
    "execution_timezone": "Europe/Paris",
    "default_status": "STOPPED"
}

ENVIRONMENT_CONFIGS: Dict[str, Dict[str, Any]] = {
    "development": {
        "schedules_enabled": False,
        "sensors_enabled": True,
        "log_level": "DEBUG",
        "retry_policy": {
            "max_retries": 1,
            "retry_delay_seconds": 60
        }
    },
    "staging": {
        "schedules_enabled": True,
        "sensors_enabled": True,
        "log_level": "INFO",
        "retry_policy": {
            "max_retries": 2,
            "retry_delay_seconds": 180
        }
    },
    "production": {
        "schedules_enabled": True,
        "sensors_enabled": True,
        "log_level": "INFO",
        "retry_policy": {
            "max_retries": 3,
            "retry_delay_seconds": 300
        }
    }
}

def get_environment_config(environment: str = "production") -> Dict[str, Any]:
    """Get environment-specific configuration."""
    return ENVIRONMENT_CONFIGS.get(environment, ENVIRONMENT_CONFIGS["production"])

def resolve_config(config: Dict[str, Any], environment: str = "production") -> Dict[str, Any]:
    """Resolve configuration with environment variables and environment-specific overrides."""
    
    env_config = get_environment_config(environment)
    resolved_config = config.copy()
    
    for key, value in env_config.items():
        if key not in resolved_config:
            resolved_config[key] = value

    def resolve_env_vars(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: resolve_env_vars(v) for k, v in cast(Dict[Any, Any], obj).items()}
        elif isinstance(obj, list):
            return [resolve_env_vars(item) for item in cast(List[Any], obj)]
        elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
            env_var = obj[2:-1]
            return os.getenv(env_var, obj)
        else:
            return obj
    
    return cast(Dict[str, Any], resolve_env_vars(resolved_config))
