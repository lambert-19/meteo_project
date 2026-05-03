"""Run configurations for Dagster jobs."""

from dagster import ScheduleEvaluationContext
from typing import Dict, Any, Optional
from datetime import datetime

DEFAULT_RUN_CONFIG: Dict[str, Any] = {
    "execution": {
        "config": {
            "log_level": "INFO",
            "raise_on_skip": True
        }
    },
    "resources": {
        "io_manager": {
            "config": {
                "base_dir": "/tmp/dagster/storage"
            }
        }
    }
}

def get_extraction_run_config(
    cities: Optional[list[str]] = None,
    batch_id: Optional[str] = None,
    execution_date: Optional[str] = None,
    source: str = "manual",
    trigger: str = "manual"
) -> Dict[str, Any]:
    """Get run configuration for weather extraction."""
    
    config: Dict[str, Any] = {
        "ops": {
            "raw_weather_data": {
                "config": {
                    "cities": cities or ["Paris", "Lyon", "Marseille"],
                    "batch_id": batch_id,
                    "execution_date": execution_date,
                    "source": source,
                    "trigger": trigger
                }
            }
        }
    }
    
    return config
def get_loading_run_config(
    target: str = "motherduck",
    batch_id: Optional[str] = None,
    execution_date: Optional[str] = None,
    backup_enabled: bool = True
) -> Dict[str, Any]:
    """Get run configuration for weather loading."""
    
    config: Dict[str, Any] = {
        "ops": {
            "load_weather_to_duckdb": {
                "config": {
                    "target": target,
                    "batch_id": batch_id,
                    "execution_date": execution_date,
                    "backup_enabled": backup_enabled
                }
            }
        }
    }
    
    return config

# Complete pipeline run configuration
def get_pipeline_run_config(
    cities: Optional[list[str]] = None,
    batch_id: Optional[str] = None,
    execution_date: Optional[str] = None,
    source: str = "manual",
    trigger: str = "manual",
    target: str = "motherduck",
    backup_enabled: bool = True
) -> Dict[str, Any]:
    """Get run configuration for complete weather pipeline."""
    
    config: Dict[str, Any] = {
        "ops": {
            "raw_weather_data": {
                "config": {
                    "cities": cities or ["Paris", "Lyon", "Marseille"],
                    "batch_id": batch_id,
                    "execution_date": execution_date,
                    "source": source,
                    "trigger": trigger
                }
            },
            "load_weather_to_duckdb": {
                "config": {
                    "target": target,
                    "batch_id": batch_id,
                    "execution_date": execution_date,
                    "backup_enabled": backup_enabled
                }
            }
        }
    }
    
    return config

def get_development_run_config() -> Dict[str, Any]:
    """Get development environment run configuration."""
    
    return get_pipeline_run_config(
        cities=["Paris"],  
        source="development",
        trigger="manual",
        backup_enabled=True
    )

def get_production_run_config() -> Dict[str, Any]:
    """Get production environment run configuration."""
    
    return get_pipeline_run_config(
        cities=["Paris", "Lyon", "Marseille"],
        source="production",
        trigger="scheduled",
        backup_enabled=True
    )

def get_testing_run_config() -> Dict[str, Any]:
    """Get testing environment run configuration."""
    
    return get_pipeline_run_config(
        cities=["Paris"],  
        source="testing",
        trigger="test",
        backup_enabled=False  
    )

def get_daily_schedule_run_config(context: ScheduleEvaluationContext) -> Dict[str, Any]:
    """Get run configuration for daily schedule."""
    
    execution_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    batch_id = f"weather_daily_{execution_date}"
    
    return get_pipeline_run_config(
        cities=["Paris", "Lyon", "Marseille"],
        batch_id=batch_id,
        execution_date=execution_date,
        source="daily_schedule",
        trigger="scheduled",
        backup_enabled=True
    )

def get_hourly_schedule_run_config(context: ScheduleEvaluationContext) -> Dict[str, Any]:
    """Get run configuration for hourly schedule."""
    
    execution_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    batch_id = f"weather_hourly_{execution_date}_{context.scheduled_execution_time.hour}"
    
    return get_pipeline_run_config(
        cities=["Paris"], 
        batch_id=batch_id,
        execution_date=execution_date,
        source="hourly_schedule",
        trigger="scheduled",
        backup_enabled=True
    )

def get_csv_sensor_run_config(file_name: str, timestamp: str) -> Dict[str, Any]:
    """Get run configuration for CSV file sensor trigger."""
    
    execution_date = timestamp.split("T")[0] if "T" in timestamp else timestamp
    batch_id = f"weather_csv_{file_name.replace('.csv', '')}"
    
    return get_pipeline_run_config(
        cities=["Paris", "Lyon", "Marseille"],
        batch_id=batch_id,
        execution_date=execution_date,
        source="csv_sensor",
        trigger="sensor",
        backup_enabled=True
    )

def get_api_sensor_run_config(hour: int) -> Dict[str, Any]:
    """Get run configuration for API rate limit sensor trigger."""
    
    execution_date = datetime.now().strftime("%Y-%m-%d")
    batch_id = f"weather_api_optimal_{hour}_{execution_date}"
    
    return get_pipeline_run_config(
        cities=["Paris", "Lyon", "Marseille"],
        batch_id=batch_id,
        execution_date=execution_date,
        source="api_sensor",
        trigger="sensor",
        backup_enabled=True
    )

def get_manual_run_config(
    cities: Optional[list[str]] = None,
    custom_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Get run configuration for manual trigger."""
    
    execution_date = datetime.now().strftime("%Y-%m-%d")
    batch_id = f"weather_manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    base_config = get_pipeline_run_config(
        cities=cities or ["Paris", "Lyon", "Marseille"],
        batch_id=batch_id,
        execution_date=execution_date,
        source="manual",
        trigger="manual",
        backup_enabled=True
    )
    
    if custom_config:
        for op_name, op_config in custom_config.items():
            if op_name in base_config["ops"]:
                base_config["ops"][op_name]["config"].update(op_config)
    
    return base_config

def validate_run_config(config: Dict[str, Any]) -> bool:
    """Validate run configuration."""
    
    required_keys = ["ops"]
    for key in required_keys:
        if key not in config:
            return False
 
    if "raw_weather_data" not in config["ops"]:
        return False
    
    return True

def merge_run_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple run configurations."""
    
    merged: Dict[str, Any] = {"ops": {}}
    
    for config in configs:
        if "ops" in config:
            for op_name, op_config in config["ops"].items():
                if op_name not in merged["ops"]:
                    merged["ops"][op_name] = {"config": {}}
                
                if "config" in op_config:
                    merged["ops"][op_name]["config"].update(op_config.get("config", {}))
    
    return merged
