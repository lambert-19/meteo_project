from typing import Any
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from . import assets
from .assets import dbt_project
from .jobs import all_jobs, all_schedules
from .sensors import all_sensors

all_assets: Any = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
    }
)