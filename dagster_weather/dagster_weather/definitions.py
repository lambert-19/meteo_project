from typing import Any
from dagster import Definitions, load_assets_from_modules  # type: ignore
from . import assets

# Import jobs and schedules
from .jobs import all_jobs, all_schedules
from .sensors import all_sensors

all_assets: Any = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors
)
