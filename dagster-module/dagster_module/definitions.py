from pathlib import Path

from dagster import (
    Definitions,
    ScheduleDefinition,
    load_assets_from_modules
    # build_schedule_from_partitioned_time_window,
)
from dagster_dbt import DbtCliResource

from dagster_module import assets

dbt_project_path = Path(__file__).parent.parent.parent / "transform"

all_assets = load_assets_from_modules([assets])

# dbt_schedule = ScheduleDefinition(
#     job_name="transform_dbt_assets_job",
#     cron_schedule="0 2 * * *",  
# )

defs = Definitions(
    assets=all_assets,
    # schedules=[dbt_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project_path),
    },
)
