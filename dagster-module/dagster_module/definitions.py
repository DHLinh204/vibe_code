from pathlib import Path

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from dagster_module import assets

dbt_project_path = Path(__file__).parent.parent.parent / "transform"

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project_path),
    },
)

