import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

dbt_project_path = Path(__file__).parent.parent.parent / "transform"

dbt_project = DbtProject(
    project_dir=dbt_project_path,
)


@dbt_assets(
    manifest=dbt_project.manifest_path,
)
def transform_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> None:

    yield from dbt.cli(["run"], context=context).stream()

