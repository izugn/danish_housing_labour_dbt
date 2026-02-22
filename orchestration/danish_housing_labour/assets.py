import subprocess
from pathlib import Path

from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_project"
INGESTION_DIR   = Path(__file__).parent.parent.parent / "ingestion"


@asset(group_name="ingestion")
def dst_raw_tables(context: AssetExecutionContext) -> None:
    """Fetch all DST tables and load them into Snowflake raw tables."""
    result = subprocess.run(
        ["python", "run_ingestion.py"],
        cwd=str(INGESTION_DIR),
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Ingestion failed:\n{result.stderr}")


dbt_resource = DbtCliResource(project_dir=str(DBT_PROJECT_DIR))


@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def danish_housing_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    yield from dbt.cli(["build"], context=context).stream()


all_assets = [dst_raw_tables, danish_housing_dbt_assets]
