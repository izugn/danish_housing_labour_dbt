from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from .assets import all_assets

danish_housing_labour_job = define_asset_job(
    name="danish_housing_labour_job",
    selection=AssetSelection.all(),
)

daily_refresh_schedule = ScheduleDefinition(
    name="daily_housing_labour_refresh",
    cron_schedule="0 6 * * *",  # 06:00 UTC every day
    job=danish_housing_labour_job,
)
