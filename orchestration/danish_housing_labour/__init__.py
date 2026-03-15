from dagster import Definitions
from .assets import all_assets, dbt_resource
from .schedules import daily_refresh_schedule

defs = Definitions(
    assets=all_assets,
    schedules=[daily_refresh_schedule],
    resources={"dbt": dbt_resource},
)
