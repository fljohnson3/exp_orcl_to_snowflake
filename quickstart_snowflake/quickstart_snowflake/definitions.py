from dagster import (
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)
from dagster_snowflake_pandas import SnowflakePandasIOManager

from dagster_embedded_elt.sling import SlingConnectionResource,SlingResource
from . import assets

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)


defs = Definitions(
    assets=load_assets_from_package_module(assets),
    resources={
        "io_manager": SnowflakePandasIOManager(
            # Read about using environment variables and secrets in Dagster:
            # https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets
            account="VL04224.east-us-2.azure",
            database="ARTEMIS",
            password="Snowball88$$",
            user="mwilliams",
        ),
        "sling": SlingResource(
            type="oracle",
            host="192.168.233.135",
            password="af!J2Y47hG",
            port=1521,
            service_name="devl.fdevdba3.sdidcnet.oraclevcn.com",
            user="SDIRPA"
        )
    },
    schedules=[daily_refresh_schedule],
)
