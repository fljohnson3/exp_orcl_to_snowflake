from dagster_embedded_elt.sling import (
    SlingConnectionResource,
    SlingResource,
    sling_assets,
)

sling_resource = SlingResource(
    connections=[
        # Using a connection string from an environment variable
        # SlingConnectionResource(
        #     name="ORACLE_DEV",
        #     type="oracle",
        #     connection_string=EnvVar("POSTGRES_CONNECTION_STRING"),
        # ),
        # Using a hard-coded connection string
        SlingConnectionResource(
            name="MY_DUCKDB",
            type="duckdb",
            connection_string="duckdb:///var/tmp/duckdb.db",
        ),
        # Using a keyword-argument constructor
        
         

    ]
)

source = SlingConnectionResource(
            name="ORACLE_DEV",
            type="oracle",
            host='192.168.233.135',
            password='af!J2Y47hG',
            port='1521',
            service_name='devl.fdevdba3.sdidcnet.oraclevcn.com',
            user='SDIRPA'
)

target = SlingConnectionResource(
            name="SNOWFLAKE_DEV",
            type="snowflake",
            user='mwilliams',
            account='VL04224.east-us-2.azure',
            database='ARTEMIS',
            password='Snowball88$$',
            role="ACCOUNTADMIN"
)

sling = SlingResource(
    connections=[
        source,
        target,
    ]
)
replication_config = {
    "source": "ORACLE_DEV",
    "target": "SNOWFLAKE_DEV",
    "defaults": {"mode": "full-refresh", "object": "{stream_schema}_{stream_table}"},
    "streams": {
        "SYSADM8.PS_BI_ACCT_ENTRY": {"object": "ARTEMIS.___MIGRATION_STG.PEOPLESOFT_DEV_PS_BI_ACCT_ENTRY"},
    },
}


@sling_assets(replication_config=replication_config)
def my_assets(context, sling: SlingResource):
    yield from sling.replicate(context=context)