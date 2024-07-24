from dagster_embedded_elt.sling import (
    SlingConnectionResource,
    SlingResource,
    sling_assets
)
import datetime
from dagster import BackfillPolicy, MonthlyPartitionsDefinition, HourlyPartitionsDefinition,TimeWindowPartitionsDefinition

# sling_resource = SlingResource(
#     connections=[
#         # Using a connection string from an environment variable
#         # SlingConnectionResource(
#         #     name="ORACLE_DEV",
#         #     type="oracle",
#         #     connection_string=EnvVar("POSTGRES_CONNECTION_STRING"),
#         # ),
#         # Using a hard-coded connection string
#         SlingConnectionResource(
#             name="MY_DUCKDB",
#             type="duckdb",
#             connection_string="duckdb:///var/tmp/duckdb.db",
#         ),
#         # Using a keyword-argument constructor
        
         

#     ]
# )

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

test_sling = SlingResource(
    connections=[
        source,
        target,
    ]
)
#Not yet worked out
test_partitions = MonthlyPartitionsDefinition(start_date="1998-08-10",fmt="%Y-%m-%d")

# test_partitions = TimeWindowPartitionsDefinition(start=datetime.datetime(2024,7,22,12,55), fmt="%Y-%m-%dT%HH_%MM", end=datetime.datetime(2024,7,22,13,55), schedule_type=None,
#                                 timezone=None, 
#                                 end_offset=0, minute_offset=None, hour_offset=None, day_offset=None, cron_schedule="*/15 * * * *")
replication_config = {
    "source": "ORACLE_DEV",
    "target": "SNOWFLAKE_DEV",
    "defaults": {"mode": "incremental", "primary_key":["BUSINESS_UNIT","INVOICE","LINE_SEQ_NUM","ACCOUNTING_DT","ACCT_ENTRY_TYPE","DISC_SUR_LVL",
                                                    "DISC_SUR_ID","LINE_DST_SEQ_NUM","TAX_AUTHORITY_CD"],
                                                    "update_key":"accounting_dt",
                                                    "object": "{stream_schema}_{stream_table}",
                                                    #"source_options":{"range":"1998-08-10,2024-07-22"}
                                                    },
    "streams": {
        "SYSADM8.PS_BI_ACCT_ENTRY": {"object": "ARTEMIS.___MIGRATION_STG.PEOPLESOFT_DEV_PS_BI_ACCT_ENTRY"},
    },
}


@sling_assets(replication_config=replication_config, partitions_def=test_partitions) #,backfill_policy=BackfillPolicy.multi_run(10))
def my_assets(context, sling: SlingResource):
    #this is not the way to do it: the SlingResource passed in is not what we think it is
    yield from test_sling.replicate(context=context)