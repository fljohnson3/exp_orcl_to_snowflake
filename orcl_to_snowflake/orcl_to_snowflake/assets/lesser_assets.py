from dagster_embedded_elt.sling import (
    SlingConnectionResource,
    SlingResource,
    sling_assets
)
import json
import datetime
import snowflake.connector
import oracledb
import re
from dagster import asset, BackfillPolicy, MonthlyPartitionsDefinition, HourlyPartitionsDefinition,TimeWindowPartitionsDefinition

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
with open('/etc/PythonConfig/config.json') as config_file: 
    settings = json.load(config_file)

source = SlingConnectionResource(
            name="ORACLE_DEV",
            type="oracle",
            host='192.168.233.135',
            password=settings['password_Oracle'],
            port='1521',
            service_name='devl.fdevdba3.sdidcnet.oraclevcn.com',
            user='SDIRPA'
)

target = SlingConnectionResource(
            name="SNOWFLAKE_DEV",
            type="snowflake",
            user=settings['snowflake_conn']['sf_user'],
            account=settings['snowflake_conn']['sf_acct'],
            database='ARTEMIS',
            password=settings['snowflake_conn']['sf_pwd'],
            role="ACCOUNTADMIN"
)

test_sling = SlingResource(
    connections=[
        source,
        target,
    ]
)

test_partitions = MonthlyPartitionsDefinition(start_date="1998-08-10",fmt="%Y-%m-%d")

# test_partitions = TimeWindowPartitionsDefinition(start=datetime.datetime(2024,7,22,12,55), fmt="%Y-%m-%dT%HH_%MM", end=datetime.datetime(2024,7,22,13,55), schedule_type=None,
#                                 timezone=None, 
#                                 end_offset=0, minute_offset=None, hour_offset=None, day_offset=None, cron_schedule="*/15 * * * *")
proto_replication_config = {
    "source": "ORACLE_DEV",
    "target": "SNOWFLAKE_DEV",
    "defaults": {"mode": "incremental", 
                 #"primary_key":["BUSINESS_UNIT","INVOICE","LINE_SEQ_NUM","ACCOUNTING_DT","ACCT_ENTRY_TYPE","DISC_SUR_LVL",
                  #                                  "DISC_SUR_ID","LINE_DST_SEQ_NUM","TAX_AUTHORITY_CD"],
                                                    "update_key":"accounting_dt",
                                                    "object": "{stream_schema}_{stream_table}",
                                                    #"source_options":{"range":"1998-08-10,2024-07-22"}
                                                    },
    "streams": {
       # "SYSADM8.PS_BI_ACCT_ENTRY": {"object": "ARTEMIS.___MIGRATION_STG.PEOPLESOFT_DEV_PS_BI_ACCT_ENTRY"},
    },
}

def get_orcl_keys(orcl_cursor,datecatcher,source_table):
    can_peek_table = source_table.replace(".","_")
    creator='CREATE TABLE {temp} AS SELECT * FROM  {tbl_name} WHERE ROWNUM=1'.format(temp=can_peek_table,tbl_name=source_table)
    
    try:
        orcl_cursor.execute(creator)
    except Exception as ex:
        None
    qry='''
        SELECT REPLACE(REPLACE(DBMS_METADATA. GET_DDL('TABLE', '{tbl_name}', '{user}'),'NOT NULL',''),'ENABLE','') FROM DUAL
        '''.format(tbl_name=can_peek_table,user='SDIRPA')
    orcl_cursor.execute(qry)
    ddl=str(orcl_cursor.fetchone()[0])
    m=datecatcher.search(ddl)
    rv=None
    if m == None:
        print(ddl)
    else:
        rv=(m.group(1))
    return rv

#@asset
def get_tables():

    datecatcher=re.compile("\"(.*)\".+?DATE",re.MULTILINE + re.IGNORECASE)
    oracledb.init_oracle_client()
    ORACLE_USER = 'SDIRPA'
    ORACLE_PASSWORD = settings['password_Oracle']
    ORACLE_CONNECTION = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, host='192.168.233.135',service_name='devl.fdevdba3.sdidcnet.oraclevcn.com')
    ORACLE_CURSOR = ORACLE_CONNECTION.cursor()


    snowflake.connector.paramstyle='numeric'
    snow_connector = snowflake.connector.connect(
        user=settings['snowflake_conn']['sf_user'],
        account=settings['snowflake_conn']['sf_acct'],
        password=settings['snowflake_conn']['sf_pwd'],
        role="ACCOUNTADMIN"

    )
    snow_cursor = snow_connector.cursor()
    qry='''
    select  
    (CASE WHEN table_schema='PRD' THEN 'FSPRD@'
        WHEN table_schema='DEV' THEN 'DEV@' 
        ELSE TABLE_CATALOG||'.' END)
        ||
        (CASE WHEN table_catalog='PEOPLESOFT' THEN 'SYSADM8' ELSE TABLE_CATALOG||'.'||table_name end)
        ||'.'||table_name
    as oracle_source,
    table_catalog, table_schema, table_name, src_row_count, src_bytes,
    replace(artemis_target_object,'.MIGRATION_STG','.___MIGRATION_STG') as artemis_target_name
    
    from ARTEMIS.___MIGRATION_STG.DATA_TRANSPORT_QUEUE
    where table_catalog = '{catalog}' and table_schema='{schema}' and lower(table_name) not in ('ps_bi_acct_entry')
    ORDER BY 2,3,4
    LIMIT 3
    '''.format(catalog="PEOPLESOFT",schema="DEV")
    
    snow_cursor.execute(qry)
    collection=snow_cursor.fetchall()
    for r in collection:
        src = str(r[0])
        tgt = str(r[6])
        #get the Oracle source environment and the Oracle source table name
        bits = src.split('@')
        src_env=bits[0]
        src_tbl=bits[1]
        #get the SF target catalog.schema and the target table name
        bits= tgt.split('.')
        tgt_schema=bits[0]+"."+bits[1]
        tgt_tbl=bits[2]
        #'generic_oracle_table_to_snow.py'
        command=['./bin/python3','generic_oracle_table_to_snow.py',src_env,src_tbl, tgt_schema, tgt_tbl,'PEOPLESOFT.PRD.PS_REPLEN']
        #that last param was a CSV translator. TODO:point of research is how Snowflake uploads a DataFrame
        der_keyring=get_orcl_keys(ORACLE_CURSOR,datecatcher,src_tbl)
        proto_replication_config["streams"][src_tbl]={"object":tgt}
        if der_keyring:
            proto_replication_config["streams"][src_tbl]["update_key"]=der_keyring
    return proto_replication_config


@sling_assets(replication_config=get_tables(),name="multiple", partitions_def=test_partitions) #,backfill_policy=BackfillPolicy.multi_run(10))
def short_dbs(context, sling: SlingResource):
    #this is not the way to do it: the SlingResource passed in is not what we think it is
    yield from test_sling.replicate(context=context)