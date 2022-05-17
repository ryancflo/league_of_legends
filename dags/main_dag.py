import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from custom_operators.riot_matchdetails_toADLS_Operator import riot_matchDetailsToADLSOperator
from custom_transfers.azureToSnowflake import AzureDataLakeToSnowflakeTransferOperator
# from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
# from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

LEAGUE_VERSION = '12.9.1'

d = datetime.today() - timedelta(days = 1)
start_epoch = int(d.timestamp())

static_data = ['champions', 'map', 'items', 'masteries', 'runes']

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 3, 31, 21),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(dag_id = 'league_of_legends_dag',
          default_args=default_args,
          max_active_runs=1,
          description='Load and transform data in Azure with Airflow',
          schedule_interval='0 * * * *'
        )

staging_matchDetails = riot_matchDetailsToADLSOperator(
    task_id='staging',
    dag=dag,
    riot_conn_id = 'riot_conn_id',
    # start_epoch = start_epoch,
    end_epoch = datetime.now(),
    count = 5,
    summoner_name = 'dild0wacker',
    azure_blob = 'test',
    azure_conn_id = 'azure_conn_id',
    region = 'na1',
    ignore_headers=1
)

staging_dataDragon = riot_matchDetailsToADLSOperator(
    task_id='staging2',
    dag=dag,
    riot_conn_id = 'riot_conn_id',
    azure_blob = 'test',
    azure_conn_id = 'azure_conn_id',
    version = LEAGUE_VERSION,
    data_url = static_data,
    ignore_headers=1
)

with dag as dag:
    with TaskGroup(group_id='load_toSnowflakeStaging') as load_toSnowflakeStaging:
        snowflake_op_sql_str = SnowflakeOperator(
            task_id='snowflake_op_sql_str',
            dag=dag,
            sql=CREATE_TABLE_SQL_STRING,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
        )
            
        snowflake_op_with_params = SnowflakeOperator(
            task_id='snowflake_op_with_params',
            dag=dag,
            sql=SQL_INSERT_STATEMENT,
            parameters={"id": 56},
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
        )

        []

start_operator = DummyOperator(task_id='start_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> end_operator