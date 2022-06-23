import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from custom_operators.riot_matchdetails_toADLS_Operator import riot_matchDetailsToADLSOperator
from custom_transfers.azureToSnowflake import AzureDataLakeToSnowflakeTransferOperator
from sql_helpers.sql_queries import sqlQueries

SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
end_epoch= int(datetime.now().timestamp())
start_epoch = datetime.now() - timedelta(days=1)
start_epoch = int(start_epoch.timestamp())
CURRENT_TIME = datetime.now()

static_data = {
    'matchdetails' : ['STAGING_MATCHDETAILS', 'MY_AZURE_MATCH_DETAILS_STAGE', 'MATCH_DETAILS_FILEFORMAT', sqlQueries.CREATE_MATCHDETAILS_STAGING_TABLE_SQL_STRING, 'csv'], 
    'matchinfo' : ['STAGING_MATCHINFO', 'MY_AZURE_MATCH_INFO_STAGE', 'MATCH_INFO_FILEFORMAT', sqlQueries.CREATE_MATCHINFO_STAGING_TABLE_SQL_STRING, 'csv'],
    'players' : ['STAGING_PLAYERS', 'MY_AZURE_PLAYERS_STAGE', 'PLAYERS_FILEFORMAT', sqlQueries.CREATE_PLAYERS_STAGING_TABLE_SQL_STRING, 'json']
}

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 6, 20),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'schedule_interval': '@daily'
}

dag = DAG(dag_id = 'match_details_dag',
          default_args=default_args,
        #   concurrency=10,
          max_active_runs=1,
          description='Load and transform data in Azure with Airflow',
        #   schedule_interval='0 * * * *'
        )

staging_match_data = riot_matchDetailsToADLSOperator(
    task_id='staging_match_data',
    dag=dag,
    # pool="test_pool",
    riot_conn_id = 'riot_conn_id',
    azure_conn_id = 'azure_conn_id',
    region = 'na1',
    match_queue = "RANKED_SOLO_5x5",
    summoner_name = 'dild0wacker',
    tier = 'PLATINUM',
    division = 'I',
    page = 1,
    count = 30,
    player_count = 100,
    queue_type = 'ranked',
    end_epoch = CURRENT_TIME,
    ignore_headers=1
)


with dag as dag:
    with TaskGroup(group_id='azure_toSnowflake_paths') as azure_toSnowflake_paths:
        for data in static_data:
            with TaskGroup(group_id=f'path_{data}') as path:

                create_stage = SnowflakeOperator(
                    task_id='create_{}'.format(data),
                    dag=dag,
                    sql=static_data[data][3],
                    snowflake_conn_id=SNOWFLAKE_CONN_ID
                )

                copy_toSnowflake = AzureDataLakeToSnowflakeTransferOperator(
                    task_id='azure_{}_snowflake'.format(data),
                    dag=dag,
                    azure_keys=['{0}/{1}/{2}/{3}.{4}'.format(CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour, static_data[data][4])],
                    stage=static_data[data][1],
                    table=static_data[data][0],
                    file_format=static_data[data][2],
                    snowflake_conn_id=SNOWFLAKE_CONN_ID
                )

                create_stage >> copy_toSnowflake

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt && dbt deps && dbt run',
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test',
    dag=dag
)

start_operator = DummyOperator(task_id='start_execution',  dag=dag)
test_operator = DummyOperator(task_id='test_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> test_operator >> staging_match_data >> azure_toSnowflake_paths >> dbt_run >> dbt_test >> end_operator