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
    'matchdetails' : ['STAGING_DATADRAGON_MATCHDETAILS', 'MY_AZURE_MATCH_DETAILS_STAGE', 'MATCH_DETAILS_FILEFORMAT', sqlQueries.CREATE_MATCHDETAILS_STAGING_TABLE_SQL_STRING], 
    'matchinfo' : ['STAGING_DATADRAGON_MATCHINFO', 'MY_AZURE_MATCH_INFO_STAGE', 'MATCH_INFO_FILEFORMAT', sqlQueries.CREATE_MATCHINFO_STAGING_TABLE_SQL_STRING],
    'players' : ['STAGING_DATADRAGON_PLAYERS', 'MY_AZURE_PLAYERS_STAGE', 'PLAYERS_FILEFORMAT', sqlQueries.CREATE_PLAYERS_STAGING_TABLE_SQL_STRING]
}

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 3, 31, 21),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(dag_id = 'match_details_dag',
          default_args=default_args,
          max_active_runs=1,
          description='Load and transform data in Azure with Airflow',
          schedule_interval='0 * * * *'
        )

staging_matchData = riot_matchDetailsToADLSOperator(
    task_id='staging',
    dag=dag,
    riot_conn_id = 'riot_conn_id',
    # start_epoch = start_epoch,
    # end_epoch = datetime.now(),
    queue = "RANKED_SOLO_5x5",
    count = 3,
    azure_conn_id = 'azure_conn_id',
    region = 'na1',
    ignore_headers=1
)

# create_matchdinfo = SnowflakeOperator(
#     task_id='create_matchdinfo',
#     dag=dag,
#     sql=sqlQueries.CREATE_MATCHINFO_STAGING_TABLE_SQL_STRING,
#     snowflake_conn_id=SNOWFLAKE_CONN_ID
# )

with dag as dag:
    with TaskGroup(group_id='paths') as paths:
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
                    azure_keys=['{0}/{1}/{2}/{3}.json'.format(CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour)],
                    stage=static_data[data][1],
                    table=static_data[data][0],
                    file_format=static_data[data][2],
                    snowflake_conn_id=SNOWFLAKE_CONN_ID
                )

                create_stage >> copy_toSnowflake

task_1 = BashOperator(
    task_id='daily_transform',
    bash_command='cd /dbt && dbt run --models transform --profiles-dir .',
    env={
        'dbt_user': '{{ var.value.dbt_user }}',
        'dbt_password': '{{ var.value.dbt_password }}',
        **os.environ
    },
    dag=dag
)

task_2 = BashOperator(
    task_id='daily_analysis',
    bash_command='cd /dbt && dbt run --models analysis --profiles-dir .',
    env={
        'dbt_user': '{{ var.value.dbt_user }}',
        'dbt_password': '{{ var.value.dbt_password }}',
        **os.environ
    },
    dag=dag
)

start_operator = DummyOperator(task_id='start_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> staging_matchData >> paths >> end_operator