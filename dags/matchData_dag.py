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
    'matchdetails' : ['STAGING_MATCHDETAILS', 'MY_AZURE_MATCH_DETAILS_STAGE', 'MATCH_DETAILS_FILEFORMAT', sqlQueries.CREATE_MATCHDETAILS_STAGING_TABLE_SQL_STRING], 
    'matchinfo' : ['STAGING_MATCHINFO', 'MY_AZURE_MATCH_INFO_STAGE', 'MATCH_INFO_FILEFORMAT', sqlQueries.CREATE_MATCHINFO_STAGING_TABLE_SQL_STRING],
    # 'players' : ['STAGING__PLAYERS', 'MY_AZURE_PLAYERS_STAGE', 'PLAYERS_FILEFORMAT', sqlQueries.CREATE_PLAYERS_STAGING_TABLE_SQL_STRING]
}

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 5, 23),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(dag_id = 'match_details_dag',
          default_args=default_args,
          concurrency=10,
          max_active_runs=1,
          description='Load and transform data in Azure with Airflow',
          schedule_interval='0 * * * *'
        )

# staging_matchData = riot_matchDetailsToADLSOperator(
#     task_id='staging',
#     dag=dag,
#     riot_conn_id = 'riot_conn_id',
#     azure_conn_id = 'azure_conn_id',
#     region = 'na1',
#     queue = "RANKED_SOLO_5x5",
#     count = 3,
#     end_epoch = CURRENT_TIME,
#     ignore_headers=1
# )

staging_match_data = riot_matchDetailsToADLSOperator(
    task_id='staging_match_data',
    dag=dag,
    # pool="test_pool",
    riot_conn_id = 'riot_conn_id',
    azure_conn_id = 'azure_conn_id',
    region = 'na1',
    match_queue = "RANKED_SOLO_5x5",
    summoner_name = 'dild0wacker',
    count = 5,
    end_epoch = CURRENT_TIME,
    ignore_headers=1
)


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
                    azure_keys=['{0}/{1}/{2}/{3}.csv'.format(CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour)],
                    stage=static_data[data][1],
                    table=static_data[data][0],
                    file_format=static_data[data][2],
                    snowflake_conn_id=SNOWFLAKE_CONN_ID
                )

                create_stage >> copy_toSnowflake

test_task = BashOperator(
    task_id='chck',
    bash_command='cd /opt/airflow/dbt && ls',
    dag=dag
)

task_1 = BashOperator(
    task_id='daily_transform',
    bash_command='cd /opt/airflow/dbt && dbt deps && dbt run --target dev',
    dag=dag
)

# task_2 = BashOperator(
#     task_id='daily_analysis',
#     bash_command='cd /dbt && dbt run --models analysis --profiles-dir .',
#     env={
#         'dbt_user': '{{ var.value.dbt_user }}',
#         'dbt_password': '{{ var.value.dbt_password }}',
#         **os.environ
#     },
#     dag=dag
# )

start_operator = DummyOperator(task_id='start_execution',  dag=dag)
test_operator = DummyOperator(task_id='test_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> test_operator >> staging_match_data >> paths >> test_task >> task_1 >> end_operator