import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from custom_operators.riot_datadragon_toADLS_Operator import riot_dataDragonToADLSOperator
from custom_transfers.azureToSnowflake import AzureDataLakeToSnowflakeTransferOperator

LEAGUE_VERSION = '12.9.1'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
CURRENT_TIME = datetime.now()

static_data = {
    'champions' : ['STAGING_DATADRAGON_CHAMPIONS', 'MY_AZURE_DATADRAGON_CHAMPIONS_STAGE'], 
    'maps' : ['STAGING_DATADRAGON_MAPS', 'MY_AZURE_DATADRAGON_MAPS_STAGE'],
    'items' : ['STAGING_DATADRAGON_ITEMS', 'MY_AZURE_DATADRAGON_ITEMS_STAGE'] , 
    'runesreforged' : ['STAGING_DATADRAGON_RUNES_REFORGED', 'MY_AZURE_DATADRAGON_RUNESREFORGED_STAGE'], 
    'summonerspells' : ['STAGING_DATADRAGON_SUMMONERSPELLS', 'MY_AZURE_DATADRAGON_SUMMONERSPELLS_STAGE']
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

dag = DAG(dag_id = 'league_of_legends_dag',
          default_args=default_args,
          max_active_runs=1,
          description='Load and transform data in Azure with Airflow',
          schedule_interval='0 * * * *'
        )


with dag as dag:
    with TaskGroup(group_id='paths') as paths:
        for data in static_data:
            with TaskGroup(group_id=f'path_{data}') as path:

                CREATE_DATADRAGON_STAGING_TABLE_SQL_STRING = (
                f"CREATE OR REPLACE TABLE {static_data[data][0]} (json_data variant);"
                )

                schedule = '@weekly'

                staging_dataDragon = riot_dataDragonToADLSOperator(
                    task_id='staging_{}'.format(data),
                    dag=dag,
                    riot_conn_id = 'riot_conn_id',
                    azure_blob = 'test',
                    azure_conn_id = 'azure_conn_id',
                    version = LEAGUE_VERSION,
                    data_url = data,
                    end_epoch = CURRENT_TIME,
                    ignore_headers=1
                )

                create_staging_tables = SnowflakeOperator(
                    task_id='Create_StagingSnowflake_Tables_{}'.format(data),
                    dag=dag,
                    sql=CREATE_DATADRAGON_STAGING_TABLE_SQL_STRING,
                    snowflake_conn_id=SNOWFLAKE_CONN_ID
                )

                azure_hashtags_toSnowflake = AzureDataLakeToSnowflakeTransferOperator(
                    task_id='azure_{}_snowflake'.format(data),
                    dag=dag,
                    azure_keys=['{0}/{1}/{2}/{3}.json'.format(CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour)],
                    stage=static_data[data][1],
                    table=static_data[data][0],
                    file_format='DATADRAGON_FILEFORMAT',
                    snowflake_conn_id=SNOWFLAKE_CONN_ID
                )

                staging_dataDragon >> create_staging_tables >> azure_hashtags_toSnowflake
    


start_operator = DummyOperator(task_id='start_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> paths >> end_operator