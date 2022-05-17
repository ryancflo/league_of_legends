import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from custom_operators.riot_datadragon_toADLS_Operator import riot_dataDragonToADLSOperator
from custom_transfers.azureToSnowflake import AzureDataLakeToSnowflakeTransferOperator
# from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
# from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

LEAGUE_VERSION = '12.9.1'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
CURRENT_TIME = datetime.today()


d = datetime.today() - timedelta(days = 1)
start_epoch = int(d.timestamp())

static_data = ['champions', 'maps', 'items', 'masteries', 'runes']
file_format = ['DATADRAGON_CHAMPIONS_FILEFORMAT', 'DATADRAGON_MAPS_FILEFORMAT', 'DATADRAGON_ITEMS_FILEFORMAT', 'DATADRAGON_MASTERIES_FILEFORMAT', 'DATADRAGON_RUNES_FILEFORMAT']

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

def create_dag(dag_id, schedule, task_name, dag, default_args):

    def hello_world_py(*args):
        print('Hello World')
        print('This is DAG: {}'.format(task_name))

    with dag:
        staging_dataDragon = riot_dataDragonToADLSOperator(
            task_id='staging_{}'.format(task_name),
            dag=dag,
            riot_conn_id = 'riot_conn_id',
            azure_blob = 'test',
            azure_conn_id = 'azure_conn_id',
            version = LEAGUE_VERSION,
            data_url = task_name,
            ignore_headers=1
        )

        azure_hashtags_toSnowflake = AzureDataLakeToSnowflakeTransferOperator(
            task_id='azure_{}_snowflake'.format(task_name),
            dag=dag,
            azure_keys=['{0}/{1}/{2}/{3}/{4}.json'.format("hashtags_data", CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour)],
            stage='MY_AZURE_TWITTER_STAGE',
            table='STAGING_TWITTER_HASHTAGS',
            file_format='TWITTER_FILEFORMAT',
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )
        
        staging_dataDragon >> azure_hashtags_toSnowflake
    return dag


staging_dataDragon = riot_dataDragonToADLSOperator(
    task_id='staging_test'
    dag=dag,
    riot_conn_id = 'riot_conn_id',
    azure_blob = 'test',
    azure_conn_id = 'azure_conn_id',
    version = LEAGUE_VERSION,
    data_url = champions,
    ignore_headers=1
)

azure_toSnowflake = AzureDataLakeToSnowflakeTransferOperator(
    task_id='azure_snowflake',
    dag=dag,
    azure_keys=['{0}/{1}/{2}/{3}/{4}.json'.format("datadragon-champions", CURRENT_TIME.year, CURRENT_TIME.month, CURRENT_TIME.day, CURRENT_TIME.hour)],
    stage='MY_AZURE_DATADRAGON_CHAMPIONS_STAGE',
    table='STAGING_TWITTER_HASHTAGS',
    file_format='DATADRAGON_CHAMPIONS_FILEFORMAT',
    snowflake_conn_id=SNOWFLAKE_CONN_ID
)


# for data in static_data:
#     dag_id = 'loop_hello_world_{}'.format(data)

#     schedule = '@weekly'

#     globals()[dag_id] = create_dag(dag_id,
#                                 schedule,
#                                 data,
#                                 dag,
#                                 default_args)


# with dag as dag:
#     with TaskGroup(group_id='load_toSnowflakeStaging') as load_toSnowflakeStaging:
#         snowflake_op_sql_str = SnowflakeOperator(
#             task_id='snowflake_op_sql_str',
#             dag=dag,
#             sql=CREATE_TABLE_SQL_STRING,
#             warehouse=SNOWFLAKE_WAREHOUSE,
#             database=SNOWFLAKE_DATABASE,
#             schema=SNOWFLAKE_SCHEMA,
#             role=SNOWFLAKE_ROLE,
#         )
            
#         snowflake_op_with_params = SnowflakeOperator(
#             task_id='snowflake_op_with_params',
#             dag=dag,
#             sql=SQL_INSERT_STATEMENT,
#             parameters={"id": 56},
#             warehouse=SNOWFLAKE_WAREHOUSE,
#             database=SNOWFLAKE_DATABASE,
#             schema=SNOWFLAKE_SCHEMA,
#             role=SNOWFLAKE_ROLE,
#         )

#         []

start_operator = DummyOperator(task_id='start_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> staging_dataDragon >> azure_toSnowflake >> end_operator