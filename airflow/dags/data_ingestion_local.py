import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


from datetime import datetime

from ingest_script import ingest_parquet_data_callable, ingest_csv_data_callable
from upload_s3_script import upload_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

# Postgres DB variables
PG_HOST = os.environ.get("PG_HOST")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")

# Amazon access variables
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Amazon resource variables
S3_BUCKET_NAME = "taxi-data-bucket-klr"

yt_local_workflow = DAG(
    "YellowTaxiLocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019,1,1),
    end_date = datetime(2021,1,1),
    max_active_runs= 1,
    catchup=True
)

fhv_local_workflow = DAG(
    "FHVLocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019,1,1),
    end_date = datetime(2020,1,1),
    max_active_runs= 1,
    catchup=True
)

gt_local_workflow = DAG(
    'GreenTaxiLocalIngestionDag',
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019,1,1),
    end_date = datetime(2021,1,1),
    max_active_runs= 1,
    catchup=True
)

zones_local_workflow = DAG(
    "ZonesLocalIngestionDag",
    schedule_interval="@once",
    start_date=datetime(2024,5,1)
)

yt_snowflake_workflow = DAG(
    "YellowTaxiSnowflakeIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019,1,1),
    end_date = datetime(2021,1,1),
    max_active_runs= 1,
    catchup=True
)

gt_snowflake_workflow = DAG (
    'GreenTaxiSnowflakeIngestionDag',
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019,1,1),
    end_date = datetime(2021,1,1),
    max_active_runs= 1,
    catchup=True
)

fhv_snowflake_workflow = DAG(
    "FHVSnowflakeIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019,1,1),
    end_date = datetime(2020,1,1),
    max_active_runs= 1,
    catchup=True
)

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

# Variables for the yellow taxi data
YT_URL_TEMPLATE = URL_PREFIX + '/' + 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YT_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_taxi_output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YT_TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}'

# Variables for the FHV data
FHV_URL_TEMPLATE = URL_PREFIX + '/' + 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TABLE_NAME_TEMPLATE = 'fhv_{{ execution_date.strftime(\'%Y-%m\') }}'

# Variables for the Green Trip data
GT_URL_TEMPLATE = URL_PREFIX + '/' + 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GT_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/green_taxi_output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GT_TABLE_NAME_TEMPLATE = 'green_{{ execution_date.strftime(\'%Y-%m\') }}'

# Variables for the zones data
ZONE_URL = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
ZONE_TABLE_NAME_TEMPLATE = 'taxi_zone_lookup'
ZONE_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + f'/{ZONE_TABLE_NAME_TEMPLATE}.csv'

# Variables for Snowflake statements
YT_SNOWFLAKE_TABLE = 'yellow_taxi_data'
FHV_SNOWFLAKE_TABLE = 'fhv_taxi_data'
GT_SNOWFLAKE_TABLE = 'green_taxi_data'
DATABASE_NAME = "TAXI_DATA"
SCHEMA_NAME = "PUBLIC"
STAGING_NAME = "TAXI_DATA"
YT_INSERT_STATMENT = f"""copy into {YT_SNOWFLAKE_TABLE} 
                     from '@"TAXI_DATA"."PUBLIC"."TAXI_DATA"/{YT_TABLE_NAME_TEMPLATE}.parquet'
                     file_format = (type = parquet) 
                     match_by_column_name = case_insensitive;"""
FHV_INSERT_STATEMENT = f"""copy into {FHV_SNOWFLAKE_TABLE} 
                     from '@"TAXI_DATA"."PUBLIC"."TAXI_DATA"/{FHV_TABLE_NAME_TEMPLATE}.parquet'
                     file_format = (type = parquet) 
                     match_by_column_name = case_insensitive;"""
GT_INSERT_STATEMENT = f"""copy into {GT_SNOWFLAKE_TABLE} 
                     from '@"TAXI_DATA"."PUBLIC"."TAXI_DATA"/{GT_TABLE_NAME_TEMPLATE}.parquet'
                     file_format = (type = parquet) 
                     match_by_column_name = case_insensitive;"""

# DAGs to download data, upload to s3, and remove from docker container

with yt_local_workflow:
    wget_yellow_taxi_data_task = BashOperator(
        task_id= 'wget_yellow_taxi_data',
        bash_command= f'wget {YT_URL_TEMPLATE} -O {YT_OUTPUT_FILE_TEMPLATE}'
    )

    upload_yellow_taxi_data_to_s3_task = PythonOperator(
        task_id='upload_yellow_taxi_data_to_s3',
        python_callable=upload_callable,
        op_kwargs=dict(
            file_name=YT_TABLE_NAME_TEMPLATE + '.parquet',
            file_location=YT_OUTPUT_FILE_TEMPLATE,
            bucket_name=S3_BUCKET_NAME,

        )
    )

    remove_yellow_taxi_file_from_directory_task = BashOperator(
        task_id='remove_yellow_taxi_file_from_directory',
        bash_command= f'rm {YT_OUTPUT_FILE_TEMPLATE}'
    )

    wget_yellow_taxi_data_task >> upload_yellow_taxi_data_to_s3_task >> remove_yellow_taxi_file_from_directory_task

with fhv_local_workflow:
    wget_fhv_data_task = BashOperator(
        task_id= 'wget_fhv_data',
        bash_command= f'wget {FHV_URL_TEMPLATE} -O {FHV_OUTPUT_FILE_TEMPLATE}'
    )


    wait_for_other_calls_to_s3_to_finish_task_fhv = ExternalTaskSensor(
        task_id='wait_for_other_calls_to_s3_to_finish_fhv',
        external_dag_id='YellowTaxiLocalIngestionDag',
        external_task_id='upload_yellow_taxi_data_to_s3',
        start_date=datetime(2019,1,1)
    )

    upload_fhv_data_to_s3_task = PythonOperator(
        task_id='upload_fhv_data_to_s3',
        python_callable=upload_callable,
        op_kwargs=dict(
            file_name=FHV_TABLE_NAME_TEMPLATE + '.parquet',
            file_location=FHV_OUTPUT_FILE_TEMPLATE,
            bucket_name=S3_BUCKET_NAME
        )
    )

    remove_fhv_file_from_directory_task = BashOperator(
        task_id='remove_fhv_file_from_directory',
        bash_command= f'rm {FHV_OUTPUT_FILE_TEMPLATE}'
    )

    wget_fhv_data_task >> wait_for_other_calls_to_s3_to_finish_task_fhv >> upload_fhv_data_to_s3_task >> remove_fhv_file_from_directory_task
    
with gt_local_workflow:
    wget_gt_data_task = BashOperator(
        task_id= 'wget_gt_data',
        bash_command= f'wget {GT_URL_TEMPLATE} -O {GT_OUTPUT_FILE_TEMPLATE}'
    )

    wait_for_other_calls_to_s3_to_finish_task_gt = ExternalTaskSensor(
        task_id='wait_for_other_calls_to_s3_to_finish_gt',
        external_dag_id='YellowTaxiLocalIngestionDag',
        external_task_id='upload_yellow_taxi_data_to_s3',
        start_date=datetime(2019,1,1)
    )

    upload_gt_data_to_s3_task = PythonOperator(
        task_id='upload_gt_data_to_s3',
        python_callable=upload_callable,
        op_kwargs=dict(
            file_name=GT_TABLE_NAME_TEMPLATE + '.parquet',
            file_location=GT_OUTPUT_FILE_TEMPLATE,
            bucket_name=S3_BUCKET_NAME
        )
    )

    remove_gt_file_from_directory_task = BashOperator(
        task_id='remove_gt_file_from_directory',
        bash_command= f'rm {GT_OUTPUT_FILE_TEMPLATE}'
    )

    wget_gt_data_task >> wait_for_other_calls_to_s3_to_finish_task_gt >> upload_gt_data_to_s3_task >> remove_gt_file_from_directory_task

with zones_local_workflow:
    wget_zones_data_task = BashOperator(
        task_id= 'wget_zones_data',
        bash_command= f'wget {ZONE_URL} -O {ZONE_OUTPUT_FILE_TEMPLATE}'
    )

    wait_for_other_calls_to_s3_to_finish_task_zones = ExternalTaskSensor(
        task_id='wait_for_other_calls_to_s3_to_finish_zones',
        external_dag_id='GreenTaxiLocalIngestionDag',
        external_task_id='upload_gt_data_to_s3'
    )

    upload_zone_data_to_s3_task = PythonOperator(
        task_id='upload_zone_data_to_s3',
        python_callable=upload_callable,
        op_kwargs=dict(
            file_name=ZONE_TABLE_NAME_TEMPLATE + '.csv',
            file_location=ZONE_OUTPUT_FILE_TEMPLATE,
            bucket_name=S3_BUCKET_NAME
        )
    )

    remove_zone_file_from_directory_task = BashOperator(
        task_id='remove_zone_file_from_directory',
        bash_command= f'rm {ZONE_OUTPUT_FILE_TEMPLATE}'
    )

    wget_zones_data_task  >> wait_for_other_calls_to_s3_to_finish_task_zones >> upload_zone_data_to_s3_task >> remove_zone_file_from_directory_task

# DAGs to upload staged data into snowflake

with yt_snowflake_workflow:
    yt_snowflake_external_table_task = SnowflakeOperator(
        task_id='yt_snowflake_external_table',
        snowflake_conn_id='snowflake',
        sql=YT_INSERT_STATMENT
    )

    yt_snowflake_external_table_task

with gt_snowflake_workflow:
    gt_snowflake_external_table_task = SnowflakeOperator(
        task_id='gt_snowflake_external_table',
        snowflake_conn_id='snowflake',
        sql=GT_INSERT_STATEMENT
    )

    gt_snowflake_external_table_task

with fhv_snowflake_workflow:
    fhv_snowflake_external_table_task = SnowflakeOperator(
        task_id='fhv_snowflake_external_table',
        snowflake_conn_id='snowflake',
        sql=FHV_INSERT_STATEMENT
    )

    fhv_snowflake_workflow