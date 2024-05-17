import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from ingest_script import ingest_parquet_data_callable, ingest_csv_data_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

PG_HOST = os.environ.get("PG_HOST")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

yt_local_workflow = DAG(
    "YellowTaxiLocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019,1,1),
    end_date = datetime(2021,1,1),
    catchup=True
)

fhv_local_workflow = DAG(
    "FHVLocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019,1,1),
    end_date = datetime(2020,1,1),
    catchup=True
)

zones_local_workflow = DAG(
    "ZonesLocalIngestionDag",
    schedule_interval="@once",
    start_date=datetime(2024,5,1)
)


URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
YT_URL_TEMPLATE = URL_PREFIX + '/' + 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YT_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_taxi_output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YT_TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}'

FHV_URL_TEMPLATE = URL_PREFIX + '/' + 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TABLE_NAME_TEMPLATE = 'fhv_{{ execution_date.strftime(\'%Y-%m\') }}'

ZONE_URL = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
ZONE_TABLE_NAME_TEMPLATE = 'taxi_zone_lookup'
ZONE_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + f'/{ZONE_TABLE_NAME_TEMPLATE}.csv'

with yt_local_workflow:
    wget_yellow_taxi_data_task = BashOperator(
        task_id= 'wget_yellow_taxi_data',
        bash_command= f'wget {YT_URL_TEMPLATE} -O {YT_OUTPUT_FILE_TEMPLATE}'
    )


    ingest_yellow_taxi_data_task = PythonOperator(
        task_id='ingest_yellow_taxi_data',
        python_callable=ingest_parquet_data_callable,
        op_kwargs=dict(
            user= PG_USER,
            password= PG_PASSWORD,
            host= PG_HOST,
            port= PG_PORT,
            db= PG_DATABASE,
            table_name=YT_TABLE_NAME_TEMPLATE,
            parquet_file_name=YT_OUTPUT_FILE_TEMPLATE
        )
    )

    remove_yt_file_from_directory_task = BashOperator(
        task_id='remove_yt_file_from_directory',
        bash_command= f'rm {YT_OUTPUT_FILE_TEMPLATE}'
    )

    wget_yellow_taxi_data_task >> ingest_yellow_taxi_data_task >> remove_yt_file_from_directory_task

with fhv_local_workflow:
    wget_fhv_data_task = BashOperator(
        task_id= 'wget_fhv_data',
        bash_command= f'wget {FHV_URL_TEMPLATE} -O {FHV_OUTPUT_FILE_TEMPLATE}'
    )

    ingest_fhv_data_task = PythonOperator(
        task_id='ingest_fhv_data',
        python_callable=ingest_parquet_data_callable,
        op_kwargs=dict(
            user= PG_USER,
            password= PG_PASSWORD,
            host= PG_HOST,
            port= PG_PORT,
            db= PG_DATABASE,
            table_name=FHV_TABLE_NAME_TEMPLATE,
            parquet_file_name=FHV_OUTPUT_FILE_TEMPLATE
        )
    )

    remove_fhv_file_from_directory_task = BashOperator(
        task_id='remove_fhv_file_from_directory',
        bash_command= f'rm {FHV_OUTPUT_FILE_TEMPLATE}'
    )

    wget_fhv_data_task >> ingest_fhv_data_task >> remove_fhv_file_from_directory_task

with zones_local_workflow:
    wget_zones_data_task = BashOperator(
        task_id= 'wget_zones_data',
        bash_command= f'wget {ZONE_URL} -O {ZONE_OUTPUT_FILE_TEMPLATE}'
    )

    ingest_zone_data_task = PythonOperator(
        task_id= 'ingest_zone_data',
        python_callable=ingest_csv_data_callable,
        op_kwargs=dict(
            user= PG_USER,
            password= PG_PASSWORD,
            host= PG_HOST,
            port= PG_PORT,
            db= PG_DATABASE,
            table_name=ZONE_TABLE_NAME_TEMPLATE,
            csv_file_name=ZONE_OUTPUT_FILE_TEMPLATE
        )
    )

    remove_zone_file_from_directory_task = BashOperator(
        task_id='remove_zone_file_from_directory',
        bash_command= f'rm {ZONE_OUTPUT_FILE_TEMPLATE}'
    )

    wget_zones_data_task >> ingest_zone_data_task >> remove_zone_file_from_directory_task