from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import clickhouse_connect
from datetime import datetime, timedelta
import pandas as pd
import boto3
import os

from config import S3_ENDPOINT_URL, S3_AWS_ACCESS_KEY_ID, S3_AWS_SECRET_ACCESS_KEY
from config import CLICKHOUS_HOST, CLICKHOUS_PORT, CLICKHOUS_USER, CLICKHOUS_PASSWORD

default_args = {
    'owner': 'me',
    'start_date': datetime(2022,6,30),
    'end_date': datetime(2022,7,26),
    'max_active_runs': 1
}

dag = DAG(
    'site_visits',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

def download_objects_from_s3(**context):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=S3_AWS_SECRET_ACCESS_KEY
    )
    s3.download_file(
        Bucket='yc-metrics-theme',
        Key=f'{context["ds"]}-site-visits.csv',
        Filename=f'/tmp/{context["ds"]}-site_visits_downloaded_file.csv'
    )

def load_object_from_s3_to_clickhouse(**context):
    df = pd.read_csv(f'/tmp/{context["ds"]}-site_visits_downloaded_file.csv')

    client = clickhouse_connect.get_client(
        host=CLICKHOUS_HOST,
        port=CLICKHOUS_PORT,
        user=CLICKHOUS_USER,
        password=CLICKHOUS_PASSWORD,
        verify=False
    )

    client.insert_df('tmp.site_visits', df)

def etl_inside_clickhouse(**context):
    client = clickhouse_connect.get_client(
        host=CLICKHOUS_HOST,
        port=CLICKHOUS_PORT,
        user=CLICKHOUS_USER,
        password=CLICKHOUS_PASSWORD,
        verify=False
    )

    client.command(
        f"""
        insert into raw.site_visits
        select
            toDateTime(date) as date,
            toDateTime(timestamp) as timestamp,
            user_client_id,
            action_type,
            placement_type,
            placement_id,
            user_visit_url, 
            now() as insert_time,
            cityHash64(*) as hash
      FROM tmp.site_visits 
      where date = '{context['ds']}'
    """
    )

def remove_tmp_file(**context):
    os.remove(f'/tmp/{context["ds"]}-site_visits_downloaded_file.csv')

download_objects_from_s3 = PythonOperator(
    task_id='download_objects_from_s3',
    python_callable=download_objects_from_s3,
    dag=dag
)

load_object_from_s3_to_clickhouse = PythonOperator(
    task_id='load_object_from_s3_to_clockhouse',
    python_callable=load_object_from_s3_to_clickhouse,
    dag=dag
)

etl_inside_clickhouse = PythonOperator(
    task_id='etl_inside_clickhouse',
    python_callable=etl_inside_clickhouse,
    dag=dag
)

remove_tmp_file = PythonOperator(
    task_id='remove_tmp_file',
    python_callable=remove_tmp_file,
    dag=dag
)

download_objects_from_s3 >> load_object_from_s3_to_clickhouse >> etl_inside_clickhouse >> remove_tmp_file