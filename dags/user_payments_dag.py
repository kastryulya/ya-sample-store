from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from sqlalchemy import create_engine
import clickhouse_connect
from datetime import datetime, timedelta
import pandas as pd
import os

from config import CLICKHOUS_HOST, CLICKHOUS_PORT, CLICKHOUS_USER, CLICKHOUS_PASSWORD
from config import MYSQL_HOST, MYSQL_DB_NAME, MYSQL_USERNAME, MYSQL_PASSWORD

default_args = {
    'owner': 'me',
    'start_date': datetime(2022,6,30),
    'end_date': datetime(2022,7,26),
    'max_active_runs': 1
}

dag = DAG(
    'user_payments',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

def download_objects_from_mysql(**context):
    host = MYSQL_HOST
    db_name = MYSQL_DB_NAME
    username = MYSQL_USERNAME
    password = MYSQL_PASSWORD
    engine = create_engine('mysql://{username}:{password}@{url}/{db_name}?charset=utf8'
                            .format(username=username, password=password,
                                    url=host, db_name=db_name), echo=False)
    
    data = pd.read_sql(f"""
        select * from ya_sample_store.user_payments
        where date = '{context["ds"]}'
    """, 
    engine)
    
    data.to_csv(f'/tmp/{context["ds"]}-user_payments_downloaded_file.csv', index=False) 


def load_objects_from_mysql_to_clickhous(**context):
    df = pd.read_csv(f'/tmp/{context["ds"]}-user_payments_downloaded_file.csv')

    client = clickhouse_connect.get_client(
        host=CLICKHOUS_HOST,
        port=CLICKHOUS_PORT,
        user=CLICKHOUS_USER,
        password=CLICKHOUS_PASSWORD,
        verify=False
    )

    client.insert_df('tmp.user_payments', df)


def etl_inside_clickhouse(**context):
    client = clickhouse_connect.get_client(
        host=CLICKHOUS_HOST,
        port=CLICKHOUS_PORT,
        user=CLICKHOUS_USER,
        password=CLICKHOUS_PASSWORD,
        verify=False
    )

    client.command(f"""
    insert into raw.user_payments
    select 
        toDateTime(date) as date,
        toDateTime(timestamp) as timestamp,
        user_client_id,
        item,
        price,
        quantity,
        amount,
        discount,
        order_id,
        status,
        now() as insert_time,
        cityHash64(*) as hash  
    from tmp.user_payments
    where date = '{context["ds"]}'
    """)

def remove_tmp_file(**context):
    os.remove(f'/tmp/{context["ds"]}-user_payments_downloaded_file.csv')

download_objects_from_mysql = PythonOperator(
    task_id='download_objects_from_mysql',
    python_callable=download_objects_from_mysql,
    dag=dag
)

load_objects_from_mysql_to_clickhous = PythonOperator(
    task_id='load_objects_from_mysql_to_clickhous',
    python_callable=load_objects_from_mysql_to_clickhous,
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

download_objects_from_mysql >> load_objects_from_mysql_to_clickhous >> etl_inside_clickhouse >> remove_tmp_file