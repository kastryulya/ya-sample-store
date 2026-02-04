from sqlalchemy import create_engine
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from config import MYSQL_HOST, MYSQL_DB_NAME, MYSQL_USERNAME, MYSQL_PASSWORD

def func():
    host = MYSQL_HOST
    db_name = MYSQL_DB_NAME
    username = MYSQL_USERNAME
    password = MYSQL_PASSWORD
    connection = create_engine('mysql://{username}:{password}@{url}/{db_name}?charset=utf8'
                            .format(username=username, password=password,
                                    url=host, db_name=db_name), echo=False)
    conn = connection.connect()
    result = conn.execute("SELECT max(base_grand_total) FROM sales_order_grid")

    return str(result.fetchone()[0])

dag = DAG(
    'our_second_dag', schedule_interval="*/30 * * * *", start_date= datetime(2022, 12, 8),)

process_dag = PythonOperator(
    task_id='func',
    python_callable=func,
    dag=dag)