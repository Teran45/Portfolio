import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db


def query_postgresql():
    conn_string = "dbname='spark_labs' host='localhost' user='postgres' password='postgres'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from covid", conn)
    df.to_csv("/home/teran45/airflow/covid.csv")
    print("-------Data Saved------")


default_args = {
    'owner': 'teran45',
    'start_date': dt.datetime(2022, 5, 21),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=10000),
}
with DAG('Covid', default_args=default_args, schedule_interval=timedelta(minutes=10000)) as dag:
    getData = PythonOperator(task_id='QueryPostgreSQL', python_callable=query_postgresql)


