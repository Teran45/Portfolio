import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch


def query_postgresql():
    conn_string = "dbname='dataengineering' host='localhost' user='postgres' password='postgres'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select name, city from users", conn)
    df.to_csv("/home/teran45/airflow/postgresqldata.csv")
    print("-------Data Saved------")


def insert_elasticsearch():
    es = Elasticsearch({'127.0.0.1'})
    df = pd.read_csv('/home/teran45/airflow/postgresqldata.csv')
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="frompostgresql", document=doc)
    print(res)


default_args = {
    'owner': 'teran',
    'start_date': dt.datetime(2021, 11, 13),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}
with DAG('MyDBdag', default_args=default_args, schedule_interval=timedelta(minutes=5)) as dag:
    getData = PythonOperator(task_id='QueryPostgreSQL', python_callable=query_postgresql)
    insertData = PythonOperator(task_id='InsertDataElasticsearch', python_callable=insert_elasticsearch)
getData >> insertData

