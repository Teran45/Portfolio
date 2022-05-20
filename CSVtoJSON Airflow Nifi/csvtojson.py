import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd


def csv_to_json(**context):
    df = pd.read_csv('/home/teran45/airflow/data.csv')
    for i, r in df.iterrows():
        print(r['name'])
    df.to_json('fromAirflow.json', orient='records')


# csv_to_json()
default_args = {
    'owner': 'teran45',
    'start_date': dt.datetime(2021, 11, 9),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}
with DAG('MyCSVDAG', default_args=default_args, schedule_interval=timedelta(minutes=5)) as dag:
    print_starting = BashOperator(task_id='starting', bash_command='echo "I am reading the CSV now....."')
    CSVJson = PythonOperator(task_id='convertCSVtoJson', python_callable=csv_to_json)
