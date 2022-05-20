import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd


def clean_scooter():
    df = pd.read_csv('scooter.csv')
    df.drop(columns=['region_id'], inplace=True)
    df.columns = [x.lower() for x in df.columns]
    df['started_at'] = pd.to_datetime(df['started_at'], format='%m/%d/%Y %H:%M')
    df.to_csv('cleanscooter.csv')


def filter_data():
    df = pd.read_csv('cleanscooter.csv')
    from_d = '2019-05-23'
    tod = '2019-06-03'
    tofrom = df[(df['started_at'] > from_d) & (df['started_at'] < tod)]
    tofrom.to_csv('may23-june3.csv')


default_args = {
    'owner': 'teran45',
    'start_date': dt.datetime(2021, 11, 18),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('CleanData',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),  # '0 * * * *',
         ) as dag:
    cleanData = PythonOperator(task_id='clean',
                               python_callable=clean_scooter)

    selectData = PythonOperator(task_id='filter',
                                python_callable=filter_data)

    moveFile = BashOperator(task_id='move',
                            bash_command='mv /home/teran45/may23-june3.csv /home/teran45/Desktop')

cleanData >> selectData >> moveFile
