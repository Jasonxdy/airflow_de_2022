from urllib import response
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json


def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor


def etl(**context):
    '''
    link = context["params"]["url"]
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)
    f = requests.get(link)
    return (f.text)
    '''
    # api information
    lat = 37.56
    lon = 127.01
    api_key = context["params"]["api_key"]
    link = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric"


    res = requests.get(link).json()
    res[]
    
    


'''
def transform(**context):
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = text.split("\n")[1:]
    return lines

def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    sql = "BEGIN; DELETE FROM {schema}.{table};".format(schema=schema, table=table)
    for line in lines:
        if line != "":
            (name, gender) = line.split(",")
            print(name, "-", gender)
            sql += f"""INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');"""
    sql += "END;"

    logging.info(sql)
    cur.execute(sql)
'''


openweather_dyc_dag = DAG(
    dag_id = 'openweather_dyc_dag',
    start_date = datetime(2022,8,18), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 2 * * *',  # 적당히 조절
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


etl = PythonOperator(
    task_id = 'etl',
    python_callable = etl,
    params = {
        'api_key':  Variable.get("weather_api_key")
    },
    provide_context=True,
    dag = openweather_dyc_dag)