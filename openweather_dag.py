from urllib import response
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import json


def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor


def etl(**context):
    # api information
    lat = 37.5665
    lon = 126.9780
    api_key = context["params"]["api_key"]
    link = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts"

    # getting data
    response = requests.get(link)
    data = json.load(response.text)

    ret = []
    for d in data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        ret.append("('{}', {}, {}, {})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))
    
    cur = get_Redshift_connection()
    insert_sql = """DELETE FROM dicobomb.weather_forecast; INSERT INTO dicobomb.weather_forecast VALUES """ + ','.join(ret)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except:
        cur.execute("ROLLBACK;")
        raise

    


openweather_dyc_dag = DAG(
    dag_id = 'openweather_dyc_dag',
    start_date = datetime(2022,8,29), # 날짜가 미래인 경우 실행이 안됨
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