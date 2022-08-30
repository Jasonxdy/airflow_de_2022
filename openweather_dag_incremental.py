import secrets
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
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    link = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts"

    # getting data
    response = requests.get(link)
    data = json.load(response.text)

    ret = []
    for d in data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        ret.append("('{}', {}, {}, {})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))
    
    cur = get_Redshift_connection()


    # 임시 테이블 생성 및 값 넣기
    temp_insert_sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};
    CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS);
    INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table};
    INSERT INTO {schema}.temp_{table} VALUES """ + ','.join(ret)

    try:
        cur.execute(temp_insert_sql)
        cur.excute("COMMIT;")
    except:
        cur.execute("ROLLBACK;")
        raise

    
    # 임시 테이블에서 중복값 제거한 뒤 원본 테이블로 로드
    insert_sql = f"""
    DELETE FROM {schema}.{table};
    INSERT INTO {schema}.{table}
    SELECT date, temp, min_temp, max_temp FROM (
        SELECT date, temp, min_temp, max_temp, ROW_NUMBER() OVER (PARTITON BY date ORDER BY updated_time DESC) AS row_num
        FROM {schema}.{table}
    ) WHERE row_num = 1;"""

    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except:
        cur.execute("ROLLBACK;")
        raise
    

openweather_dyc_dag_incremental = DAG(
    dag_id = 'openweather_dyc_dag_incremental',
    start_date = datetime(2022,8,29), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 2 * * *',  # 적당히 조절
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3)
    }
)


etl = PythonOperator(
    task_id = 'etl',
    python_callable = etl,
    params = {
        'api_key': Variable.get("weather_api_key"),
        'schema' : 'dicobomb',
        'table' : 'weather_forecast'
    },
    provide_context=True,
    dag = openweather_dyc_dag_incremental)