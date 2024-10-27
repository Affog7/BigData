from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_etl',
    default_args=default_args,
    description='A simple ETL pipeline for weather data',
    schedule_interval=timedelta(hours=3),  # J'aexécute toutes les 3 heures en tenant compte des infos de mise de 3 h de l'api 
                                           # https://openweathermap.org/api
)

def extract_data():
    api_key = '6be1e02a627aa6fa055f7eecec21be40'
    city = 'Paris'
    url = f'https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise ValueError(f"Failed to fetch data: {response.status_code}")

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    weather_list = data['list']
    rows = []
    for item in weather_list:
        row = {
            'dt': item['dt'],
            'temperature': item['main']['temp'],
            'humidity': item['main']['humidity'],
            'weather_description': item['weather'][0]['description'],
            'dt_txt': item['dt_txt']
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    return df.to_json()

def load_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    df = pd.read_json(data)
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    df.to_sql('weather', engine, if_exists='replace', index=False)

with dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task
