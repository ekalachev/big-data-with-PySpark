# https://airflow.apache.org/docs/apache-airflow/stable/start/index.html
# export AIRFLOW_HOME=/mnt/d/Projects/Python/big-data-with-PySpark/5_apache-airflow

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from transformation import *


def etl():
    hotel_weather_df = extract_hotel_weather()
    expedia_df = extract_expedia()

    hotel_weather_df.show()
    expedia_df.show()


# define the orguments for the DAG
default_args = {
    'owner': 'eldar',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': True,
    'email': ['eldar_kalachev@epam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30)
}

# instantiate the DAG
dag = DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * *'
)

# define the etl task
etl_task = PythonOperator(
    task_id='etl_task',
    python_callable=etl,
    dag=dag
)


etl()
