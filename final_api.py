from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

from python_functions.funcs import api_to_bronze, api_to_silver


default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

dag = DAG(
    dag_id="final_api",
    description="Load data from API to Data Lake",
    schedule_interval="@daily",
    start_date=datetime(2021, 8, 22,23,59),
    default_args=default_args
)

api_bronze=PythonOperator(
    task_id='api_bronze',
    dag=dag,
    python_callable=api_to_bronze
)

api_silver=PythonOperator(
    task_id='api_silver',
    dag=dag,
    python_callable=api_to_silver
)

dummy1 = DummyOperator(
    task_id='start_load_to_bronze',
    dag=dag
)

dummy2 = DummyOperator(
    task_id='finish_load_to_bronze',
    dag=dag
)

dummy3 = DummyOperator(
    task_id='start_load_to_silver',
    dag=dag
)

dummy4 = DummyOperator(
    task_id='finish_load_to_silver',
    dag=dag
)
dummy1 >> api_bronze >>dummy2 >> dummy3 >> api_silver >>dummy4