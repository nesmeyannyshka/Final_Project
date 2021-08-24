from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

from python_functions.funcs import dwh

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

dag = DAG(
    dag_id="final_gp",
    description="Load data to DWH",
    schedule_interval="@daily",
    start_date=datetime(2021, 8, 22,23,59),
    default_args=default_args
)

dwh=PythonOperator(
    task_id='dwh',
    dag=dag,
    python_callable=dwh
)

dummy1 = DummyOperator(
    task_id='start_load_to_dwh',
    dag=dag
)

dummy2 = DummyOperator(
    task_id='finish_load_to_dwh',
    dag=dag
)

dummy1 >> dwh >>dummy2