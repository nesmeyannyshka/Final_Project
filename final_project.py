from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime

from python_functions.funcs import api_to_bronze, api_to_silver, load_to_bronze_spark, load_to_silver_spark, dwh

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

dag = DAG(
    dag_id="final_project",
    description="Building Data Platform",
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

dummy5 = DummyOperator(
    task_id='start_load_tables',
    dag=dag
)

dummy6 = DummyOperator(
    task_id='finish_load_tables',
    dag=dag
)
dummy7 = DummyOperator(
    task_id='start_load_to_dwh',
    dag=dag
)

dwh=PythonOperator(
    task_id='dwh',
    dag=dag,
    python_callable=dwh
)

dummy8 = DummyOperator(
    task_id='finish_load_to_dwh',
    dag=dag
)

tables=['orders','products','departments','aisles','clients','stores','store_types','location_areas']

for table in tables:
    load_to_bronze_group=PythonOperator(
        task_id="load_"+table+"_to_bronze",
        python_callable=load_to_bronze_spark,
        op_kwargs={"table": table}
    )
    load_to_silver_group=PythonOperator(
        task_id="load_"+table+"_to_silver",
        python_callable=load_to_silver_spark,
        op_kwargs={"table": table}
    )
    dummy1 >> api_bronze >> dummy2 >> dummy3 >> api_silver >> dummy4 >> dummy5 >> load_to_bronze_group >> load_to_silver_group >> dummy6 >> dummy7 >> dwh >>dummy8






