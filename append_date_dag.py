from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dwh import functions as DWH

default_args = {
    'owner': 'lesyk_maksym',
    'email': ['kiev.blues@gmail.com'],
    'email_on_failure': False,
    'retries': 2
}

def fn(**c):
    pass

with DAG(
    'append_date_dag',
    description='Appends date to dim_date in DWH',
    schedule_interval='@daily',
    start_date=datetime(2021,8,22,0,0),
    default_args=default_args
) as dag:

    download_api_data = PythonOperator(
        task_id='append_date',
        dag=dag,
        python_callable=DWH.append_dim_date,
        provide_context=True,
    )

