from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dwh import functions as DWH
from tools import cfg

default_args = {
    'owner': cfg.append_date.owner,
    'email': cfg.append_date.email,
    'email_on_failure': cfg.append_date.email_on_failure,
    'retries': cfg.append_date.retries
}

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

