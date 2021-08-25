from datetime import date, datetime
from os import path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.connection import Connection
from airflow.hooks.base_hook import BaseHook

from data_loader.config import Config
from data_loader.loader import Loader
from data_loader.hdfs_serializer import HDFSSerializer
from operators.bronze_to_silver_append_operator import BronzeToSilverAppendOperator
from dwh import functions as DWH
from tools import cfg


default_args = {
    'owner': cfg.out_of_stock.owner,
    'email': cfg.out_of_stock.email,
    'email_on_failure': cfg.out_of_stock.email_on_failure,
    'retries': cfg.out_of_stock.retries
}

def download_data(execution_date, **kwargs):
    api_connection: Connection = BaseHook.get_connection(cfg.out_of_stock.api_connection)
    hdfs_connection: Connection = BaseHook.get_connection(cfg.common.hdfs_connection)

    CONFIG = {
                'data':{
                    'endpoint': cfg.out_of_stock.data_endpoint,
                    'dates': [execution_date._datetime.strftime('%Y-%m-%d')],
                    'url': api_connection.host
                },
                'auth':{
                    'endpoint': cfg.out_of_stock.auth_endpoint,
                    'username': api_connection.login,
                    'password': api_connection.password
                }
            }
    
    loader = Loader(config      = Config.load_dict(CONFIG),
                    serializer  = HDFSSerializer(cfg.out_of_stock.bronze_path, hdfs_url=f'http://{hdfs_connection.host}:{hdfs_connection.port}', hdfs_user=hdfs_connection.login))
    loader.download()


with DAG(
    'out_of_stock_dag',
    description='Download out of stock information from API',
    schedule_interval='@daily',
    start_date=datetime(2021,8,21,22,0),
    default_args=default_args
) as dag:

    download_api_data = PythonOperator(
        task_id='download_api_data',
        dag=dag,
        python_callable=download_data,
        provide_context=True,
    )

    bronze_to_silver = BronzeToSilverAppendOperator(
        task_id='bronze_to_silver',
        dag=dag,
        load_path=cfg.out_of_stock.bronze_path,
        save_path=cfg.out_of_stock.silver_path,
        python_callable=DWH.transform_common,
        partitition_by='date',
        schema=cfg.out_of_stock.table_schema,
    )

    silver_to_dwh = PythonOperator(
        task_id=f'silver_to_dwh',
        dag=dag,
        python_callable=DWH.process_fact_out_of_stock,
    )

    download_api_data >> bronze_to_silver >> silver_to_dwh
    