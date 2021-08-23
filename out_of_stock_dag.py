from datetime import date, datetime
from os import path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.connection import Connection
from airflow.hooks.base_hook import BaseHook

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


from data_loader.config import Config
from data_loader.loader import Loader
from data_loader.hdfs_serializer import HDFSSerializer
from operators.bronze_to_silver_append_operator import BronzeToSilverAppendOperator

BRONZE_PATH = '/bronze/out_of_stock/data'
SILVER_PATH = '/silver/out_of_stock/'
API_CONNECTION = 'out_of_stock_connection'
HDFS_CONNECTION = 'hadoop_connection'
CONFIG = {
            'data':{
                'endpoint': "/out_of_stock",
                'dates': []
            },
            'auth':{
                'endpoint': "/auth",
            }
        }
TODAY_DATE = date.today().strftime('%Y-%m-%d')

default_args = {
    'owner': 'lesyk_maksym',
    'email': ['kiev.blues@gmail.com'],
    'email_on_failure': False,
    'retries': 2
}

def download_data():
    api_connection: Connection = BaseHook.get_connection(API_CONNECTION)
    hdfs_connection: Connection = BaseHook.get_connection(HDFS_CONNECTION)

    CONFIG['data']['dates'].append(TODAY_DATE)
    CONFIG['data']['url'] = api_connection.host
    CONFIG['auth']['username'] = api_connection.login
    CONFIG['auth']['password'] = api_connection.password
    
    loader = Loader(config      = Config.load_dict(CONFIG),
                    serializer  = HDFSSerializer(BRONZE_PATH, hdfs_url=f'http://{hdfs_connection.host}:{hdfs_connection.port}', hdfs_user=hdfs_connection.login))
    loader.download()

def prepare_table(session: SparkSession, df: DataFrame):
    return df.dropDuplicates()

with DAG(
    'out_of_stock_dag',
    description='Download out of stock information from API',
    schedule_interval='@daily',
    start_date=datetime(2021,8,10,22,0),
    default_args=default_args
) as dag:

    download_api_data = PythonOperator(
        task_id='download_api_data',
        dag=dag,
        python_callable=download_data,
    )

    bronze_to_silver = BronzeToSilverAppendOperator(
        task_id='bronze_to_silver',
        dag=dag,
        load_path=path.join(BRONZE_PATH, TODAY_DATE, 'data.json'),
        save_path=SILVER_PATH,
        python_callable=prepare_table,
        partitition_by='date',
        schema=None,
    )

    download_api_data >> bronze_to_silver 