from datetime import datetime, date
from os import path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from postgres_dump_to_hdfs_operator import PostgresDumpToHDFSOperator
from bronze_to_silver_operator import BronzeToSilverOperator

BRONZE_PATH = '/bronze/dshop_bu'
SILVER_PATH = '/silver/dshop_bu'
HDFS_CONNECTION = 'hadoop_connection'
POSTGRES_CONNECTION = 'postgres_dshop_bu_connection'
TABLES = ['aisles', 'clients', 'departments', 'orders', 'products', 'location_areas', 'store_types', 'stores']

DOWNLOAD_PATH = path.join(BRONZE_PATH,'data')
TEMP_PATH = path.join(BRONZE_PATH, 'temp')
TODAY_DATE = date.today().strftime('%Y-%m-%d')

default_args = {
    'owner': 'lesyk_maksym',
    'email': ['kiev.blues@gmail.com'],
    'email_on_failure': False,
    'retries': 2
}

def prepare_table(session: SparkSession, df: DataFrame):
    return df.dropDuplicates()

with DAG(
    'dshop_bu_dag',
    description='Dump tables from dshop_bu DB',
    schedule_interval='@daily',
    start_date=datetime(2021,8,12,22,0),
    default_args=default_args
) as dag:

    start = DummyOperator(task_id=f'start', dag=dag)
    stage2 = DummyOperator(task_id=f'stage2', dag=dag)
    end = DummyOperator(task_id=f'end', dag=dag)

    for table in TABLES:
        t = PostgresDumpToHDFSOperator(
            task_id=f'download_db_data_{table}',
            dag=dag,
            postgres_conn_id=POSTGRES_CONNECTION, 
            dump_table=table,
            dump_path=DOWNLOAD_PATH,
            dump_temp_folder=TEMP_PATH,
            hdfs_conn_id=HDFS_CONNECTION,
        )
        start >> t >> stage2

    for table in TABLES:
        t = BronzeToSilverOperator(
            task_id=f'bronze_to_silver_{table}',
            dag=dag,
            load_path=path.join(BRONZE_PATH, 'data', TODAY_DATE, table+'.csv'),
            save_path=path.join(SILVER_PATH, table),
            python_callable=prepare_table,
        )
        stage2 >> t >> end

