from datetime import date, datetime
from os import path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from data_loader.config import Config
from data_loader.loader import Loader
from data_loader.hdfs_serializer import HDFSSerializer

from postgres_dump_to_hdfs_operator import PostgresDumpToHDFSOperator

DOWNLOAD_PATH = '/mnt/shared_folder/data'
CONFIG = {
            'data':{
                'url': "https://robot-dreams-de-api.herokuapp.com",
                'endpoint': "/out_of_stock",
                'dates': ['2021-01-01','2021-01-02','2021-01-03','2021-01-04']
            },
            'auth':{
                'endpoint': "/auth",
                'username': "rd_dreams",
                'password': "djT6LasE",
            }
        }
TABLES = ['aisles', 'clients', 'departments', 'orders', 'products', 'location_areas', 'store_types', 'stores']

def download_data():

    #Add current date to the download request
    CONFIG['data']['dates'].append(date.today().strftime('%Y-%m-%d'))

    loader = Loader(config      = Config.load_dict(CONFIG),
                    serializer  = HDFSSerializer(DOWNLOAD_PATH, hdfs_url='http://127.0.0.1:50070', hdfs_user='user'))
    loader.download()


default_args = {
    'owner': 'lesyk_maksym',
    'email': ['kiev.blues@gmail.com'],
    'email_on_failure': False,
    'retries': 2
}

with DAG(
    'homework5_dag',
    description='Download out of stock information and dump dshop DB',
    schedule_interval='@daily',
    start_date=datetime(2021,7,28,22,0),
    default_args=default_args
) as dag:

    start = DummyOperator(task_id='start', dag=dag)
    end = DummyOperator(task_id='end', dag=dag)

    t1 = PythonOperator(
        task_id='download_api_data',
        dag=dag,
        python_callable=download_data,
    )

    dump_tasks = []
    for table in TABLES:
        dump_tasks.append(PostgresDumpToHDFSOperator(
            task_id=f'download_db_data_{table}',
            dag=dag,
            postgres_conn_id='postgres_dshop_bu_connection', 
            dump_table=table,
            dump_path=path.join(DOWNLOAD_PATH, f'{table}.csv'),
            hdfs_conn_id='hadoop_connection',
        ))

    for i in range(1,len(dump_tasks)):
        dump_tasks[i-1] >> dump_tasks[i]

    start >> t1 >> end
    start >> dump_tasks[0]
    dump_tasks[len(dump_tasks)-1] >> end
