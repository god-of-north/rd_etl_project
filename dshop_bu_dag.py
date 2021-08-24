from datetime import datetime, date
from os import path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from operators.postgres_dump_to_hdfs_operator import PostgresDumpToHDFSOperator
from operators.bronze_to_silver_operator import BronzeToSilverOperator
from dwh import functions as DWH
from tools import cfg


default_args = {
    'owner': cfg.dshop.owner,
    'email': cfg.dshop.email,
    'email_on_failure': cfg.dshop.email_on_failure,
    'retries': cfg.dshop.retries
}


with DAG(
    'dshop_bu_dag',
    description='Dump tables from dshop_bu DB',
    schedule_interval='@daily',
    start_date=datetime(2021,8,21,22,0),
    default_args=default_args
) as dag:

    start = DummyOperator(task_id=f'start', dag=dag)
    stage2 = DummyOperator(task_id=f'stage2', dag=dag)
    stage3 = DummyOperator(task_id=f'stage3', dag=dag)
    end = DummyOperator(task_id=f'end', dag=dag)

    for table in cfg.dshop.tables:
        t = PostgresDumpToHDFSOperator(
            task_id=f'download_db_data_{table}',
            dag=dag,
            postgres_conn_id=cfg.common.postgres_connection, 
            dump_table=table,
            dump_path=path.join(cfg.dshop.bronze_path,'data'),
            dump_temp_folder=path.join(cfg.dshop.bronze_path, 'temp'),
            hdfs_conn_id=cfg.common.hdfs_connection,
        )
        start >> t >> stage2

    for table in cfg.dshop.tables:
        table_name = table
        table_schema = cfg.dshop.tables[table].table_schema
        table_transform = getattr(DWH, cfg.dshop.tables[table].transform)
        table_partition = cfg.dshop.tables[table].partitionBy
        t = BronzeToSilverOperator(
            task_id=f'bronze_to_silver_{table_name}',
            dag=dag,
            load_path=path.join(cfg.dshop.bronze_path, 'data', date.today().strftime('%Y-%m-%d'), table_name+'.csv'),
            save_path=path.join(cfg.dshop.silver_path, table_name),
            schema=table_schema,
            python_callable=table_transform,
            partitition_by=table_partition,
        )
        stage2 >> t >> stage3

    for process in cfg.dshop.silver_to_dwh_process:
        t = PythonOperator(
            task_id=f'silver_to_dwh_{process}',
            dag=dag,
            python_callable=getattr(DWH, process),
        )
        stage3 >> t >> end

