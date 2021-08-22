from datetime import datetime, date
from os import path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField
import pyspark.sql.functions as F

from operators.postgres_dump_to_hdfs_operator import PostgresDumpToHDFSOperator
from operators.bronze_to_silver_operator import BronzeToSilverOperator

BRONZE_PATH = '/bronze/dshop_bu'
SILVER_PATH = '/silver/dshop_bu'
HDFS_CONNECTION = 'hadoop_connection'
POSTGRES_CONNECTION = 'postgres_dshop_bu_connection'
TABLES = [
    {'name': 'aisles',
    'schema': StructType([
        StructField('aisle_id', IntegerType(), False),
        StructField('aisle', StringType(), True),
    ]),
    'transform': lambda s, df: df.where(F.isnull(df.aisle) != True)\
                                 .dropDuplicates(),
    'partitionBy': None,
    },

    {'name': 'clients',
    'schema': StructType([
        StructField('id', IntegerType(), False),
        StructField('fullname', StringType(), True),
        StructField('location_area_id', IntegerType(), True),
    ]),
    'transform': lambda s, df: df.dropDuplicates(),
    'partitionBy': None,
    },

    {'name': 'departments',
    'schema': StructType([
        StructField('department_id', IntegerType(), False),
        StructField('department', StringType(), True),
    ]),
    'transform': lambda s, df: df.where(F.isnull(df.department) != True)\
                                 .dropDuplicates(),
    'partitionBy': None,
    },

    {'name': 'products',
    'schema': StructType([
        StructField('product_id', IntegerType(), False),
        StructField('product_name', StringType(), True),
        StructField('aisle_id', IntegerType(), True),
        StructField('department_id', IntegerType(), True),
    ]),
    'transform': lambda s, df: df.dropDuplicates(),
    'partitionBy': None,
    },

    {'name': 'location_areas',
    'schema': StructType([
        StructField('area_id', IntegerType(), False),
        StructField('area', StringType(), True),
    ]),
    'transform': lambda s, df: df.where(F.isnull(df.area) != True)\
                                 .dropDuplicates(),
    'partitionBy': None,
    },

    {'name': 'store_types',
    'schema': StructType([
        StructField('store_type_id', IntegerType(), False),
        StructField('type', StringType(), True),
    ]),
    'transform': lambda s, df: df.where(F.isnull(df.type) != True)\
                                 .dropDuplicates(),
    'partitionBy': None,
    },

    {'name': 'stores',
    'schema': StructType([
        StructField('store_id', IntegerType(), False),
        StructField('location_area_id', IntegerType(), True),
        StructField('store_type_id', IntegerType(), True),
    ]),
    'transform': lambda s, df: df.dropDuplicates(),
    'partitionBy': None,
    },

    {'name': 'orders',
    'schema': StructType([
        StructField('order_id', IntegerType(), True),
        StructField('product_id', IntegerType(), True),
        StructField('client_id', IntegerType(), True),
        StructField('store_id', IntegerType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('order_date', DateType(), True),
    ]),
    'transform': lambda s, df: df.dropDuplicates(),
    'partitionBy': 'store_id',
    },
]

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
        table_name = table['name']
        t = PostgresDumpToHDFSOperator(
            task_id=f'download_db_data_{table_name}',
            dag=dag,
            postgres_conn_id=POSTGRES_CONNECTION, 
            dump_table=table_name,
            dump_path=DOWNLOAD_PATH,
            dump_temp_folder=TEMP_PATH,
            hdfs_conn_id=HDFS_CONNECTION,
        )
        start >> t >> stage2

    for table in TABLES:
        table_name = table['name']
        table_schema = table['schema']
        table_transform = table['transform']
        table_partition = table['partitionBy']
        t = BronzeToSilverOperator(
            task_id=f'bronze_to_silver_{table_name}',
            dag=dag,
            load_path=path.join(BRONZE_PATH, 'data', TODAY_DATE, table_name+'.csv'),
            save_path=path.join(SILVER_PATH, table_name),
            schema=table_schema,
            python_callable=table_transform,
            partitition_by=table_partition,
        )
        stage2 >> t >> end

