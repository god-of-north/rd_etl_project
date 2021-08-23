import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
import pyspark.sql.functions as F


hdfs_url = 'http://127.0.0.1:50070/'

gp_url = "jdbc:postgresql://127.0.0.1:5433/rd_dwh"
gp_properties = {"user": "gpuser", "password": "secret"}

jdbc_path = '~/jdbc/postgresql-42.2.23.jar'

SILVER_PATH = '/silver/dshop_bu'


def get_file_from_table(table:str) -> str:
    return os.path.join(SILVER_PATH, table)

def open_spark_session() -> SparkSession:
    return SparkSession.builder\
        .config('spark.driver.extraClassPath', jdbc_path)\
        .master('local')\
        .appName("SilverToDWH")\
        .getOrCreate()
   
def load_from_silver(spark: SparkSession, table: str) -> DataFrame:
    file_name = get_file_from_table(table)
    logging.info(f'Loading data from {file_name}...')
    return spark.read.parquet(file_name)

def save_to_dwh(df: DataFrame, table: str):
    logging.info(f'Writing data to {table}...')
    df.write.options(batchSize=100000, queyTime=690)\
            .jdbc(gp_url,
                  table=table,
                  properties=gp_properties,
                  mode='overwrite')

def append_to_dwh(df: DataFrame, table: str):
    logging.info(f'Appending data to {table}...')
    df.write.jdbc(gp_url,
                  table=table,
                  properties=gp_properties,
                  mode='append')

def process_dim_products():
    spark = open_spark_session()

    aisles_df = load_from_silver(spark, 'aisles')
    departments_df = load_from_silver(spark, 'departments')
    products_df = load_from_silver(spark, 'products')

    dim_products_df = products_df.join(aisles_df,
                                       aisles_df.aisle_id == products_df.aisle_id,
                                       'left')\
                                 .join(departments_df,
                                       departments_df.department_id == products_df.department_id,
                                       'left')\
                                 .select(
                                       products_df.product_id,
                                       products_df.product_name,
                                       departments_df.department.alias('department_name'),
                                       aisles_df.aisle
                                 )

    save_to_dwh(dim_products_df, 'dim_products')


def process_dim_clients():
    spark = open_spark_session()

    clients_df = load_from_silver(spark, 'clients')

    dim_clients_df = clients_df.withColumnRenamed('id', 'client_id')\
                               .withColumnRenamed('location_area_id', 'area_id')

    save_to_dwh(dim_clients_df, 'dim_clients')

def process_dim_area():
    spark = open_spark_session()

    location_areas_df = load_from_silver(spark, 'location_areas')

    dim_area_df = location_areas_df
    
    save_to_dwh(dim_area_df, 'dim_area')

def process_dim_stores():
    spark = open_spark_session()

    store_types_df = load_from_silver(spark, 'store_types')
    stores_df = load_from_silver(spark, 'stores')

    dim_stores_df = stores_df.join(store_types_df,
                                   store_types_df.store_type_id == stores_df.store_type_id,
                                   'left')\
                             .select(
                                   stores_df.store_id,
                                   stores_df.location_area_id.alias('area_id'),
                                   store_types_df.type
                             )

    save_to_dwh(dim_stores_df, 'dim_stores')

def process_fact_out_of_stock():
    spark = open_spark_session()

    out_of_stock_df = spark.read.parquet('/silver/out_of_stock')

    fact_out_of_stock_df = out_of_stock_df.withColumnRenamed('date','oos_date')

    save_to_dwh(fact_out_of_stock_df, 'fact_out_of_stock')

def process_fact_orders():
    spark = open_spark_session()

    orders_df = load_from_silver(spark, 'orders')

    fact_orders_df = orders_df.select(
                                    orders_df.product_id,
                                    orders_df.client_id,
                                    orders_df.store_id,
                                    orders_df.order_date,
                                    orders_df.quantity
                                  )\
                               .repartition('store_id')

    save_to_dwh(fact_orders_df, 'fact_orders')

def process_dim_date():
    spark = open_spark_session()

    orders_df = load_from_silver(spark, 'orders')
    out_of_stock_df = spark.read.parquet('/silver/out_of_stock')

    dim_date_df = orders_df.select(
                               orders_df.order_date.alias('action_date'),
                           )\
                           .union(out_of_stock_df.select(out_of_stock_df.date.alias('action_date')))\
                           .distinct()\
                           .select(
                               F.col('action_date'),
                               F.dayofmonth(F.col('action_date')).alias('action_day'),
                               F.month(F.col('action_date')).alias('action_month'),
                               F.year(F.col('action_date')).alias('action_year'),
                               F.date_format(F.col('action_date'), "E").alias('action_weekday')
                           )

    save_to_dwh(dim_date_df, 'dim_date')


def append_dim_date(execution_date, **context):
    logging.info(f'Appending date {execution_date.to_date_string()}...')

    spark = open_spark_session()

    schema = StructType([
        StructField("action_date", DateType(), False),
        StructField("action_day", IntegerType(), False),
        StructField("action_month", IntegerType(), False),
        StructField("action_year", IntegerType(), False),
        StructField("action_weekday", StringType(), False),
    ])

    data = [(
        execution_date._datetime.date(), 
        execution_date.day, 
        execution_date.month, 
        execution_date.year, 
        execution_date.format('ddd'),
        )]
    
    df = spark.createDataFrame(data=data, schema=schema)    

    append_to_dwh(df, 'dim_date')
