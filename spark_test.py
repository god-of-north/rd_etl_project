from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from hdfs import InsecureClient

import os
from datetime import datetime

hdfs_url = 'http://127.0.0.1:50070/'

gp_url = "jdbc:postgresql://127.0.0.1:5433/postgres"
gp_properties = {"user": "gpuser", "password": "secret"}

def get_file_from_table(table:str) -> str:
    return os.path.join('/silver', 'dshop_bu')

def open_spark_session() -> SparkSession:
    return SparkSession.builder\
        .config('spark.driver.extraClassPath',
                '/mnt/shared_folder/postgresql-42.2.23.jar')\
        .master('local')\
        .appName("homework6")\
        .getOrCreate()
   
def load_csv_from_bronze(spark: SparkSession ,table: str) -> DataFrame:
    return spark.read.load(get_file_from_table(table),
                           header="true",
                           inferSchema="true",
                           format="csv",)

def load_from_silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.parquet(get_file_from_table(table))

if __name__ == '__main__':

    spark = open_spark_session()

    aisles_df = load_from_silver(spark, 'aisles')
    clients_df = load_from_silver(spark, 'clients')
    departments_df = load_from_silver(spark, 'departments')
    location_areas_df = load_from_silver(spark, 'location_areas')
    orders_df = load_from_silver(spark, 'orders')
    products_df = load_from_silver(spark, 'products')
    store_types_df = load_from_silver(spark, 'store_types')
    stores_df = load_from_silver(spark, 'stores')
    out_of_stock_df = spark.read.parquet('/silver/out_of_stock')



    dim_area_df = location_areas_df
    dim_clients_df = clients_df.withColumnRenamed('id', 'client_id')\
                               .withColumnRenamed('location_area_id', 'area_id')
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
    dim_stores_df = stores_df.join(store_types_df,
                                   store_types_df.store_type_id == stores_df.store_type_id,
                                   'left')\
                             .select(
                                   stores_df.store_id,
                                   stores_df.location_area_id.alias('area_id'),
                                   store_types_df.type
                             )
    fact_orders_df = orders_df.select(
                                    orders_df.product_id,
                                    orders_df.client_id,
                                    orders_df.store_id,
                                    orders_df.order_date,
                                    orders_df.quantity
                                  )
    fact_out_of_stock_df = out_of_stock_df.withColumnRenamed('date','oos_date')
    dim_date_df = fact_orders_df.select(
                                    orders_df.order_date.alias('action_date'),
                                )\
                                .union(out_of_stock_df.select(out_of_stock_df.date.alias('action_date')))\
                                .select(
                                    F.col('action_date'),
                                    F.dayofmonth(F.quarter(F.col('action_date'))).alias('action_day'),
                                    F.month(F.quarter(F.col('action_date'))).alias('action_month'),
                                    F.year(F.quarter(F.col('action_date'))).alias('action_year'),
                                    F.date_format(F.col('action_date'), "E").alias('action_weekday')
                                )

    dim_date_df.write.jdbc(gp_url,
                     table='dim_date',
                     properties=gp_properties,
                     mode='overwrite')
    dim_products_df.write.jdbc(gp_url,
                     table='dim_products',
                     properties=gp_properties,
                     mode='overwrite')
    dim_area_df.write.jdbc(gp_url,
                     table='dim_area',
                     properties=gp_properties,
                     mode='overwrite')
    dim_clients_df.write.jdbc(gp_url,
                     table='dim_clients',
                     properties=gp_properties,
                     mode='overwrite')
    dim_stores_df.write.jdbc(gp_url,
                     table='dim_stores',
                     properties=gp_properties,
                     mode='overwrite')
    fact_orders_df.write.jdbc(gp_url,
                     table='fact_orders',
                     properties=gp_properties,
                     mode='overwrite')
    fact_out_of_stock_df.write.jdbc(gp_url,
                     table='fact_out_of_stock',
                     properties=gp_properties,
                     mode='overwrite')

