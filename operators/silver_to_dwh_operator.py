from airflow.utils.decorators import apply_defaults
from airflow.operators.python_operator import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from airflow.models.connection import Connection
from airflow.hooks.base_hook import BaseHook

from .bronze_to_silver_operator import BronzeToSilverOperator

class SilverToDWHOperator(BronzeToSilverOperator):

    @apply_defaults
    def __init__(
            self, 
            dwh_connection: str,
            jdbc_path: str,
            *args, **kwargs):

        self.dwh_connection = dwh_connection
        self.jdbc_path = jdbc_path

        super(SilverToDWHOperator, self).__init__(*args, **kwargs)

    def _open_spark_session(self) -> SparkSession:
        return SparkSession.builder\
            .config('spark.driver.extraClassPath', jdbc_path)\
            .master('local')\
            .appName("SilverToDWHOperator")\
            .getOrCreate()

    def _load_df(self, spark: SparkSession, file_path: str) -> DataFrame:
        return spark.read.parquet(file_path)

    def _wite_df(self, df: DataFrame, file_path: str, partitionBy = None):
        table = file_path
        jdbc: Connection = BaseHook.get_connection(self.hdfs_conn_id)

        gp_url = f"jdbc:postgresql://{jdbc.host}:{jdbc.port}/{jdbc.schema}"
        gp_properties = {"user": jdbc.login, "password": jdbc.password}
        df.write.options(batchSize=100000, queyTime=690)\
                .jdbc(gp_url,
                      table=table,
                      properties=gp_properties,
                      mode='overwrite')
