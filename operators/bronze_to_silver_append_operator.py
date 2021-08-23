from airflow.utils.decorators import apply_defaults
from airflow.operators.python_operator import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from .bronze_to_silver_operator import BronzeToSilverOperator

class BronzeToSilverAppendOperator(BronzeToSilverOperator):

    def _load_df(self, spark: SparkSession, file_path: str) -> DataFrame:
        return spark.read.json(file_path)

    def _wite_df(self, df: DataFrame, file_path: str, partitionBy = None):
        df.write.parquet(file_path, mode='append', partitionBy=partitionBy)
