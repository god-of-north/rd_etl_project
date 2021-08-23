from os import path
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

from .bronze_to_silver_operator import BronzeToSilverOperator

class BronzeToSilverWithRejectLog(BronzeToSilverOperator):

    def _load_df(self, spark: SparkSession, file_path: str) -> DataFrame:
        df = super(BronzeToSilverWithRejectLog, self)._load_df(spark, file_path)

        self.log.info('Loading schemaless datafile...')
        self.schemaless_df = spark.read.load(file_path,
                               header="true",
                               inferSchema="true",
                               format="csv")
        return df

    def _wite_df(self, df: DataFrame, file_path: str, partitionBy = None):
        super(BronzeToSilverWithRejectLog, self)._wite_df(df, file_path, partitionBy)

        self.log.info('Searching for rejected records...')
        self.schemaless_df.exceptAll(df)\
            .write.csv(
                path.join(file_path+'_errors', date.today().strftime('%Y-%m-%d'), 'rejected.csv'), 
                mode='overwrite', 
                header=True)
