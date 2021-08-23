from airflow.utils.decorators import apply_defaults
from airflow.operators.python_operator import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

class BronzeToSilverOperator(PythonOperator):

    load_path: str = None
    save_path: str = None

    @apply_defaults
    def __init__(
            self, 
            schema: StructType,
            load_path: str,
            save_path: str,
            partitition_by = None,
            *args, **kwargs):

        self.load_path = load_path
        self.save_path = save_path
        self.partitition_by = partitition_by
        self.schema = schema

        super(BronzeToSilverOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info(f'Loading data file from {self.load_path}')
        session = self._open_spark_session()
        df = self._load_df(session, self.load_path)

        self.log.info('Performing data transformations...')
        self.op_kwargs['session'] = session
        self.op_kwargs['df'] = df
        df = super(BronzeToSilverOperator, self).execute(context)

        self.log.info(f'Saving data file to {self.save_path}')
        self._wite_df(df, self.save_path, self.partitition_by)

    def _open_spark_session(self) -> SparkSession:
        return SparkSession.builder\
            .master('local')\
            .appName("BronzeToSilverOperator")\
            .getOrCreate()

    def _load_df(self, spark: SparkSession, file_path: str) -> DataFrame:
        return spark.read.load(file_path,
                               header="true",
                               schema=self.schema,
                               format="csv")

    def _wite_df(self, df: DataFrame, file_path: str, partitionBy = None):
        df.write.parquet(file_path, mode='overwrite', partitionBy=partitionBy)
