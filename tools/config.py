import os
import yaml
import json
from pydantic import BaseModel
from typing import Dict, List, Optional
from datetime import date

import pyspark.sql.types as T

def parse_schema(schema):
    ret = []
    for field in schema:
        ret.append(parse_field(field, schema[field]))
    return T.StructType(ret)

def parse_field(field, params):
    return T.StructField(field, getattr(T, params['type'])(), params['nullable'])

    
class Config(BaseModel):

    class DTable(BaseModel):
        table_schema: Dict
        partitionBy: Optional[str] = None
        transform: str

        def __init__(self, **kwargs):
            super(Config.DTable, self).__init__(**kwargs)

            self.table_schema = parse_schema(self.table_schema)

    class OutOfStock(BaseModel):
        owner: str
        email: List[str]
        email_on_failure: bool
        retries: int

        bronze_path: str
        silver_path: str
        api_connection: str
        data_endpoint: str
        auth_endpoint: str

        table_schema: Dict

        def __init__(self, **kwargs):
            super(Config.OutOfStock, self).__init__(**kwargs)

            self.table_schema = parse_schema(self.table_schema)

    class DShop(BaseModel):
        owner: str
        email: List[str]
        email_on_failure: bool
        retries: int

        bronze_path: str
        silver_path: str

        silver_to_dwh_process: List[str]

        tables: Dict

        def __init__(self, **kwargs):
            super(Config.DShop, self).__init__(**kwargs)

            for table in self.tables:
                self.tables[table] = Config.DTable(**self.tables[table])

    class AppendDate(BaseModel):
        owner: str
        email: List[str]
        email_on_failure: bool
        retries: int

    class Common(BaseModel):
        hdfs_connection: str
        dwh_connection: str
        postgres_connection: str
        jdbc_path: str

    out_of_stock : OutOfStock
    dshop : DShop
    common: Common
    append_date: AppendDate

    @staticmethod
    def load_yaml(file_path: str):
        with open(file_path) as cfg_file:
            cfg = yaml.safe_load(cfg_file)
            return Config(**cfg)

    @staticmethod
    def load_json(file_path: str):
        with open(file_path) as cfg_file:
            cfg = json.load(cfg_file)
            return Config(**cfg)

    @staticmethod
    def load_jsons(json_str: str):
        cfg = json.loads(json_str)
        return Config(**cfg)

    @staticmethod
    def load_dict(data: dict):
        return Config(**data)

    @staticmethod
    def load_from_env():
        #return Config.load_yaml(os.environ['RD_ETL_CONFIG'])
        return Config.load_yaml('/home/user/rd_etl_config.yml')

