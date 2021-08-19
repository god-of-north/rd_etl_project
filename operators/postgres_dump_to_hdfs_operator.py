from datetime import datetime
from os import path

from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models.connection import Connection
from airflow.hooks.base_hook import BaseHook

from hdfs import InsecureClient


class PostgresDumpToHDFSOperator(PostgresOperator):

    dump_path: str = None
    dump_table: str = None
    dump_temp_folder: str = None
    hdfs_conn_id: str = None

    @apply_defaults
    def __init__(
            self, 
            dump_path: str,
            dump_table: str,
            hdfs_conn_id: str,
            dump_temp_folder: str,
            *args, **kwargs):

        self.dump_path = dump_path
        self.dump_table = dump_table
        self.dump_temp_folder = dump_temp_folder
        self.hdfs_conn_id = hdfs_conn_id

        kwargs['sql'] = f'COPY {self.dump_table} TO STDOUT WITH HEADER CSV'

        super(PostgresDumpToHDFSOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        now = datetime.now()
        self.log.info('Copying data from %s to: %s', self.dump_table, self.dump_path)

        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                    schema=self.database)
        cursor = self.hook.get_conn().cursor()

        hdfs_connection: Connection = BaseHook.get_connection(self.hdfs_conn_id)

        client = InsecureClient(f'http://{hdfs_connection.host}:{hdfs_connection.port}', user=hdfs_connection.login)

        temp_folder = path.join(self.dump_temp_folder, now.strftime('%Y-%m-%d'))
        dump_path = path.join(self.dump_path, now.strftime('%Y-%m-%d'))
	
        client.makedirs(temp_folder)
        client.makedirs(dump_path)

        temp_file_name = self._get_temp_file_name(client)
        temp_path = path.join(temp_folder, temp_file_name)
        with client.write(temp_path) as csv_file:
            cursor.copy_expert(self.sql, csv_file)

        client.rename(temp_path, path.join(dump_path, self.dump_table+'.csv'))
    
    def _get_temp_file_name(self, client):
        temp_file_name = datetime.now().strftime('%H%M%S')
        ll = client.list(self.dump_temp_folder)

        i = 0
        while True:
            tmp = temp_file_name + str(i)
            if tmp not in ll:
                temp_file_name = tmp
                break
        return temp_file_name
