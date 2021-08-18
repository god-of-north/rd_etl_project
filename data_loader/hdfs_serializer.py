import os
import json
import logging
from hdfs import InsecureClient

from .serializer_interface import SerializerInterface


class HDFSSerializer(SerializerInterface):

    def __init__(self, data_path: str, hdfs_url: str, hdfs_user: str):
        self.data_path = data_path
        self.hdfs_url = hdfs_url
        self.hdfs_user = hdfs_user

    def store_data(self, partition: str, data: dict):
        subdir = os.path.join(self.data_path, partition)
        datafile_path = os.path.join(subdir, 'data.json')

        logging.info(f'Saving data to HDFS: {datafile_path}')

        client = InsecureClient(self.hdfs_url, self.hdfs_user)
        client.makedirs(subdir)

        with client.write(datafile_path, encoding='utf-8') as datafile:
            json.dump(data, datafile)

        logging.info(f'Datafile saved')
