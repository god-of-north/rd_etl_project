import os
import json

from .serializer_interface import SerializerInterface


class JSONSerializer(SerializerInterface):

    def __init__(self, data_path: str):
        self.data_path = data_path
        os.makedirs(data_path, exist_ok=True)

    def store_data(self, partition: str, data: dict):
        subdir = os.path.join(self.data_path, partition)
        os.makedirs(subdir, exist_ok=True)
        
        with open(os.path.join(subdir, 'data.json'), 'w') as datafile:
            datafile.write(json.dumps(data))
