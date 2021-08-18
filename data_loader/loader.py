import logging
import requests

from .config import Config
from .serializer_interface import SerializerInterface

class Loader():

    def __init__(self, config: Config, serializer: SerializerInterface):
        self.__config = config
        self.__serializer = serializer

    def download(self):
        self.__authenticate()

        for date in self.__config.data.dates:
            try:
                data = self.__get_data(str(date))
                self.__store_data(str(date), data)
            except Exception as e:
                logging.error(f'Error wile processing date {date}: {str(e)}')
                raise e

    def __get_data(self, date: str):
        logging.info(f'Requesting data for {date}')
        
        while True:
            response = requests.get(str(self.__config.data.url)+self.__config.data.endpoint, 
                                    json = {"date": date}, 
                                    headers={'Authorization': 'JWT '+self.__access_token})
            if response.status_code == 401:
                logging.warning('Authenticatin error!')
                self.__authenticate()
                continue
            response.raise_for_status()
            data = response.json()
            break

        return data

    def __authenticate(self):
        logging.info('Authenticating..')
        response = requests.post(str(self.__config.data.url)+self.__config.auth.endpoint, 
                          json = {"username": self.__config.auth.username, 
                                  "password": self.__config.auth.password})
        response.raise_for_status()
        self.__access_token = response.json()['access_token']
        logging.info('Access token received')

    def __store_data(self, subdir: str, data: dict):
        self.__serializer.store_data(subdir, data)
