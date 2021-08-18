import yaml
import json
from pydantic import BaseModel, HttpUrl
from typing import List
from datetime import date


class Config(BaseModel):

    class Data(BaseModel):
        url: HttpUrl
        endpoint: str
        dates: List[date]

    class Auth(BaseModel):
        endpoint: str
        username: str
        password: str

    data : Data
    auth : Auth

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
