from pymongo import MongoClient

from helper.utility import chunks
from settings import conf


class MongoDBClient:
    """
    Клиент для MongoDB.
    :Methods:
        - insert - вставки данных.
        - get_data - получение данных.
    :Parameters:
        - db_name - Наименование базы данных, по умолчанию mongodb.
    """
    def __init__(self, db_name: str = 'mongodb'):
        self.db_name = db_name
        self.client = self.get_client()

    def get_client(self):
        return MongoClient(host=conf.MONGO_HOST, port=conf.MONGO_PORT)

    def _get_collection(self, table: str):
        db = self.client[self.db_name]
        return db[table]

    def insert(self, **kwargs):
        table = kwargs.get('table', None)
        data = kwargs.get('data', None)
        if not table or not data:
            return

        collection = self._get_collection(table)
        for chunk in chunks(data):
            collection.insert_many(chunk)

    def get_data(self, table: str, params=None, avg_params: list = None) -> list:
        collection = self._get_collection(table)

        if avg_params:
            result = collection.aggregate(avg_params)
            return list(result)

        if not params:
            result = collection.find()
            return list(result)

        result = collection.find(params)
        return list(result)
