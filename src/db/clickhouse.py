from clickhouse_driver import Client

from helpers.utility import chunks
from settings import conf


class ClickHouseClient:
    """
     Клиент ClickHouse.
      :Methods:
        - insert - вставка данных.
        - get_data - получение данных.
        - migrate - миграция, используется для миграции новых таблиц.
     :Parameters:
     - migrations - Формат tuple, по умолчанию None, кортеж запросов, для создания новых миграций.
    """
    def __init__(self, migrations: tuple = None):
        self.migrations = migrations
        self.client = self.get_client()

    def get_client(self):
        return Client(host=conf.CH_HOST, port=conf.CH_PORT)

    def make_migrations(self):
        for row in self.migrations:
            for query in row.split(';'):
                if query.strip():
                    self.make_request(query=query)

    def make_request(self, query: str, params=None):
        return self.client.execute(query, params)

    def insert(self, **kwargs):
        table = kwargs.get('table', None)
        data = kwargs.get('data', None)
        if not data or not table:
            return

        for chunk in chunks(data):
            fields = ','.join((chunk[0].keys()))

            query = 'INSERT INTO {table} ({fields}) VALUES'.format(
                table=table, fields=fields
            )
            self.make_request(query=query, params=chunk)

    def get_data(self, query: str = None):
        result = self.make_request(query=query)
        return list(result)

    def migrate(self):
        if not self.migrations:
            return

        self.make_migrations()
