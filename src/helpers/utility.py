from datetime import datetime
from itertools import islice
from typing import Any
from tabulate import tabulate


def timer(func):
    # Декоратор, возвращает время выполнения действия, а так же результат
    def _wrap(*args, **kwargs):
        start = datetime.utcnow()
        result = func(*args, **kwargs)
        interval = datetime.utcnow() - start
        return interval, result
    return _wrap


def chunks(iterable, n: int = 200):
    it = iter(iterable)

    if isinstance(iterable, dict):
        for _ in range(0, len(iterable), n):
            yield {k: iterable[k] for k in islice(it, n)}
    else:
        # Для всех остальных: list, tuple, Queryset и etc
        chunk = list(islice(it, n))
        while chunk:
            yield chunk
            chunk = list(islice(it, n))


@timer
def upload_data(client, table: str, data: list[dict]):
    # Вставка данных
    client.insert(table=table, data=data)


@timer
def get_data(
    client,
    table: str = None,
    params: dict = None,
    query: str = None,
    avg_params: list = None
):
    # Чтение данных
    from db.clickhouse import ClickHouseClient
    from db.mongo import MongoDBClient

    if isinstance(client, MongoDBClient):
        if avg_params:
            return client.get_data(table=table, avg_params=avg_params)
        return client.get_data(table=table, params=params)
    elif isinstance(client, ClickHouseClient):
        return client.get_data(query=query)
    return


def tprint(data: list[list[Any]], headers: list[str]):
    # Печатает результат в табличном виде.
    print(tabulate(tabular_data=data, headers=headers))
