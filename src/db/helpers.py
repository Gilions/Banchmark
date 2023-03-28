# type:ignore[attr-defined]
from .clickhouse import ClickHouseClient
from .migrations import TABLES


def init_db():
    """ Создаем миграции для ClickHouse."""
    ClickHouseClient(migrations=TABLES).migrate()
