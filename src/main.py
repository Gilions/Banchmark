from time import sleep

from db.clickhouse import ClickHouseClient
from db.helpers import init_db
from db.mongo import MongoDBClient
from helpers.generate_data import data_generator
from helpers.utility import get_data, tprint, upload_data

if __name__ == '__main__':
    # Создаем миграции в базу данных.
    init_db()
    # Генерируем данные
    print('Генерируем тестовые данные...')
    movies, bookmarks = data_generator()
    # Инициализируем клиенты
    ch_client = ClickHouseClient()
    mg_client = MongoDBClient()
    print('Данные сгенерированы.')
    sleep(1)
    print('Производим запись тестовых данных.')
    # Тестируем скорость запись данных
    write_movies_ch, _ = upload_data(
        client=ch_client,
        table='movies',
        data=movies
    )
    write_movies_mg, _ = upload_data(
        client=mg_client,
        table='movies',
        data=movies
    )
    write_bookmarks_ch, _ = upload_data(
        client=ch_client,
        table='bookmarks',
        data=bookmarks
    )
    write_bookmarks_mg, _ = upload_data(
        client=mg_client,
        table='bookmarks',
        data=bookmarks
    )
    print('Запись произведена, записываем результаты...')
    sleep(1)
    print('Начинаем тестирование чтения данных.')
    # Тестируем скорость чтения всех данных, каждой таблицы
    all_read_movies_ch, _ = get_data(
        client=ch_client,
        query='SELECT * FROM movies;'
    )
    all_read_movies_mg, _ = get_data(
        client=mg_client,
        table='movies'
    )
    all_read_bookmarks_ch, _ = get_data(
        client=ch_client,
        query='SELECT * FROM bookmarks;'
    )
    all_read_bookmarks_mg, _ = get_data(
        client=mg_client,
        table='bookmarks'
    )
    # Тестируем скорость чтения выборки данных. Примерно будет выбранно по movies 110к, bookmarks 60k данных.
    slice_read_movies_ch, _ = get_data(
        client=ch_client,
        query='SELECT * FROM movies WHERE rating > 3 and rating <= 9;'
    )
    slice_read_movies_mg, _ = get_data(
        client=mg_client,
        table='movies',
        params={'rating': {'$gt': 3, '$lte': 9}}
    )
    slice_read_bookmarks_ch, _ = get_data(
        client=ch_client,
        query='SELECT * FROM bookmarks WHERE user_id in (4, 7, 10);'
    )
    slice_read_bookmarks_mg, _ = get_data(
        client=mg_client,
        table='bookmarks',
        params={'user_id': {'$in': (4, 7, 10)}}
    )
    # Тестируем возможности агрегированния данных
    avg_read_movies_ch, _ = get_data(
        client=ch_client,
        query='SELECT create, count(*) FROM movies GROUP BY create ORDER BY create;'
    )
    avg_read_movies_mg, _ = get_data(
        client=mg_client, table='movies',
        avg_params=[{'$group': {'_id': '$create', 'count': {'$count': {}}}}, {'$sort': {'_id': 1}}]
    )
    avg_read_bookmarks_ch, _ = get_data(
        client=ch_client,
        query='SELECT user_id, count(*) FROM bookmarks GROUP BY user_id ORDER BY user_id;'
    )
    avg_read_bookmarks_mg, _ = get_data(
        client=mg_client,
        table='bookmarks',
        avg_params=[{'$group': {'_id': '$user_id', 'count': {'$count': {}}}}, {'$sort': {'_id': 1}}]
    )
    print('Тестирование завершилось, рисуем таблицу...')
    sleep(3)
    print('-' * 50)
    print(' ' * 10, 'РЕЗУЛЬТАТ ТЕСТИРОВАНИЯ')
    print('-' * 50)

    headers = ['Actions', 'ClickHouse', 'MongoDB']
    result = [
        ['write 200k', write_movies_ch, write_movies_mg],
        ['write ~ 500k', write_bookmarks_ch, write_bookmarks_mg],
        ['read all 200k', all_read_movies_ch, all_read_movies_mg],
        ['read all ~ 500k', all_read_bookmarks_ch, all_read_bookmarks_mg],
        ['read slice ~ 60k', slice_read_bookmarks_ch, slice_read_bookmarks_mg],
        ['read slice ~ 110k', slice_read_movies_ch, slice_read_movies_mg],
        ['read avg movies', slice_read_movies_ch, avg_read_movies_mg],
        ['read avg bookmarks', avg_read_bookmarks_ch, avg_read_bookmarks_mg]
    ]
    tprint(data=result, headers=headers)
