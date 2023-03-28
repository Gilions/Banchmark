import uuid
from datetime import datetime
from random import randint
from typing import Any
from faker import Faker


def data_generator() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    # Генерируем данные необходимые для тестов.
    fake = Faker()
    movies: list = []
    bookmarks: list = []
    for _ in range(200000):
        movie = dict(
            movie_id=str(uuid.uuid4()),
            title=fake.text(randint(7, 35)),
            like_counter=randint(0, 1000),
            rating=randint(0, 10),
            create=int(fake.year()),
            created_at=datetime.utcnow()
        )
        for _ in range(randint(1, 10)):
            if len(bookmarks) > 500000:
                break
            bookmark = dict(
                user_id=randint(1, 25),
                movie_id=movie['movie_id'],
                created_at=datetime.utcnow()
            )
            bookmarks.append(bookmark)
        movies.append(movie)
    return movies, bookmarks
