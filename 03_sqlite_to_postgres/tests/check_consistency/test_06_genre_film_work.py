"""
Tests that sqlite and postgres `genre_film_work` tables both have equal data
"""
import sqlite3
import psycopg2
import uuid

from pathlib import Path
from dataclasses import dataclass, field


@dataclass(frozen=True)
class GenreFilmwork:
    genre_id: uuid.UUID
    film_work_id: uuid.UUID
    id: uuid.UUID = field(default_factory=uuid.uuid4)


from psycopg2.extras import DictCursor


def test_film_work_data(db_name, user, password, host, port):
    """
    Test: data comparison for person_film_work table
    """
    dsl = {'dbname': db_name, 'user': user, 'password': password, 'host': host, 'port': port}
    object_path = Path(__file__).resolve().parent.parent.parent
    db_path = object_path / 'db.sqlite'

    with sqlite3.connect(db_path) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        sqlite_cursor = sqlite_conn.cursor()

        sqlite_genre_film_work_data = {}
        pg_genre_film_work_data = {}

        # extracting data from genre_film_work sqlite table
        sqlite_cursor.execute('SELECT id, genre_id, film_work_id FROM genre_film_work')
        sqlite_data = sqlite_cursor.fetchall()
        sqlite_cursor.close()
        for data in sqlite_data:
            genre_film_work = GenreFilmwork(id=data[0],
                                            genre_id=data[1],
                                            film_work_id=data[2])
            sqlite_genre_film_work_data[genre_film_work.id] = genre_film_work

        # extracting data from content.genre_film_work pg table
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute('SELECT id, genre_id, film_work_id FROM content.genre_film_work')
        pg_data = pg_cursor.fetchall()
        pg_cursor.close()
        for data in pg_data:
            genre_film_work = GenreFilmwork(id=data[0],
                                            genre_id=data[1],
                                            film_work_id=data[2])
            pg_genre_film_work_data[genre_film_work.id] = genre_film_work

        # assertion
        assert sqlite_genre_film_work_data == pg_genre_film_work_data
