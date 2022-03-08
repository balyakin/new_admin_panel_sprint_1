"""
Test that sqlite and postgres tables have equal records count after data migration is done
"""
import sqlite3
import psycopg2
import uuid
import datetime

from pathlib import Path
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Movie:
    title: str
    description: str
    creation_date: datetime.datetime
    type: str
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


from psycopg2.extras import DictCursor


def test_film_work_data(db_name, user, password, host, port):
    """
    Test: data comparison for film_work table
    """
    dsl = {'dbname': db_name, 'user': user, 'password': password, 'host': host, 'port': port}
    object_path = Path(__file__).resolve().parent.parent.parent
    db_path = object_path / 'db.sqlite'

    with sqlite3.connect(db_path) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        sqlite_cursor = sqlite_conn.cursor()

        sqlite_film_work_data = {}
        pg_film_work_data = {}

        # extracting data from film_work sqlite table
        sqlite_cursor.execute('SELECT id, title, description, creation_date, type, rating FROM film_work')
        sqlite_data = sqlite_cursor.fetchall()
        sqlite_cursor.close()
        for data in sqlite_data:
            movie = Movie(id=data[0],
                          title=data[1],
                          description=data[2],
                          creation_date=data[3],
                          type=data[4],
                          rating=data[5])
            sqlite_film_work_data[movie.id] = movie

        # extracting data from content.film_work pg table
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute('SELECT id, title, description, creation_date, type, rating FROM content.film_work')
        pg_data = pg_cursor.fetchall()
        pg_cursor.close()
        for data in pg_data:
            movie = Movie(id=data[0],
                          title=data[1],
                          description=data[2],
                          creation_date=data[3],
                          type=data[4],
                          rating=data[5])
            pg_film_work_data[movie.id] = movie

        # assertion
        assert sqlite_film_work_data == pg_film_work_data
