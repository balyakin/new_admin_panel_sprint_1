"""
Tests that sqlite and postgres `genre` tables both have equal data
"""
import sqlite3
import psycopg2
import uuid

from pathlib import Path
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Genre:
    name: str
    description: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


from psycopg2.extras import DictCursor


def test_film_work_data(db_name, user, password, host, port):
    """
    Test: data comparison for genre table
    """
    dsl = {'dbname': db_name, 'user': user, 'password': password, 'host': host, 'port': port}
    object_path = Path(__file__).resolve().parent.parent.parent
    db_path = object_path / 'db.sqlite'

    with sqlite3.connect(db_path) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        sqlite_cursor = sqlite_conn.cursor()

        sqlite_genre_data = {}
        pg_genre_data = {}

        # extracting data from genre sqlite table
        sqlite_cursor.execute('SELECT id, name, description FROM genre')
        sqlite_data = sqlite_cursor.fetchall()
        sqlite_cursor.close()
        for data in sqlite_data:
            genre = Genre(id=data[0],
                          name=data[1],
                          description=data[2])
            sqlite_genre_data[genre.id] = genre

        # extracting data from content.genre pg table
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute('SELECT id, name, description FROM content.genre')
        pg_data = pg_cursor.fetchall()
        pg_cursor.close()
        for data in pg_data:
            genre = Genre(id=data[0],
                          name=data[1],
                          description=data[2])
            pg_genre_data[genre.id] = genre

        # assertion
        assert sqlite_genre_data == pg_genre_data
