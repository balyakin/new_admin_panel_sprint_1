import datetime
import os
import sqlite3
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_batch

load_dotenv()


class SQLiteLoader:
    def __init__(self, cursor):
        self._cursor = cursor

    def load_movies(self, array_size=100) -> list:

        self._cursor.execute('SELECT id, title, description, creation_date, rating, type FROM film_work')

        dt_now = datetime.datetime.now()

        while True:
            result = []
            data = self._cursor.fetchmany(array_size)
            if not data:
                break

            for row in data:
                result.append(row + (dt_now, dt_now,))

            yield result

    def load_persons(self, array_size=100) -> list:
        self._cursor.execute('SELECT id, full_name FROM person')

        dt_now = datetime.datetime.now()

        while True:
            result = []
            data = self._cursor.fetchmany(array_size)
            if not data:
                break

            for row in data:
                result.append(row + (dt_now, dt_now,))

            yield result

    def load_genres(self, array_size=100) -> list:
        self._cursor.execute('SELECT id, name, description FROM genre')

        dt_now = datetime.datetime.now()

        while True:
            result = []
            data = self._cursor.fetchmany(array_size)
            if not data:
                break

            for row in data:
                result.append(row + (dt_now, dt_now,))

            yield result

    def load_person_filmwork(self, array_size=100) -> list:
        self._cursor.execute('SELECT id, person_id, film_work_id, role FROM person_film_work')

        dt_now = datetime.datetime.now()

        while True:
            result = []
            data = self._cursor.fetchmany(array_size)
            if not data:
                break

            for row in data:
                result.append(row + (dt_now,))

            yield result

    def load_genre_filmwork(self, array_size=100) -> list:
        self._cursor.execute('SELECT id, genre_id, film_work_id FROM genre_film_work')

        dt_now = datetime.datetime.now()

        while True:
            result = []
            data = self._cursor.fetchmany(array_size)
            if not data:
                break

            for row in data:
                result.append(row + (dt_now,))

            yield result


class PostgresSaver:
    def __init__(self, connection):
        self._connection = connection

    def save_movies(self, movies: list) -> None:

        if not movies:
            raise ValueError("movies list is empty")

        with self._connection.cursor() as cursor:
            execute_batch(cursor, """INSERT INTO content.film_work (id, 
                title, 
                description, 
                creation_date, 
                rating, type, 
                created, modified)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing""", movies)

            self._connection.commit()

    def save_persons(self, persons: list) -> None:

        if not persons:
            raise ValueError("persons list is empty")

        with self._connection.cursor() as cursor:
            execute_batch(cursor, """INSERT INTO content.person (id, 
                full_name, 
                created, 
                modified)
                VALUES (%s, %s, %s, %s) on conflict do nothing""", persons)

            self._connection.commit()

    def save_genres(self, genres: list) -> None:

        if not genres:
            raise ValueError("genres list is empty")

        with self._connection.cursor() as cursor:
            execute_batch(cursor, """INSERT INTO content.genre (id, 
                name, 
                description, 
                created, 
                modified)
                VALUES (%s, %s, %s, %s, %s) on conflict do nothing""", genres)
            self._connection.commit()

    def save_person_filmwork(self, data: list) -> None:

        if not data:
            raise ValueError("person_film_work data is empty")

        with self._connection.cursor() as cursor:
            execute_batch(cursor, """INSERT INTO content.person_film_work (id, 
                person_id, 
                film_work_id, 
                role, 
                created)
                VALUES (%s, %s, %s, %s, %s) on conflict do nothing""", data)

            self._connection.commit()

    def save_genre_filmwork(self, data: list) -> None:

        if not data:
            raise ValueError("genre_film_work data is empty")

        with self._connection.cursor() as cursor:
            execute_batch(cursor, """INSERT INTO content.genre_film_work (id, 
                genre_id, 
                film_work_id, 
                created)
                VALUES (%s, %s, %s, %s) on conflict do nothing""", data)

            self._connection.commit()


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    for movies in sqlite_loader.load_movies():
        postgres_saver.save_movies(movies)

    for persons in sqlite_loader.load_persons():
        postgres_saver.save_persons(persons)

    for genres in sqlite_loader.load_genres():
        postgres_saver.save_genres(genres)

    for data in sqlite_loader.load_person_filmwork():
        postgres_saver.save_person_filmwork(data)

    for data in sqlite_loader.load_genre_filmwork():
        postgres_saver.save_genre_filmwork(data)


@contextmanager
def open_db(file_name: str):
    conn = sqlite3.connect(file_name)
    try:
        yield conn.cursor()
    finally:
        conn.close()


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'),
           'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'),
           'host': '127.0.0.1',
           'port': 5432}
    with open_db('db.sqlite') as sqlite_curr, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_curr, pg_conn)
