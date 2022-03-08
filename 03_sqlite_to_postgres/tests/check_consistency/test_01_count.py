"""
Test that sqlite and postgres tables have equal records count after data migration is done
"""
import sqlite3
import psycopg2

from pathlib import Path

from psycopg2.extras import DictCursor


def test_records_count(db_name, user, password, host, port):
    """
    Test that records count are equal for `film_work`, `genre`, `person`,
    `genre_film_work` and `person_film_work` tables
    """
    dsl = {'dbname': db_name, 'user': user, 'password': password, 'host': host, 'port': port}
    object_path = Path(__file__).resolve().parent.parent.parent
    db_path = object_path / 'db.sqlite'

    with sqlite3.connect(db_path) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        sqlite_cursor = sqlite_conn.cursor()

        # extracting records count for sqlite database
        sqlite_cursor.execute('SELECT count(*) FROM film_work')
        sqlite_film_work_count = int(sqlite_cursor.fetchone()[0])
        sqlite_cursor.execute('SELECT count(*) FROM person')
        sqlite_person_count = int(sqlite_cursor.fetchone()[0])
        sqlite_cursor.execute('SELECT count(*) FROM genre')
        sqlite_genre_count = int(sqlite_cursor.fetchone()[0])
        sqlite_cursor.execute('SELECT count(*) FROM person_film_work')
        sqlite_person_film_work_count = int(sqlite_cursor.fetchone()[0])
        sqlite_cursor.execute('SELECT count(*) FROM genre_film_work')
        sqlite_genre_film_work_count = int(sqlite_cursor.fetchone()[0])
        sqlite_cursor.close()

        # extracting records count for postgresql
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute('SELECT count(*) FROM content.film_work')
        pg_film_work_count = int(pg_cursor.fetchone()[0])
        pg_cursor.execute('SELECT count(*) FROM content.person')
        pg_person_count = int(pg_cursor.fetchone()[0])
        pg_cursor.execute('SELECT count(*) FROM content.genre')
        pg_genre_count = int(pg_cursor.fetchone()[0])
        pg_cursor.execute('SELECT count(*) FROM content.person_film_work')
        pg_person_film_work_count = int(pg_cursor.fetchone()[0])
        pg_cursor.execute('SELECT count(*) FROM content.genre_film_work')
        pg_genre_film_work_count = int(pg_cursor.fetchone()[0])
        pg_cursor.close()

        # assertion
        assert sqlite_film_work_count == pg_film_work_count
        assert sqlite_person_count == pg_person_count
        assert sqlite_genre_count == pg_genre_count
        assert sqlite_person_film_work_count == pg_person_film_work_count
        assert sqlite_genre_film_work_count == pg_genre_film_work_count
