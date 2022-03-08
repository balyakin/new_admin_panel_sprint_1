"""
Tests that sqlite and postgres `person` tables both have equal data
"""
import sqlite3
import psycopg2
import uuid

from pathlib import Path
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Person:
    full_name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


from psycopg2.extras import DictCursor


def test_film_work_data(db_name, user, password, host, port):
    """
    Test: data comparison for person table
    """
    dsl = {'dbname': db_name, 'user': user, 'password': password, 'host': host, 'port': port}
    object_path = Path(__file__).resolve().parent.parent.parent
    db_path = object_path / 'db.sqlite'

    with sqlite3.connect(db_path) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        sqlite_cursor = sqlite_conn.cursor()

        sqlite_person_data = {}
        pg_person_data = {}

        # extracting data from person sqlite table
        sqlite_cursor.execute('SELECT id, full_name FROM person')
        sqlite_data = sqlite_cursor.fetchall()
        sqlite_cursor.close()
        for data in sqlite_data:
            person = Person(id=data[0],
                            full_name=data[1])
            sqlite_person_data[person.id] = person

        # extracting data from content.person pg table
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute('SELECT id, full_name FROM content.person')
        pg_data = pg_cursor.fetchall()
        pg_cursor.close()
        for data in pg_data:
            person = Person(id=data[0],
                            full_name=data[1])
            pg_person_data[person.id] = person

        # assertion
        assert sqlite_person_data == pg_person_data
