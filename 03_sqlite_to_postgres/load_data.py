import datetime
import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from data_clases import Movie, Person, Genre, PersonFilmwork, GenreFilmwork


class SQLiteLoader:
    def __init__(self, connection):
        self._connection = connection

    def load_movies(self) -> list:
        result = []
        cursor = self._connection.cursor()
        cursor.execute('SELECT id, title, description, creation_date, rating, type FROM film_work')
        data = cursor.fetchall()

        for row in data:
            print(row)
            movie = Movie(id=row[0],
                          title=row[1],
                          description=row[2],
                          creation_date=row[3],
                          rating=row[4],
                          type=row[5])
            result.append(movie)

        return result

    def load_persons(self) -> list:
        result = []
        cursor = self._connection.cursor()
        cursor.execute('SELECT id, full_name FROM person')
        data = cursor.fetchall()

        for row in data:
            print(row)
            person = Person(id=row[0],
                            full_name=row[1])
            result.append(person)

        return result

    def load_genres(self) -> list:
        result = []
        cursor = self._connection.cursor()
        cursor.execute('SELECT id, name, description FROM genre')
        data = cursor.fetchall()

        for row in data:
            print(row)
            genre = Genre(id=row[0],
                          name=row[1],
                          description=row[2])
            result.append(genre)

        return result

    def load_person_filmwork(self) -> list:
        result = []
        cursor = self._connection.cursor()
        cursor.execute('SELECT id, person_id, film_work_id, role FROM person_film_work')
        data = cursor.fetchall()

        for row in data:
            print(row)
            person_film_work = PersonFilmwork(id=row[0],
                                              person_id=row[1],
                                              film_work_id=row[2],
                                              role=row[3])
            result.append(person_film_work)

        return result

    def load_genre_filmwork(self) -> list:
        result = []
        cursor = self._connection.cursor()
        cursor.execute('SELECT id, genre_id, film_work_id FROM genre_film_work')
        data = cursor.fetchall()

        for row in data:
            print(row)
            genre_film_work = GenreFilmwork(id=row[0],
                                            genre_id=row[1],
                                            film_work_id=row[2])
            result.append(genre_film_work)

        return result


class PostgresSaver:
    def __init__(self, connection):
        self._connection = connection

    def save_movies(self, movies: list) -> None:

        if not movies:
            raise ValueError("movies list is empty")

        dt_now = datetime.datetime.now()

        with self._connection.cursor() as cursor:

            for movie in movies:
                cursor.execute("""SELECT id FROM content.film_work WHERE id = %s""", (movie.id,))
                if cursor.fetchone():
                    continue

                cursor.execute("""INSERT INTO content.film_work (id, 
                title, 
                description, 
                creation_date, 
                rating, type, 
                created, modified)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                               (movie.id,
                                movie.title,
                                movie.description,
                                movie.creation_date,
                                movie.rating,
                                movie.type,
                                dt_now,
                                dt_now))

                self._connection.commit()

    def save_persons(self, persons: list) -> None:

        if not persons:
            raise ValueError("persons list is empty")

        dt_now = datetime.datetime.now()

        with self._connection.cursor() as cursor:

            for person in persons:
                cursor.execute("""SELECT id FROM content.person WHERE id = %s""", (person.id,))
                if cursor.fetchone():
                    continue

                cursor.execute("""INSERT INTO content.person (id, 
                full_name, 
                created, 
                modified)
                VALUES (%s, %s, %s, %s)""", (person.id,
                                             person.full_name,
                                             dt_now,
                                             dt_now))

                self._connection.commit()

    def save_genres(self, genres: list) -> None:

        if not genres:
            raise ValueError("genres list is empty")

        dt_now = datetime.datetime.now()

        with self._connection.cursor() as cursor:

            for genre in genres:
                cursor.execute("""SELECT id FROM content.genre WHERE id = %s""", (genre.id,))
                if cursor.fetchone():
                    continue

                cursor.execute("""INSERT INTO content.genre (id, 
                name, 
                description, 
                created, 
                modified)
                VALUES (%s, %s, %s, %s, %s)""", (genre.id,
                                                 genre.name,
                                                 genre.description,
                                                 dt_now,
                                                 dt_now))

                self._connection.commit()

    def save_person_filmwork(self, data: list) -> None:

        if not data:
            raise ValueError("person_film_work data is empty")

        dt_now = datetime.datetime.now()

        with self._connection.cursor() as cursor:

            for person_film_work in data:
                cursor.execute("""SELECT id FROM content.person_film_work WHERE id = %s""", (person_film_work.id,))
                if cursor.fetchone():
                    continue

                cursor.execute("""INSERT INTO content.person_film_work (id, 
                person_id, 
                film_work_id, 
                role, 
                created)
                VALUES (%s, %s, %s, %s, %s)""", (person_film_work.id,
                                                 person_film_work.person_id,
                                                 person_film_work.film_work_id,
                                                 person_film_work.role,
                                                 dt_now))

                self._connection.commit()

    def save_genre_filmwork(self, data: list) -> None:

        if not data:
            raise ValueError("genre_film_work data is empty")

        dt_now = datetime.datetime.now()

        with self._connection.cursor() as cursor:

            for genre_film_work in data:
                cursor.execute("""SELECT id FROM content.genre_film_work WHERE id = %s""", (genre_film_work.id,))
                if cursor.fetchone():
                    continue

                cursor.execute("""INSERT INTO content.genre_film_work (id, 
                genre_id, 
                film_work_id, 
                created)
                VALUES (%s, %s, %s, %s)""", (genre_film_work.id,
                                             genre_film_work.genre_id,
                                             genre_film_work.film_work_id,
                                             dt_now))

                self._connection.commit()


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    movies = sqlite_loader.load_movies()
    postgres_saver.save_movies(movies)

    persons = sqlite_loader.load_persons()
    postgres_saver.save_persons(persons)

    genres = sqlite_loader.load_genres()
    postgres_saver.save_genres(genres)

    data = sqlite_loader.load_person_filmwork()
    postgres_saver.save_person_filmwork(data)

    data = sqlite_loader.load_genre_filmwork()
    postgres_saver.save_genre_filmwork(data)


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
