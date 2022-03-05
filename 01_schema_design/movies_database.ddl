CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE INDEX film_work_creation_date_idx ON content.film_work(creation_date);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    film_work_id uuid NOT NULL REFERENCES content.film_work(id),
    person_id uuid NOT NULL REFERENCES content.person(id),
    role TEXT NOT NULL,
    created timestamp with time zone
);

CREATE UNIQUE INDEX film_work_person_idx ON content.person_film_work (film_work_id, person_id);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    description TEXT,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    genre_id uuid NOT NULL REFERENCES content.genre(id),
    film_work_id uuid NOT NULL REFERENCES content.film_work(id),
    created timestamp with time zone
);

CREATE UNIQUE INDEX film_work_genre_idx ON content.genre_film_work (genre_id, film_work_id);
