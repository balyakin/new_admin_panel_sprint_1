"""
Dataclases definition for Sprint-1 database
"""
import uuid
import datetime

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Movie:
    title: str
    description: str
    creation_date: datetime.datetime
    type: str
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class Genre:
    name: str
    description: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class GenreFilmwork:
    genre_id: uuid.UUID
    film_work_id: uuid.UUID
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class Person:
    full_name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class PersonFilmwork:
    person_id: uuid.UUID
    film_work_id: uuid.UUID
    role: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
