import sqlite3
import json
import settings

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def get_writers():
    """
        extract data from sql-db
        :return:
        """
    connection = sqlite3.connect(settings.DB_PATH)
    cursor = connection.cursor()
    return {row[0]: row[1] for row in cursor.execute('SELECT DISTINCT * from writers WHERE name != "N/A"')}


def extract():
    """
    extract data from sql-db
    :return:
    """
    connection = sqlite3.connect(settings.DB_PATH)
    cursor = connection.cursor()

    # Получаем все поля для индекса, кроме списка актеров и сценаристов, для них только id
    cursor.execute("""
        SELECT
        m.id, m.imdb_rating, m.genre, m.title, m.plot, m.director,
        -- comma-separated actor_ids
        GROUP_CONCAT(DISTINCT a.id) as actor_ids, 
        -- comma-separated actor_names
        GROUP_CONCAT(DISTINCT REPLACE(a.name, "N/A", "")) as actor_names,
        (
            CASE WHEN m.writer == NULL or m.writer == ""
            THEN m.writers
            ELSE '[{"id":"'||m.writer||'"}]'
            END
        ) as writer_ids
        FROM movies as m
        LEFT JOIN movie_actors as ma ON m.id == ma.movie_id
        LEFT JOIN actors as a ON ma.actor_id == a.id
        GROUP BY m.id 
    """)

    return cursor.fetchall()


def transform(_writers, _raw_data):
    """

    :param _writers:
    :param _raw_data:
    :return:
    """
    documents_list = []
    for movie_info in _raw_data:
        # Разыменование списка
        movie_id, imdb_rating, genre, title, description, director, actors_id, actors_name, raw_writers = movie_info
        # Получаем список ID и имен актеров
        actors_ids = actors_id.split(",")
        actors_names = actors_name.split(",")
        actors_list = [
            {
                "id": actor[0],
                "name": actor[1]
            }
            for actor in zip(actors_ids, actors_names)
            if actor[1]
        ]

        # Получаем список writers
        writer_ids = list(set([writer_row['id'] for writer_row in json.loads(raw_writers)]))
        writer_names = [_writers.get(_id) for _id in writer_ids if _writers.get(_id)]
        writer_list = [
            {
                "id": writer[0],
                "name": writer[1]
            }
            for writer in zip(writer_ids, writer_names)
        ]

        document = {
            "_index": "movies",
            "_id": movie_id,
            "id": movie_id,
            "imdb_rating": imdb_rating,
            "genre": genre.split(', '),
            "title": title,
            "description": description,
            "director": director,
            "actors": actors_list,
            "writers": writer_list
        }

        for key in document.keys():
            if document[key] == 'N/A':
                document[key] = None

        document['actors_names'] = actors_name or None
        document['writers_names'] = writer_names or None

        import pprint
        pprint.pprint(document)

        documents_list.append(document)

    return documents_list


def load(acts):
    """

    :param acts:
    :return:
    """
    es = Elasticsearch([{
        'host': settings.ELASTIC_HOST,
        'port': settings.ELASTIC_PORT
    }])
    bulk(es, acts)

    return True


if __name__ == '__main__':
    load(transform(get_writers(), extract()))
