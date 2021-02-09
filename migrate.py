
import json
import sqlite3
import requests

conn = sqlite3.connect('db.sqlite')


class Rec:

    def __init__(self, row):
        movie_id, genre, director, writer_id, title, plot, _, imdb_rating, writers_json = row
        self.id = movie_id
        self.imdb_rating = float(imdb_rating) if imdb_rating != 'N/A' else None
        self.genre = genre if genre != 'N/A' else None
        self.title = title if title != 'N/A' else None
        self.description = plot if plot != 'N/A' else None
        self.director = director if director != 'N/A' else None
        self.writers_names, self.writers = self.compouse_writers(writer_id, writers_json)
        self.actors_names, self.actors = self.compouse_actors()

    def compouse_actors(self):
        names = []
        actors = []
        ids = set()
        for _, a in conn.execute(f'SELECT * from movie_actors WHERE movie_id="{self.id}"').fetchall():
            if a not in ids:
                ids.add(a)
                name, aname = self.fetcher('actors', a)
                names.append(name) if name is not None else None
                actors.extend(aname)
        return ', '.join(names), actors


    def fetcher(self, tname, ids):
        fetch = conn.execute(f'SELECT name from {tname} WHERE id="{ids}"')
        name = fetch.fetchone()[0]
        name = None if name == 'N/A' else name
        return name, [{'id': ids, 'name': name}]

    def compouse_writers(self, writer_id, writers_json):
        if writer_id:
            return self.fetcher('writers', writer_id)
        names = []
        writers = []
        ids = set()
        for x in json.loads(writers_json):
            w = x.get('id')
            if w and w not in ids:
                ids.add(w)
                name, wname = self.fetcher('writers', w)
                names.append(name) if name is not None else None
                writers.extend(wname)
        return ', '.join(names), writers

    def bulk(self):
        lead = json.dumps({"index": {"_index": "movies", "_id": self.id}})
        vals = json.dumps({"id": self.id, "imdb_rating": self.imdb_rating, "genre": self.genre, "title": self.title,
                           "description": self.description, "director": self.director, "writers": self.writers,
                           "writers_names": self.writers_names, "actors_names": self.actors_names, "actors": self.actors
                           })
        return f'{lead}\n{vals}\n'



URL = 'http://127.0.0.1:9200/_bulk'

if __name__ == '__main__':
    res = conn.execute('SELECT * from movies')
    buf = ''
    with open('logfile', "w") as logfile:
        for n, row in enumerate(res):
            if n % 20 == 0 and n > 0:
                logfile.write(buf)
                logfile.write('\n\n\n')
                r = requests.post(URL, data=buf, headers={'content-type': 'application/json'})
                logfile.write(f'{r.status_code}')
                logfile.write(f'{r.json()}')
                buf = ''
                logfile.write('=' * 50)
                logfile.write('\n\n\n')
            rec = Rec(row)
            buf = f'{buf}{rec.bulk()}'

        logfile.write(buf)
        logfile.write('\n\n\n')
        r = requests.post(URL, data=buf, headers={'content-type': 'application/json'})
        logfile.write(f'{r.status_code}')
        logfile.write(f'{r.json()}')
