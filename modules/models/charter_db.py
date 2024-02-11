from typing import List

import psycopg
from psycopg import sql

from modules.models.xml_archive import XmlArchive
from modules.models.xml_fond import XmlFond
from modules.models.xml_fond_charter import XmlFondCharter


class CharterDb:
    _db: str
    _host: str
    _password: str
    _port: int
    _user: str

    _con: psycopg.connection.Connection | None = None
    _cur: psycopg.cursor.Cursor | None = None

    def __init__(self, host, password, port=5432, user="postgres", db="momcheck"):
        self._db = db
        self._host = host
        self._password = password
        self._port = port
        self._user = user

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, __exc_type__, __exc_val__, __exc_tb__):
        self._close()

    def _connect(self):
        if not self._con:
            self._create_db()
            dsn = f"dbname='{self._db}' user='{self._user}' host='{self._host}' password='{self._password}' port='{self._port}'"
            self._con = psycopg.connect(dsn)
            self._cur = self._con.cursor()

    def _close(self):
        if self._con:
            self._con.close()
            self._con = None
            self._cur = None

    def _create_db(self):
        dsn = f"dbname='postgres' user='{self._user}' host='{self._host}' password='{self._password}' port='{self._port}'"
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", [self._db])
                db_exists = cur.fetchone()
                if not db_exists:
                    print(f"Database {self._db} does not exist. Creating...")
                    cur.execute(
                        sql.SQL("CREATE DATABASE {};").format(sql.Literal(self._db))
                    )
                conn.commit()

    def _reset_db(self, including_images=False):
        if not self._con or not self._cur:
            return
        self._cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        tables = self._cur.fetchall()
        for table in tables:
            if table[0] == "images" and not including_images:
                print("Skipping images table.")
                continue
            self._cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                    sql.Identifier(table[0])
                )
            )
            print(f"Table {table[0]} dropped successfully.")
        self._con.commit()

    def setup_db(self):
        if not self._con or not self._cur:
            return
        self._reset_db()
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS archives (
                id SERIAL PRIMARY KEY,
                atom_id TEXT NOT NULL,
                country_code CHAR(2) NOT NULL,
                name TEXT NOT NULL,
                repository_id TEXT NOT NULL,
                has_oai BOOLEAN DEFAULT FALSE
            )"""
        )
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS fonds (
                id SERIAL PRIMARY KEY,
                archive_id INTEGER NOT NULL,
                atom_id TEXT NOT NULL,
                free_image_access BOOLEAN NOT NULL DEFAULT FALSE,
                identifier TEXT NOT NULL,
                image_base TEXT,
                title TEXT NOT NULL,
                FOREIGN KEY (archive_id) REFERENCES archives(id)
            )"""
        )
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS charters (
                id SERIAL PRIMARY KEY,
                atom_id TEXT NOT NULL,
                idno_norm TEXT NOT NULL,
                idno_text TEXT NOT NULL,
                url TEXT NOT NULL
            )
            """
        )
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS fonds_charters (
                fond_id SERIAL NOT NULL,
                charter_id INTEGER NOT NULL,
                FOREIGN KEY (fond_id) REFERENCES fonds(id),
                FOREIGN KEY (charter_id) REFERENCES charters(id),
                PRIMARY KEY (fond_id, charter_id)
            )
            """
        )
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS images (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                is_external BOOLEAN NOT NULL DEFAULT TRUE
            )
            """
        )
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS charters_images (
                charter_id INTEGER NOT NULL,
                image_id INTEGER NOT NULL,
                FOREIGN KEY (charter_id) REFERENCES charters(id),
                FOREIGN KEY (image_id) REFERENCES images(id),
                PRIMARY KEY (charter_id, image_id)
            )
            """
        )
        self._cur.execute("CREATE INDEX ON fonds (archive_id);")
        self._cur.execute("CREATE INDEX ON fonds_charters (fond_id);")
        self._cur.execute("CREATE INDEX ON fonds_charters (charter_id);")
        self._con.execute("CREATE INDEX ON charters_images (charter_id);")
        self._con.execute("CREATE INDEX ON charters_images (image_id);")
        self._con.commit()

    def insert_archives(self, archives: List[XmlArchive]):
        if not self._con or not self._cur:
            return
        records = [
            [
                archive.id,
                archive.atom_id,
                archive.countrycode,
                archive.name,
                archive.repository_id,
                archive.oai is not None,
            ]
            for archive in archives
        ]
        self._cur.executemany(
            "INSERT INTO archives (id, atom_id, country_code, name, repository_id, has_oai) VALUES (%s, %s, %s, %s, %s, %s)",
            records,
        )
        self._con.commit()

    def insert_fonds(self, fonds: List[XmlFond]):
        if not self._con or not self._cur:
            return
        records = [
            [
                fond.id,
                fond.archive_id,
                fond.atom_id,
                fond.free_image_access,
                fond.identifier,
                fond.image_base,
                fond.title,
            ]
            for fond in fonds
        ]
        self._cur.executemany(
            "INSERT INTO fonds (id, archive_id, atom_id, free_image_access, identifier, image_base, title) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            records,
        )
        self._con.commit()

    def insert_fonds_charters(self, charters: List[XmlFondCharter]):
        if not self._con or not self._cur:
            return
        charter_records = [
            [
                charter.id,
                charter.atom_id,
                charter.idno_norm,
                charter.idno_text,
                charter.url,
            ]
            for charter in charters
        ]
        self._cur.executemany(
            "INSERT INTO charters (id, atom_id, idno_norm, idno_text, url) VALUES (%s, %s, %s, %s, %s)",
            charter_records,
        )
        fonds_charters_records = [[charter.fond_id, charter.id] for charter in charters]
        self._cur.executemany(
            "INSERT INTO fonds_charters (fond_id, charter_id) VALUES (%s, %s)",
            fonds_charters_records,
        )
        image_records = [
            [image, "images.monasterium.net" not in image]
            for charter in charters
            for image in charter.images
        ]
        self._cur.executemany(
            "INSERT INTO images (url, is_external) VALUES (%s, %s) ON CONFLICT (url) DO NOTHING",
            image_records,
        )
        unique_urls = list(
            set([image for charter in charters for image in charter.images])
        )
        self._cur.execute(
            "SELECT url, id FROM images WHERE url = ANY(%s)", (unique_urls,)
        )
        url_to_id_map = {url: id for url, id in self._cur.fetchall()}
        charters_images_records = [
            (charter.id, url_to_id_map[image])
            for charter in charters
            for image in charter.images
            if image in url_to_id_map
        ]
        self._cur.executemany(
            "INSERT INTO charters_images (charter_id, image_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            charters_images_records,
        )
        self._con.commit()

    def insert_images(self, images: List[str]):
        if not self._con or not self._cur:
            return
        records = [[image, "images.monasterium.net" not in image] for image in images]
        self._cur.executemany(
            "INSERT INTO images (url, is_external) VALUES (%s, %s) ON CONFLICT (url) DO NOTHING",
            records,
        )
        self._con.commit()
