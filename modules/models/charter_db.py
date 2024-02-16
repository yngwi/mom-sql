from typing import Dict, List, Tuple

import psycopg
from psycopg import sql

from modules.models.xml_archive import XmlArchive
from modules.models.xml_collection import XmlCollection
from modules.models.xml_collection_charter import XmlCollectionCharter
from modules.models.xml_fond import XmlFond
from modules.models.xml_fond_charter import XmlFondCharter
from modules.models.xml_mycollection import XmlMycollection
from modules.models.xml_saved_charter import XmlSavedCharter
from modules.models.xml_user import XmlUser

SETUP_QUERIES = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        first_name TEXT,
        moderator_id INTEGER,
        name TEXT,
        FOREIGN KEY (moderator_id) REFERENCES users(id)
    );
    CREATE INDEX ON users (moderator_id);

    CREATE TABLE IF NOT EXISTS collections (
        id SERIAL PRIMARY KEY,
        atom_id TEXT NOT NULL UNIQUE,
        identifier TEXT NOT NULL,
        image_base TEXT,
        oai_shared BOOLEAN DEFAULT FALSE,
        title TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS private_collections (
        id SERIAL PRIMARY KEY,
        atom_id TEXT NOT NULL UNIQUE,
        identifier TEXT NOT NULL,
        title TEXT NOT NULL,
        owner_id INTEGER NOT NULL,
        FOREIGN KEY (owner_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS archives (
        id SERIAL PRIMARY KEY,
        atom_id TEXT NOT NULL UNIQUE,
        country_code CHAR(2) NOT NULL,
        name TEXT NOT NULL,
        oai_shared BOOLEAN DEFAULT FALSE,
        repository_id TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS fonds (
        id SERIAL PRIMARY KEY,
        archive_id INTEGER NOT NULL,
        atom_id TEXT NOT NULL UNIQUE,
        free_image_access BOOLEAN NOT NULL DEFAULT FALSE,
        identifier TEXT NOT NULL,
        image_base TEXT,
        oai_shared BOOLEAN DEFAULT FALSE,
        title TEXT NOT NULL,
        FOREIGN KEY (archive_id) REFERENCES archives(id)
    );
    CREATE INDEX ON fonds (archive_id);
    CREATE INDEX ON fonds (image_base);

    CREATE TABLE IF NOT EXISTS charters (
        id SERIAL PRIMARY KEY,
        atom_id TEXT NOT NULL UNIQUE,
        idno_id TEXT NOT NULL,
        idno_text TEXT NOT NULL,
        url TEXT NOT NULL,
        last_editor_id INTEGER,
        FOREIGN KEY (last_editor_id) REFERENCES users(id)
    );
    CREATE INDEX ON charters (last_editor_id);

    CREATE TABLE IF NOT EXISTS saved_charters (
        id SERIAL PRIMARY KEY,
        atom_id TEXT NOT NULL UNIQUE,
        editor_id INTEGER NOT NULL,
        idno_id TEXT NOT NULL,
        idno_text TEXT NOT NULL,
        is_released BOOLEAN NOT NULL DEFAULT FALSE,
        original_charter_id INTEGER NOT NULL,
        start_time TIMESTAMP NOT NULL,
        url TEXT NOT NULL,
        FOREIGN KEY (editor_id) REFERENCES users(id),
        FOREIGN KEY (original_charter_id) REFERENCES charters(id)
    );
    CREATE INDEX ON saved_charters (editor_id);

    CREATE TABLE IF NOT EXISTS collections_charters (
        collection_id INTEGER NOT NULL,
        charter_id INTEGER NOT NULL,
        FOREIGN KEY (collection_id) REFERENCES collections(id),
        FOREIGN KEY (charter_id) REFERENCES charters(id),
        PRIMARY KEY (collection_id, charter_id)
    );
    CREATE INDEX ON collections_charters (collection_id);
    CREATE INDEX ON collections_charters (charter_id);

    CREATE TABLE IF NOT EXISTS fonds_charters (
        fond_id INTEGER NOT NULL,
        charter_id INTEGER NOT NULL,
        FOREIGN KEY (fond_id) REFERENCES fonds(id),
        FOREIGN KEY (charter_id) REFERENCES charters(id),
        PRIMARY KEY (fond_id, charter_id)
    );
    CREATE INDEX ON fonds_charters (fond_id);
    CREATE INDEX ON fonds_charters (charter_id);

    CREATE TABLE IF NOT EXISTS collection_fonds (
        collection_id INTEGER NOT NULL,
        fond_id INTEGER NOT NULL,
        FOREIGN KEY (collection_id) REFERENCES collections(id),
        FOREIGN KEY (fond_id) REFERENCES fonds(id),
        PRIMARY KEY (collection_id, fond_id)
    );
    CREATE INDEX ON collection_fonds (collection_id);
    CREATE INDEX ON collection_fonds (fond_id);

    CREATE TABLE IF NOT EXISTS collection_source (
        collection_id INTEGER NOT NULL,
        source_collection_id INTEGER NOT NULL,
        FOREIGN KEY (collection_id) REFERENCES collections(id),
        FOREIGN KEY (source_collection_id) REFERENCES private_collections(id),
        PRIMARY KEY (collection_id, source_collection_id)
    );
    CREATE INDEX ON collection_source (collection_id);
    CREATE INDEX ON collection_source (source_collection_id);

    CREATE TABLE IF NOT EXISTS images (
        id SERIAL PRIMARY KEY,
        url TEXT UNIQUE NOT NULL,
        is_external BOOLEAN NOT NULL DEFAULT TRUE
    );

    CREATE TABLE IF NOT EXISTS charters_images (
        charter_id INTEGER NOT NULL,
        image_id INTEGER NOT NULL,
        FOREIGN KEY (charter_id) REFERENCES charters(id),
        FOREIGN KEY (image_id) REFERENCES images(id),
        PRIMARY KEY (charter_id, image_id)
    );
    CREATE INDEX ON charters_images (charter_id);
    CREATE INDEX ON charters_images (image_id);

    CREATE TABLE IF NOT EXISTS saved_charters_images (
        saved_charter_id INTEGER NOT NULL,
        image_id INTEGER NOT NULL,
        FOREIGN KEY (saved_charter_id) REFERENCES saved_charters(id),
        FOREIGN KEY (image_id) REFERENCES images(id),
        PRIMARY KEY (saved_charter_id, image_id)
    );
    CREATE INDEX ON saved_charters_images (saved_charter_id);
    CREATE INDEX ON saved_charters_images (image_id);

    CREATE TABLE IF NOT EXISTS user_charter_bookmarks (
        user_id INTEGER NOT NULL,
        charter_id INTEGER NOT NULL,
        note TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (charter_id) REFERENCES charters(id),
        PRIMARY KEY (user_id, charter_id)
    );
    CREATE INDEX ON user_charter_bookmarks (user_id);
    CREATE INDEX ON user_charter_bookmarks (charter_id);
    CREATE INDEX ON user_charter_bookmarks (note);
"""


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
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", [self._db])
                db_exists = cur.fetchone()
                if not db_exists:
                    print(f"Database {self._db} does not exist. Creating...")
                    cur.execute(
                        sql.SQL("CREATE DATABASE {};").format(sql.Identifier(self._db))
                    )

    def _list_tables(self) -> List[str]:
        if not self._con or not self._cur:
            return []
        self._cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        return [table[0] for table in self._cur.fetchall()]

    def _reset_db(self, reset_tables: List[str]):
        if not self._con or not self._cur:
            return
        tables: List[str] = (
            reset_tables if len(reset_tables) > 0 else self._list_tables()
        )
        for table in tables:
            self._cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table))
            )
            print(f"Table {table} dropped")
        self._con.commit()

    def setup_db(self, reset_tables: List[str] = []):
        if not self._con or not self._cur:
            return
        self._reset_db(reset_tables)
        self._cur.execute(SETUP_QUERIES)
        self._con.commit()

    def insert_collections(self, collections: List[XmlCollection]):
        if not self._con or not self._cur:
            return
        records = [
            [
                collection.id,
                collection.atom_id,
                collection.identifier,
                collection.image_base,
                collection.oai_shared,
                collection.title,
            ]
            for collection in collections
        ]
        with self._cur.copy(
            "COPY collections (id, atom_id, identifier, image_base, oai_shared, title) FROM STDIN"
        ) as copy:
            for record in records:
                copy.write_row(record)
        fonds_records = [
            (collection.id, fond_id)
            for collection in collections
            for fond_id in collection.linked_fonds
        ]
        with self._cur.copy(
            "COPY collection_fonds (collection_id, fond_id) FROM STDIN"
        ) as copy:
            for record in fonds_records:
                copy.write_row(record)
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
                archive.oai is not None,
                archive.repository_id,
            ]
            for archive in archives
        ]
        with self._cur.copy(
            "COPY archives (id, atom_id, country_code, name, oai_shared, repository_id) FROM STDIN"
        ) as copy:
            for record in records:
                copy.write_row(record)
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
                fond.oai_shared,
                fond.title,
            ]
            for fond in fonds
        ]
        with self._cur.copy(
            "COPY fonds (id, archive_id, atom_id, free_image_access, identifier, image_base, oai_shared, title) FROM STDIN"
        ) as copy:
            for record in records:
                copy.write_row(record)
        self._con.commit()

    def insert_saved_charters(self, charters: List[XmlSavedCharter]):
        if not self._con or not self._cur:
            return
        original_charter_atom_ids = [charter.atom_id for charter in charters]
        self._cur.execute(
            "SELECT id, atom_id FROM charters WHERE atom_id = ANY(%s)",
            (original_charter_atom_ids,),
        )
        atom_id_to_charter_id_map = {
            atom_id: id for id, atom_id in self._cur.fetchall()
        }
        valid_charters = []
        charter_records = []
        for charter in charters:
            if charter.url is None:
                print(f"URL not found for saved charter {charter.atom_id}")
                continue
            original_id = atom_id_to_charter_id_map.get(charter.atom_id, None)
            if original_id is None:
                print(f"Original charter not found for saved charter {charter.atom_id}")
                continue
            charter_records.append(
                [
                    charter.id,
                    charter.atom_id,
                    charter.editor_id,
                    charter.idno_id,
                    charter.idno_text,
                    charter.released,
                    original_id,
                    charter.start_time,
                    charter.url,
                ]
            )
            valid_charters.append(charter)
        with self._cur.copy(
            "COPY saved_charters (id, atom_id, editor_id, idno_id, idno_text, is_released, original_charter_id, start_time, url) FROM STDIN"
        ) as copy:
            for record in charter_records:
                copy.write_row(record)
        image_records = [
            [image, "images.monasterium.net" not in image]
            for charter in valid_charters
            for image in charter.images
        ]
        self._cur.executemany(
            "INSERT INTO images (url, is_external) VALUES (%s, %s) ON CONFLICT (url) DO NOTHING",
            image_records,
        )
        unique_urls = list(
            set([image for charter in valid_charters for image in charter.images])
        )
        self._cur.execute(
            "SELECT url, id FROM images WHERE url = ANY(%s)", (unique_urls,)
        )
        url_to_id_map = {url: id for url, id in self._cur.fetchall()}
        charters_images_records = [
            (charter.id, url_to_id_map[image])
            for charter in valid_charters
            for image in charter.images
            if image in url_to_id_map
        ]
        self._cur.executemany(
            "INSERT INTO saved_charters_images (saved_charter_id, image_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            charters_images_records,
        )
        self._con.commit()

    def insert_collections_charters(self, charters: List[XmlCollectionCharter]):
        if not self._con or not self._cur:
            return
        # Insert charters
        charter_records = [
            [
                charter.id,
                charter.atom_id,
                charter.idno_id,
                charter.idno_text,
                charter.url,
                charter.last_editor_id,
            ]
            for charter in charters
        ]
        with self._cur.copy(
            "COPY charters (id, atom_id, idno_id, idno_text, url, last_editor_id) FROM STDIN"
        ) as copy:
            for record in charter_records:
                copy.write_row(record)
        # Insert collections_charters
        collections_charters_records = [
            [charter.collection_id, charter.id] for charter in charters
        ]
        with self._cur.copy(
            "COPY collections_charters (collection_id, charter_id) FROM STDIN"
        ) as copy:
            for record in collections_charters_records:
                copy.write_row(record)
        # Insert images
        image_records = [
            [image, "images.monasterium.net" not in image]
            for charter in charters
            for image in charter.images
        ]
        self._cur.executemany(
            "INSERT INTO images (url, is_external) VALUES (%s, %s) ON CONFLICT (url) DO NOTHING",
            image_records,
        )
        # Insert charters_images
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

    def insert_fonds_charters(self, charters: List[XmlFondCharter]):
        if not self._con or not self._cur:
            return
        charter_records = [
            [
                charter.id,
                charter.atom_id,
                charter.idno_id,
                charter.idno_text,
                charter.url,
                charter.last_editor_id,
            ]
            for charter in charters
        ]
        # Insert charters
        with self._cur.copy(
            "COPY charters (id, atom_id, idno_id, idno_text, url, last_editor_id) FROM STDIN"
        ) as copy:
            for record in charter_records:
                copy.write_row(record)
        # Insert fonds_charters
        fonds_charters_records = [[charter.fond_id, charter.id] for charter in charters]
        with self._cur.copy(
            "COPY fonds_charters (fond_id, charter_id) FROM STDIN"
        ) as copy:
            for record in fonds_charters_records:
                copy.write_row(record)
        # Insert images
        image_records = [
            [image, "images.monasterium.net" not in image]
            for charter in charters
            for image in charter.images
        ]
        self._cur.executemany(
            "INSERT INTO images (url, is_external) VALUES (%s, %s) ON CONFLICT (url) DO NOTHING",
            image_records,
        )
        # Insert charters_images
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
        records_dict: Dict[str, Tuple[str, bool]] = {
            image: (image, "images.monasterium.net" not in image) for image in images
        }
        records = list(records_dict.values())
        with self._cur.copy("COPY images (url, is_external) FROM STDIN") as copy:
            for record in records:
                copy.write_row(record)
        self._con.commit()

    def insert_users(self, users: List[XmlUser]):
        if not self._con or not self._cur:
            return
        email_id_map = {user.email.lower(): user.id for user in users}
        records = [
            [
                user.id,
                user.email,
                user.first_name,
                user.name,
            ]
            for user in users
        ]
        with self._cur.copy(
            "COPY users (id, email, first_name, name) FROM STDIN"
        ) as copy:
            for record in records:
                copy.write_row(record)
        moderated_records = []
        for user in users:
            moderator_email = user.moderater_email
            if moderator_email is None:
                continue
            moderator_id = email_id_map.get(moderator_email.lower())
            if moderator_id is None or moderator_id == user.id:
                continue
            moderated_records.append((moderator_id, user.id))
        self._cur.executemany(
            "UPDATE users SET moderator_id = %s WHERE id = %s", moderated_records
        )
        self._con.commit()

    def insert_user_charter_bookmarks(self, users: List[XmlUser]):
        if not self._con or not self._cur:
            return
        unique_atom_ids: List[str] = list(
            set([bookmark.atom_id for user in users for bookmark in user.bookmarks])
        )
        self._cur.execute(
            "SELECT atom_id, id FROM charters WHERE atom_id = ANY(%s)",
            (unique_atom_ids,),
        )
        atom_id_to_charter_id_map: Dict[str, int] = {
            atom_id: charter_id for atom_id, charter_id in self._cur.fetchall()
        }
        user_bookmarks_records = [
            (user.id, atom_id_to_charter_id_map[bookmark.atom_id], bookmark.note)
            for user in users
            for bookmark in user.bookmarks
            if bookmark.atom_id in atom_id_to_charter_id_map
        ]
        self._cur.executemany(
            "INSERT INTO user_charter_bookmarks (user_id, charter_id, note) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            user_bookmarks_records,
        )
        self._con.commit()

    def insert_private_collections(self, mycollections: List[XmlMycollection]):
        if not self._con or not self._cur:
            return
        records = [
            [
                mycollection.id,
                mycollection.atom_id,
                mycollection.identifier,
                mycollection.title,
                mycollection.owner_id,
            ]
            for mycollection in mycollections
        ]
        with self._cur.copy(
            "COPY private_collections (id, atom_id, identifier, title, owner_id) FROM STDIN"
        ) as copy:
            for record in records:
                copy.write_row(record)
        self._con.commit()

    def insert_public_collections(self, mycollections: List[XmlMycollection]):
        if not self._con or not self._cur:
            return
        records = [
            [
                mycollection.id,
                mycollection.atom_id,
                mycollection.identifier,
                mycollection.oai_shared,
                mycollection.title,
            ]
            for mycollection in mycollections
        ]
        with self._cur.copy(
            "COPY collections (id, atom_id, identifier, oai_shared, title) FROM STDIN"
        ) as copy:
            for record in records:
                copy.write_row(record)
        source_records = [
            (mycollection.id, mycollection.private_mycollection_id)
            for mycollection in mycollections
            if mycollection.private_mycollection_id is not None
        ]
        with self._cur.copy(
            "COPY collection_source (collection_id, source_collection_id) FROM STDIN"
        ) as copy:
            for record in source_records:
                copy.write_row(record)
        self._con.commit()
