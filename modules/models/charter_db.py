import sqlite3
from typing import List

from modules.models.xml_archive import XmlArchive
from modules.models.xml_fond import XmlFond
from modules.models.xml_fond_charter import XmlFondCharter


class CharterDb:
    _con: sqlite3.Connection | None = None
    _cur: sqlite3.Cursor | None = None
    _path: str

    def __init__(self, path: str):
        self._path = path

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, __exc_type__, __exc_val__, __exc_tb__):
        self._close()

    def _connect(self):
        if not self._con:
            self._con = sqlite3.connect(self._path)
            self._cur = self._con.cursor()
            sqlite3.register_adapter(bool, int)
            sqlite3.register_converter("BOOLEAN", lambda v: bool(int(v)))

    def _close(self):
        if self._con:
            self._con.close()
            self._con = None
            self._cur = None

    def _reset_db(self):
        if not self._con or not self._cur:
            return
        # Drop all tables
        self._cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = self._cur.fetchall()
        for table in tables:
            table_name = table[0]
            self._cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Table {table_name} dropped successfully.")
        # Commit the changes
        self._con.commit()

    def setup_db(self):
        if not self._con or not self._cur:
            return
        # Reset the database
        self._reset_db()
        # Create the archives table
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS archives (
                id INTEGER PRIMARY KEY,
                atom_id TEXT NOT NULL,
                country_code CHAR(2) NOT NULL,
                name TEXT NOT NULL,
                repository_id TEXT NOT NULL,
                has_oai BOOLEAN DEFAULT FALSE
            )"""
        )
        # Create the archival fonds table
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS fonds (
                id INTEGER PRIMARY KEY,
                archive_id INTEGER NOT NULL,
                atom_id TEXT NOT NULL,
                free_image_access BOOLEAN NOT NULL DEFAULT FALSE,
                identifier TEXT NOT NULL,
                image_base TEXT,
                title TEXT NOT NULL,
                FOREIGN KEY (archive_id) REFERENCES archives(id)
            )"""
        )
        # Create the charters table
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS charters (
                id integer PRIMARY KEY,
                atom_id TEXT NOT NULL,
                idno_norm TEXT NOT NULL,
                idno_text TEXT NOT NULL,
                url TEXT NOT NULL
            )
            """
        )
        # Creato the fonds_charters table
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS fonds_charters (
                fond_id INTEGER NOT NULL,
                charter_id INTEGER NOT NULL,
                FOREIGN KEY (fond_id) REFERENCES fonds(id),
                FOREIGN KEY (charter_id) REFERENCES charters(id)
                PRIMARY KEY (fond_id, charter_id)
            )
            """
        )
        # Create the images table
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                is_external BOOLEAN NOT NULL DEFAULT TRUE
            )
            """
        )
        # Create the charter images table
        self._cur.execute(
            """CREATE TABLE IF NOT EXISTS charters_images (
                charter_id INTEGER NOT NULL,
                image_id INTEGER NOT NULL,
                FOREIGN KEY (charter_id) REFERENCES charters(id)
                FOREIGN KEY (image_id) REFERENCES images(id)
                PRIMARY KEY (charter_id, image_id)
            )
            """
        )
        # Commit the changes
        self._con.commit()

    def insert_archives(self, archives: List[XmlArchive]):
        if not self._con or not self._cur:
            return
        for archive in archives:
            self._cur.execute(
                "INSERT INTO archives (id, atom_id, country_code, name, repository_id, has_oai) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    archive.id,
                    archive.atom_id,
                    archive.countrycode,
                    archive.name,
                    archive.repository_id,
                    archive.oai is not None,
                ),
            )
        self._con.commit()

    def insert_fonds(self, fonds: List[XmlFond]):
        if not self._con or not self._cur:
            return
        for fond in fonds:
            self._cur.execute(
                "INSERT INTO fonds (id, archive_id, atom_id, free_image_access, identifier, image_base, title) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    fond.id,
                    fond.archive_id,
                    fond.atom_id,
                    fond.free_image_access,
                    fond.identifier,
                    fond.image_base,
                    fond.title,
                ),
            )
        self._con.commit()

    def insert_fonds_charters(self, charters: List[XmlFondCharter]):
        if not self._con or not self._cur:
            return
        for charter in charters:
            self._cur.execute(
                "INSERT INTO charters (id, atom_id, idno_norm, idno_text, url) VALUES (?, ?, ?, ?, ?)",
                (
                    charter.id,
                    charter.atom_id,
                    charter.idno_norm,
                    charter.idno_text,
                    charter.url,
                ),
            )
            self._cur.execute(
                "INSERT INTO fonds_charters (fond_id, charter_id) VALUES (?, ?)",
                (
                    charter.fond_id,
                    charter.id,
                ),
            )
            for image in charter.images:
                external_image = "images.monasterium.net" not in image
                self._cur.execute(
                    "INSERT OR IGNORE INTO images (url, is_external) VALUES (?, ?)",
                    (image, external_image),
                )
                self._cur.execute(
                    """INSERT OR IGNORE INTO charters_images (charter_id, image_id)
                        VALUES (
                            ?,
                            (SELECT id FROM images WHERE url = ?)
                        )
                    """,
                    (charter.id, image),
                )
        self._con.commit()

    def insert_images(self, images: List[str]):
        if not self._con or not self._cur:
            return
        for image in images:
            external_image = "images.monasterium.net" not in image
            self._cur.execute(
                "INSERT OR IGNORE INTO images (url, is_external) VALUES (?, ?)",
                (image, external_image),
            )
        self._con.commit()
