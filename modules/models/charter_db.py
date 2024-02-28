import io
from datetime import date
from typing import Dict, List, LiteralString, Set, Tuple, cast

import psycopg
from lxml import etree
from psycopg import sql
from psycopg.types.range import Range

from modules.constants import IndexLocation
from modules.logger import Logger
from modules.models.person_index import PersonIndex
from modules.models.xml_archive import XmlArchive
from modules.models.xml_collection import XmlCollection
from modules.models.xml_collection_charter import XmlCollectionCharter
from modules.models.xml_fond import XmlFond
from modules.models.xml_fond_charter import XmlFondCharter
from modules.models.xml_mycharter import XmlMycharter
from modules.models.xml_mycollection import XmlMycollection
from modules.models.xml_saved_charter import XmlSavedCharter
from modules.models.xml_user import XmlUser

log = Logger()


def _read_sql_file(path: str) -> LiteralString:
    with open(path, "r") as file:
        return cast(LiteralString, file.read())


def _dates_to_range(date_range: None | Tuple[date, date]) -> None | Range:
    if date_range is None:
        return None
    lower, upper = date_range
    bounds = "[]"
    return Range(lower=lower, upper=upper, bounds=bounds)


def _serialize_xml(element: None | etree._Element) -> None | str:
    if element is None:
        return None
    string = etree.tostring(element, encoding="unicode", pretty_print=True).strip()
    if string[0] != "<" or string[-1] != ">":
        try:
            parser = etree.XMLParser(recover=True)
            root = etree.parse(io.StringIO(string), parser).getroot()
            cleaned_xml = etree.tostring(
                root, encoding="unicode", pretty_print=True
            ).strip()
            return None if cleaned_xml == "" else cleaned_xml
        except etree.XMLSyntaxError as e:
            raise Exception(f"Error parsing invalid XML: {e}")
    return string


class CharterDb:
    def __init__(self, host, password, port=5432, user="postgres", db="momcheck"):
        self._db = db
        self._host = host
        self._password = password
        self._port = port
        self._user = user
        self._con: psycopg.connection.Connection | None = None
        self._cur: psycopg.cursor.Cursor | None = None

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
                    log.debug(f"Database {self._db} does not exist. Creating...")
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

    def _reset_db(self):
        if not self._con or not self._cur:
            return
        # Drop tables
        tables: List[str] = self._list_tables()
        for table in tables:
            self._cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table))
            )
            log.debug(f"Table {table} dropped")
        # Drop all functions
        self._cur.execute(
            """
            SELECT 'DROP FUNCTION IF EXISTS ' || ns.nspname || '.' || proname || '(' || oidvectortypes(proargtypes) || ');'
            FROM pg_proc INNER JOIN pg_namespace ns ON (pg_proc.pronamespace = ns.oid)
            WHERE ns.nspname NOT IN ('pg_catalog', 'information_schema')
        """
        )
        functions = self._cur.fetchall()
        for func in functions:
            self._cur.execute(func[0])
            log.debug(f"Function dropped: {func[0]}")
        self._con.commit()

    def reset_serial_id_sequences(self):
        if not self._con or not self._cur:
            return
        query = """
            SELECT sequence_name, table_name, column_name 
            FROM information_schema.sequences 
            JOIN information_schema.columns 
                ON CONCAT(columns.table_name, '_', columns.column_name, '_seq') = sequences.sequence_name
            WHERE sequence_schema = 'public';
        """
        self._cur.execute(query)
        sequences = self._cur.fetchall()
        for sequence_name, table_name, column_name in sequences:
            self._cur.execute(
                sql.SQL(
                    "SELECT setval('{sequence}', COALESCE((SELECT MAX({column}) FROM {table}) + 1, 1), false);"
                ).format(
                    sequence=sql.Identifier(sequence_name),
                    column=sql.Identifier(column_name),
                    table=sql.Identifier(table_name),
                ),
            )

        self._con.commit()

    def _setup_db_structures(self):
        if not self._con or not self._cur:
            return
        self._cur.execute(_read_sql_file("sql/tables.sql"))
        self._cur.execute(_read_sql_file("sql/functions.sql"))
        self._cur.execute(_read_sql_file("sql/alterations.sql"))
        self._con.commit()

    def setup_db(self):
        self._reset_db()
        self._setup_db_structures()

    def enable_triggers(self):
        if not self._con or not self._cur:
            return
        self._cur.execute(_read_sql_file("sql/triggers.sql"))
        self._con.commit()

    def insert_index_locations(self):
        if not self._con or not self._cur:
            return
        for location in IndexLocation:
            self._cur.execute(
                "INSERT INTO index_locations (id, location) VALUES (%s, %s)",
                (
                    location.value,
                    location.name,
                ),
            )

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
                log.warn(f"URL not found for saved charter {charter.atom_id}")
                continue
            original_id = atom_id_to_charter_id_map.get(charter.atom_id, None)
            if original_id is None:
                log.warn(
                    f"Original charter not found for saved charter {charter.atom_id}"
                )
                continue
            charter_records.append(
                [
                    charter.id,
                    _serialize_xml(charter.abstract),
                    charter.atom_id,
                    charter.editor_id,
                    charter.idno_id,
                    charter.idno_text,
                    charter.released,
                    original_id,
                    charter.start_time,
                    _serialize_xml(charter.tenor),
                    charter.url,
                    _dates_to_range(charter.issued_date),
                    charter.issued_date_text,
                    charter.sort_date,
                ]
            )
            valid_charters.append(charter)
        with self._cur.copy(
            "COPY saved_charters (id, abstract, atom_id, editor_id, idno_id, idno_text, is_released, original_charter_id, start_time, tenor, url, issued_date, issued_date_text, sort_date) FROM STDIN"
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
                _serialize_xml(charter.abstract),
                charter.atom_id,
                charter.idno_id,
                charter.idno_text,
                charter.url,
                charter.last_editor_id,
                _dates_to_range(charter.issued_date),
                charter.issued_date_text,
                charter.sort_date,
                _serialize_xml(charter.tenor),
            ]
            for charter in charters
        ]
        with self._cur.copy(
            "COPY charters (id, abstract, atom_id, idno_id, idno_text, url, last_editor_id, issued_date, issued_date_text, sort_date, tenor) FROM STDIN"
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
                _serialize_xml(charter.abstract),
                charter.atom_id,
                charter.idno_id,
                charter.idno_text,
                charter.url,
                charter.last_editor_id,
                _dates_to_range(charter.issued_date),
                charter.issued_date_text,
                charter.sort_date,
                _serialize_xml(charter.tenor),
            ]
            for charter in charters
        ]
        # Insert charters
        with self._cur.copy(
            "COPY charters (id, abstract, atom_id, idno_id, idno_text, url, last_editor_id, issued_date, issued_date_text, sort_date, tenor) FROM STDIN"
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

    def insert_public_mycollections(self, mycollections: List[XmlMycollection]):
        if not self._con or not self._cur:
            return
        records = [
            [
                mycollection.id,
                mycollection.atom_id,
                mycollection.identifier,
                mycollection.oai_shared,
                mycollection.private_mycollection_id,
                mycollection.title,
            ]
            for mycollection in mycollections
        ]
        with self._cur.copy(
            "COPY collections (id, atom_id, identifier, oai_shared, source_collection_id, title) FROM STDIN"
        ) as copy:
            for record in records:
                copy.write_row(record)
        self._con.commit()

    def insert_private_mycharters(self, charters: List[XmlMycharter]):
        if not self._con or not self._cur:
            return
        # Insert charters
        records = [
            [
                charter.id,
                _serialize_xml(charter.abstract),
                charter.atom_id,
                charter.collection_id,
                charter.idno_id,
                charter.idno_text,
                charter.source_charter_id,
                _dates_to_range(charter.issued_date),
                charter.issued_date_text,
                charter.sort_date,
                _serialize_xml(charter.tenor),
            ]
            for charter in charters
        ]
        with self._cur.copy(
            "COPY private_charters (id, abstract, atom_id, private_collection_id, idno_id, idno_text, source_charter_id, issued_date, issued_date_text, sort_date, tenor) FROM STDIN"
        ) as copy:
            for record in records:
                copy.write_row(record)
        # Insert user shares
        user_shares_records = [
            (charter.id, user_id)
            for charter in charters
            for user_id in charter.shared_with_user_ids
        ]
        with self._cur.copy(
            "COPY private_charter_user_shares (private_charter_id, user_id) FROM STDIN"
        ) as copy:
            for record in user_shares_records:
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
            "INSERT INTO private_charters_images (private_charter_id, image_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            charters_images_records,
        )
        # Commit
        self._con.commit()

    def insert_public_mycharters(self, charters: List[XmlCollectionCharter]):
        if not self._con or not self._cur:
            return
        # Insert charters
        charter_records = [
            [
                charter.id,
                _serialize_xml(charter.abstract),
                charter.atom_id,
                charter.idno_id,
                charter.idno_text,
                charter.url,
                charter.last_editor_id,
                _dates_to_range(charter.issued_date),
                charter.issued_date_text,
                charter.sort_date,
                _serialize_xml(charter.tenor),
            ]
            for charter in charters
        ]
        with self._cur.copy(
            "COPY charters (id, abstract, atom_id, idno_id, idno_text, url, last_editor_id, issued_date, issued_date_text, sort_date, tenor) FROM STDIN"
        ) as copy:
            for record in charter_records:
                copy.write_row(record)
        # Insert collections_charters
        collections_charters_records = [
            [charter.collection_id, charter.id, charter.source_mycharter_id]
            for charter in charters
        ]
        with self._cur.copy(
            "COPY collections_charters (collection_id, charter_id, private_charter_id) FROM STDIN"
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

    def insert_persons(
        self,
        person_index: PersonIndex,
        public_charters: List[XmlFondCharter | XmlCollectionCharter],
        private_charters: List[XmlMycharter],
        saved_charters: List[XmlSavedCharter],
    ):
        if not self._con or not self._cur:
            return
        # collect persons
        person_records = [
            [
                person.id,
                ";".join(person.names),
                person.mom_iri,
                person.wikidata_iri,
            ]
            for person in person_index.list_persons()
        ]
        with self._cur.copy(
            "COPY persons (id, label, mom_iri, wikidata_iri) FROM STDIN"
        ) as copy:
            for record in person_records:
                copy.write_row(record)
        person_name_records = []
        charters_person_name_records = []
        # collect public charters person names
        for charter in public_charters:
            for person_name in charter.person_names:
                person_name_records.append(
                    [
                        person_name.id,
                        person_name.person_id,
                        person_name.text,
                        person_name.reg,
                        person_name.key,
                        person_name.location.value,
                    ]
                )
                charters_person_name_records.append(
                    [
                        person_name.charter_id,
                        person_name.id,
                    ]
                )
        # collect private charters person names
        private_charters_person_name_records = []
        for charter in private_charters:
            for person_name in charter.person_names:
                person_name_records.append(
                    [
                        person_name.id,
                        person_name.person_id,
                        person_name.text,
                        person_name.reg,
                        person_name.key,
                        person_name.location.value,
                    ]
                )
                private_charters_person_name_records.append(
                    [
                        person_name.charter_id,
                        person_name.id,
                    ]
                )
        # collect saved charters person names
        self._cur.execute("SELECT id FROM saved_charters")
        id_set: Set[int] = {id[0] for id in self._cur.fetchall()}
        saved_charters_person_names = []
        for charter in saved_charters:
            for person_name in charter.person_names:
                if person_name.charter_id not in id_set:
                    # Skip person names that are from saved charters that
                    # could not be saved in the database
                    continue
                person_name_records.append(
                    [
                        person_name.id,
                        person_name.person_id,
                        person_name.text,
                        person_name.reg,
                        person_name.key,
                        person_name.location.value,
                    ]
                )
                saved_charters_person_names.append(
                    [
                        person_name.charter_id,
                        person_name.id,
                    ]
                )
        # insert person name records
        with self._cur.copy(
            "COPY person_names (id, person_id, text, reg, key, location_id) FROM STDIN"
        ) as copy:
            for record in person_name_records:
                copy.write_row(record)
        # insert charters person name records
        with self._cur.copy(
            "COPY charters_person_names (charter_id, person_name_id) FROM STDIN"
        ) as copy:
            for record in charters_person_name_records:
                copy.write_row(record)
        # insert private charters person name records
        with self._cur.copy(
            "COPY private_charters_person_names (private_charter_id, person_name_id) FROM STDIN"
        ) as copy:
            for record in private_charters_person_name_records:
                copy.write_row(record)
        # insert saved charters person name records
        with self._cur.copy(
            "COPY saved_charters_person_names (saved_charter_id, person_name_id) FROM STDIN"
        ) as copy:
            for record in saved_charters_person_names:
                copy.write_row(record)
        self._con.commit()
