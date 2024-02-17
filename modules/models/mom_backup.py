import zipfile
from typing import Dict, List, Sequence

from lxml import etree

from modules.models.contents_xml import ContentsXml
from modules.models.xml_archive import XmlArchive
from modules.models.xml_charter import XmlCharter
from modules.models.xml_collection import XmlCollection
from modules.models.xml_collection_charter import XmlCollectionCharter
from modules.models.xml_fond import XmlFond
from modules.models.xml_fond_charter import XmlFondCharter
from modules.models.xml_mycharter import XmlMycharter
from modules.models.xml_mycollection import XmlMycollection
from modules.models.xml_saved_charter import XmlSavedCharter
from modules.models.xml_user import XmlUser
from modules.utils import join_url_parts


class MomBackup:
    path: str = ""
    zip: zipfile.ZipFile | None = None

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self.zip = zipfile.ZipFile(self.path, "r")
        return self

    def __exit__(self, __exc_type__, __exc_val__, __exc_tb__):
        if self.zip:
            self.zip.close()
            self.zip = None

    def _get_xml(self, path: str) -> etree._ElementTree:
        """
        Gets the XML file represented by the `path` from the backup zip.
        Raises an exception if it doesn't exist.
        """
        if not self.zip:
            raise Exception("Zip file not open")
        with self.zip.open(path) as contents:
            return etree.parse(contents)

    def _get_xml_optional(self, path: str) -> None | etree._ElementTree:
        """
        Gets the XML file represented by the `path` from the backup zip,
        or `None` if it doesn't exist. Raises an exception if the file has invalid XML.
        """
        try:
            return self._get_xml(path)
        except KeyError:
            # Ignore missing file exceptions
            return None

    def _get_contents(self, folder_path: str) -> ContentsXml:
        """
        Gets the collection contents for the folder represented by the `folder_path` from the backup zip.
        Raises an exception if it doesn't exist.
        """
        file_name = "__contents__.xml"
        path = (
            folder_path
            if folder_path.endswith(file_name)
            else join_url_parts(folder_path, file_name)
        )
        return ContentsXml(self._get_xml(path))

    def _list_resource_paths(self, base_path: str) -> List[str]:
        """
        Recursively lists all paths to resources within any subfolders from the given `base_path`.
        """
        resource_paths: List[str] = []
        try:
            contents = self._get_contents(base_path)
            for resource in contents.resources:
                resource_path = join_url_parts(base_path, resource.file)
                resource_paths.append(resource_path)
            for collection in contents.collections:
                collection_path = join_url_parts(base_path, collection.file)
                resource_paths.extend(self._list_resource_paths(collection_path))
            return resource_paths
        except KeyError:
            return []

    def _list_resources(self, base_path: str) -> List[etree._ElementTree]:
        """
        Recursively lists all resources within any subfolders from the given `base_path`.
        """
        resource_paths = self._list_resource_paths(base_path)
        return [self._get_xml(path) for path in resource_paths]

    def list_users(self) -> List[XmlUser]:
        users: Dict[str, XmlUser] = {}
        contents_path = "db/mom-data/xrx.user"
        for user_entry in self._get_contents(contents_path).resources:
            file = user_entry.file
            if file == "admin.xml" or file == "guest.xml":
                continue
            file_lower = file.lower()
            if file_lower in users:
                print(
                    f"Different case for {file}. Potential conflict with {users[file_lower].file}. Skipping."
                )
                continue
            xrx = self._get_xml(f"db/mom-data/xrx.user/{file}")
            bookmark_notes_path = (
                f"db/mom-data/xrx.user/{file.rsplit(".xml")[0]}/metadata.bookmark-notes"
            )
            bookmark_notes = self._list_resources(bookmark_notes_path)
            users[file_lower] = XmlUser(file, xrx, bookmark_notes)
        return list(users.values())

    def list_archives(self) -> List[XmlArchive]:
        archives: List[XmlArchive] = []
        contents_path = "db/mom-data/metadata.archive.public"
        for archive_entry in self._get_contents(contents_path).collections:
            file = archive_entry.file
            eag_path = f"db/mom-data/metadata.archive.public/{file}/{file}.eag.xml"
            oai_path = f"db/mom-data/metadata.archive.public/{file}/oai.xml"
            eag = self._get_xml_optional(eag_path)
            if eag is None:
                continue
            oai = self._get_xml_optional(oai_path)
            archives.append(XmlArchive(file, eag, oai))
        return archives

    def list_fonds(self, archives: List[XmlArchive]) -> List[XmlFond]:
        fonds: List[XmlFond] = []
        for archive in archives:
            contents_path = f"db/mom-data/metadata.fond.public/{archive.file}"
            for fond_entry in self._get_contents(contents_path).collections:
                fond_file = fond_entry.file
                ead_path = f"db/mom-data/metadata.fond.public/{archive.file}/{fond_file}/{fond_file}.ead.xml"
                preferences_path = f"db/mom-data/metadata.fond.public/{archive.file}/{fond_file}/{fond_file}.preferences.xml"
                ead = self._get_xml_optional(ead_path)
                if ead is None:
                    print(f"Failed to open fond ead {ead_path}")
                    continue
                preferences = self._get_xml_optional(preferences_path)
                fonds.append(XmlFond(fond_file, archive, ead, preferences))
        return fonds

    def list_fond_charters(
        self, fonds: List[XmlFond], users: List[XmlUser]
    ) -> List[XmlFondCharter]:
        charters: Dict[str, XmlFondCharter] = {}
        for fond in fonds:
            contents_path = (
                f"db/mom-data/metadata.charter.public/{fond.archive_file}/{fond.file}"
            )
            contents: None | ContentsXml = None
            try:
                contents = self._get_contents(contents_path)
            except KeyError:
                print(f"No content for fond {fond.archive_file}; {fond.identifier}")
                continue
            for charter_entry in contents.resources:
                charter_file = charter_entry.file
                cei_path = f"db/mom-data/metadata.charter.public/{fond.archive_file}/{fond.file}/{charter_file}"
                cei = self._get_xml_optional(cei_path)
                if cei is None:
                    print(f"Failed to open charter cei {cei_path}")
                    continue
                try:
                    charter = XmlFondCharter(charter_file, fond, cei, users)
                    if charter.atom_id in charters:
                        print(f"Duplicate charter {charter.atom_id}. Skipping.")
                        continue
                    else:
                        charters[charter.atom_id] = charter
                except Exception as e:
                    print(f"Failed to create charter {cei_path}: {e}")
        return list(charters.values())

    def list_collections(self, fonds: List[XmlFond]) -> List[XmlCollection]:
        collections: List[XmlCollection] = []
        contents_path = "db/mom-data/metadata.collection.public"
        for collection_entry in self._get_contents(contents_path).collections:
            file = collection_entry.file
            cei_path = f"db/mom-data/metadata.collection.public/{file}/{file}.cei.xml"
            cei = self._get_xml_optional(cei_path)
            if cei is None:
                print(f"Failed to open collection cei {cei_path}")
                continue
            collections.append(XmlCollection(file, cei, fonds))
        return collections

    def list_collection_charters(
        self, collections: List[XmlCollection], users: List[XmlUser]
    ) -> List[XmlCollectionCharter]:
        charters: Dict[str, XmlCollectionCharter] = {}
        for collection in collections:
            contents_path = f"db/mom-data/metadata.charter.public/{collection.file}"
            contents: None | ContentsXml = None
            try:
                contents = self._get_contents(contents_path)
            except KeyError:
                print(
                    f"No content for collection {collection.file}; {collection.identifier}"
                )
                continue
            for charter_entry in contents.resources:
                charter_file = charter_entry.file
                cei_path = f"db/mom-data/metadata.charter.public/{collection.file}/{charter_file}"
                cei = self._get_xml_optional(cei_path)
                if cei is None:
                    print(f"Failed to open charter cei {cei_path}")
                    continue
                try:
                    charter = XmlCollectionCharter(charter_file, collection, cei, users)
                    if charter.atom_id in charters:
                        print(f"Duplicate charter {charter.atom_id}. Skipping.")
                        continue
                    else:
                        charters[charter.atom_id] = charter
                except Exception as e:
                    print(f"Failed to create charter {cei_path}: {e}")
        return list(charters.values())

    def list_saved_charters(
        self,
        users: List[XmlUser],
        fonds: List[XmlFond],
        collections: List[XmlCollection],
    ) -> List[XmlSavedCharter]:
        charters_map: Dict[str, XmlSavedCharter] = {}
        contents_path = "db/mom-data/metadata.charter.saved"
        for saved_entry in self._get_contents(contents_path).resources:
            saved_file = saved_entry.file
            cei_path = f"db/mom-data/metadata.charter.saved/{saved_file}"
            cei = self._get_xml(cei_path)
            try:
                charter = XmlSavedCharter(saved_file, cei, users, fonds, collections)
                if charter.atom_id in charters_map:
                    print(f"Duplicate charter {charter.atom_id}. Skipping.")
                    continue
                else:
                    charters_map[charter.atom_id] = charter
            except Exception as e:
                print(f"Failed to create charter {contents_path}: {e}")
                continue
        charters: List[XmlSavedCharter] = []
        for user in users:
            for saved in user.saved_charters:
                if saved.atom_id in charters_map:
                    charter = charters_map[saved.atom_id]
                    charter.editor_id = user.id
                    charter.start_time = saved.start_time
                    charter.released = saved.released
                    charters.append(charter)
        return charters

    def list_private_charters(
        self,
        users: List[XmlUser],
        private_mycollections: List[XmlMycollection],
        charters: Sequence[XmlCharter],
    ) -> List[XmlMycharter]:
        mycharters: Dict[str, XmlMycharter] = {}
        charters_map: Dict[str, XmlCharter] = {c.atom_id: c for c in charters}
        for user in users:
            for mycollection in private_mycollections:
                if mycollection.author_email != user.email:
                    continue
                charters_path = f"db/mom-data/xrx.user/{user.email}/metadata.charter/{mycollection.identifier}"
                for path in self._list_resource_paths(charters_path):
                    file = path.rsplit("/")[0]
                    cei = self._get_xml(path)
                    try:
                        charter = XmlMycharter(file, cei, mycollection)
                        if charter.source_atom_id is not None:
                            source_charter = charters_map.get(
                                charter.source_atom_id, None
                            )
                            if source_charter is not None:
                                charter.set_source_charter(source_charter)
                        mycharters[charter.atom_id] = charter
                    except Exception as e:
                        print(f"Failed to create mycharter {path}: {e}")
        return list(mycharters.values())

    def list_public_charters(
        self,
        private_charters: List[XmlMycharter],
        public_mycollections: List[XmlMycollection],
    ) -> List[XmlCollectionCharter]:
        charters: List[XmlCollectionCharter] = []
        private_charters_map: Dict[str, XmlMycharter] = {
            c.owner_email + str(c.collection_atom_id) + c.atom_id: c
            for c in private_charters
        }
        for collection in public_mycollections:
            file = collection.file
            charters_path = f"db/mom-data/metadata.charter.public/{file}"
            for cei_path in self._list_resource_paths(charters_path):
                file = cei_path.rsplit("/")[-1]
                cei = self._get_xml(cei_path)
                try:
                    charter = XmlCollectionCharter(file, collection, cei)
                    print(
                        "Key",
                        collection.owner_email + collection.file + charter.atom_id,
                    )
                    source_charter = private_charters_map.get(
                        collection.owner_email + collection.atom_id + charter.atom_id,
                        None,
                    )
                    if source_charter is None:
                        print(
                            f"Failed to find source charter for {collection.owner_email}; {collection.atom_id}; {charter.atom_id}"
                        )
                        continue
                    charter.set_source_charter(source_charter)
                    charters.append(charter)
                except Exception as e:
                    print(f"Failed to create mycharter {cei_path}: {e}")
                    continue
        for key in list(private_charters_map.keys())[:20]:
            c = private_charters_map[key]
            print("key in src", key)
            print("charter", c.atom_id, c.owner_email, c.collection_file)
        return charters

    def list_private_mycollections(self, users: List[XmlUser]) -> List[XmlMycollection]:
        my_collections: Dict[str, XmlMycollection] = {}
        for user in users:
            file = user.file
            my_collections_path = (
                f"db/mom-data/xrx.user/{file.rsplit(".xml")[0]}/metadata.mycollection"
            )
            cei_files = self._list_resources(my_collections_path)
            for cei in cei_files:
                mycollection = XmlMycollection(file, cei, user)
                if mycollection.atom_id in my_collections:
                    print(
                        f"Duplicate mycollection {mycollection.file}/{mycollection.atom_id}. Skipping."
                    )
                    continue
                my_collections[mycollection.atom_id] = mycollection
        return list(my_collections.values())

    def list_public_mycollections(
        self, users: List[XmlUser], private_mycollections: List[XmlMycollection]
    ) -> List[XmlMycollection]:
        mycollections: List[XmlMycollection] = []
        contents_path = "db/mom-data/metadata.mycollection.public"
        for mycollection_entry in self._get_contents(contents_path).collections:
            file = mycollection_entry.file
            cei_path = f"db/mom-data/metadata.mycollection.public/{file}/{file}.mycollection.xml"
            cei = self._get_xml_optional(cei_path)
            if cei is None:
                print(f"Failed to open mycollection cei {cei_path}")
                continue
            mycollection = XmlMycollection(file, cei, None, True)
            oai_path = f"db/mom-data/metadata.mycollection.public/{file}/oai.xml"
            oai = self._get_xml_optional(oai_path)
            if oai is not None:
                mycollection.oai_shared = True
            user = next(
                (u for u in users if u.email == mycollection.author_email), None
            )
            if user is None:
                print(
                    f"Failed to find user for mycollection {mycollection.author_email}"
                )
                continue
            mycollection.set_user(user)
            private_mycollection = next(
                (
                    m
                    for m in private_mycollections
                    if m.atom_id == mycollection.atom_id
                    and m.author_email == user.email
                ),
                None,
            )
            if private_mycollection is None:
                print(
                    f"Failed to find private mycollection for {mycollection.atom_id} and {user.email}"
                )
                continue
            mycollection.private_mycollection_id = private_mycollection.id
            mycollections.append(mycollection)
        return mycollections
