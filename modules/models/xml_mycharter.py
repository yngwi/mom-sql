from typing import Dict, List

from lxml import etree

from modules.constants import NAMESPACES
from modules.logger import Logger
from modules.models.person_index import PersonIndex
from modules.models.xml_charter import XmlCharter
from modules.models.xml_mycollection import XmlMycollection
from modules.models.xml_user import XmlUser

log = Logger()


class XmlMycharter(XmlCharter):
    def __init__(
        self,
        file: str,
        cei: etree._ElementTree,
        collection: XmlMycollection,
        person_index: PersonIndex,
    ):
        # url
        url = f"https://www.monasterium.net/mom/{collection.file}/{collection.file}/{file.split('.cei.xml')[0]}/my-charter"

        # init base charter
        super().__init__(file, cei, None, url, person_index, [], XmlMycharter)

        # owner_id
        self.owner_id = collection.owner_id

        # owner_email
        self.owner_email = collection.owner_email

        # collection_id
        self.collection_id = collection.id

        # collection_atom_id
        self.collection_atom_id = collection.atom_id

        # collection_file
        self.collection_file = collection.file

        # source_atom_id
        self.source_atom_id = None
        atom_link = cei.find(".//atom:link", NAMESPACES)
        if atom_link is not None:
            atom_id: None | str = atom_link.attrib.get("ref", None)
            if atom_id is not None:
                self.source_atom_id = str(atom_id)

        # source_charter_id
        self.source_charter_id = None

        # shared_with_user_ids
        self.shared_with_user_ids: List[int] = []

    def set_source_charter(self, source_charter: XmlCharter):
        self.source_charter_id = source_charter.id

    def add_shared_users(self, xrx: etree._ElementTree, users: Dict[str, XmlUser]):
        unique_ids: Dict[int, int] = {}
        for user in xrx.xpath(
            ".//xrx:userid[not(@type='owner')]", namespaces=NAMESPACES
        ):
            user_email = user.text
            if user_email in users:
                id = users[user_email].id
                if id in unique_ids:
                    log.warn(f"User {user_email} already shared with charter {self.id}")
                    continue
                unique_ids[id] = id
            self.shared_with_user_ids = list(unique_ids.keys())
