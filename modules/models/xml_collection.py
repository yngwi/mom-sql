from typing import List, Type

import validators
from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator, T
from modules.models.xml_fond import XmlFond
from modules.models.xml_fond_charter import join_url_parts
from modules.utils import normalize_string


class XmlCollection:
    atom_id: str
    author_email: None | str = None
    file: str
    id: int
    identifier: str
    linked_fonds: list[int]
    image_base: None | str = None
    oai_shared: bool = False
    title: str

    def __init__(
        self,
        file: str,
        cei: etree._ElementTree,
        fonds: List[XmlFond] = [],
        override_id_gen_name: None | Type[T] = None,
    ):
        # id
        self.id = SerialIDGenerator().get_serial_id(
            XmlCollection if override_id_gen_name is None else override_id_gen_name
        )

        # file
        self.file = file

        # atom_id
        atom_id = cei.findtext("./atom:id", "", NAMESPACES)
        assert atom_id != ""
        self.atom_id = atom_id

        # author_email
        self.author_email = cei.findtext(".//atom:email", None, NAMESPACES)

        # identifier
        self.identifier = self.atom_id.rsplit("/", 1)[-1]

        # title
        provenance = cei.find(".//cei:provenance", NAMESPACES)
        # Only get direct child text without any nested tags
        provenance_xpath = (
            provenance.xpath("./text()") if provenance is not None else []
        )
        if len(provenance_xpath) > 0:
            self.title = normalize_string(provenance_xpath[0])
        else:
            # If there is no provenance, try to get the title
            title = cei.findtext(".//cei:title", "", NAMESPACES)
            if title != "":
                self.title = normalize_string(title)
            # If there is no title, use the identifier
            else:
                self.title = self.identifier

        # oai_shared
        self.oai_shared = False

        # image_base
        address = cei.findtext(".//cei:image_server_address", "", NAMESPACES)
        folder = cei.findtext(".//cei:image_server_folder", "", NAMESPACES)
        if address != "":
            if address == "images.monasterium.net":
                address = "http://images.monasterium.net"
            url = join_url_parts(address, folder) if folder != "" else address
            if validators.url(url):
                self.image_base = url

        # linked_fonds
        linked_fonds = []
        for cei_text in cei.findall(".//cei:group/cei:text", NAMESPACES):
            if cei_text is not None:
                atom_id = cei_text.attrib.get("id")
                if atom_id is not None:
                    fond = next((f for f in fonds if f.atom_id == atom_id), None)
                    if fond is not None:
                        linked_fonds.append(fond.id)

        self.linked_fonds = linked_fonds
