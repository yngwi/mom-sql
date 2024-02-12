from typing import List

import validators
from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator
from modules.models.xml_fond import XmlFond
from modules.models.xml_fond_charter import join_url_parts
from modules.utils import normalize_string


class XmlCollection:
    atom_id: str
    file: str
    id: int
    identifier: str
    linked_fonds: list[int]
    image_base: None | str = None
    oai_shared: bool = False
    title: str

    def __init__(self, file: str, cei: etree._ElementTree, fonds: List[XmlFond]):
        # id
        self.id = SerialIDGenerator().get_serial_id(XmlCollection)

        # file
        self.file = file

        # atom_id
        atom_id = cei.find("./atom:id", NAMESPACES)
        assert atom_id is not None
        assert atom_id.text is not None
        self.atom_id = str(atom_id.text)

        # identifier
        self.identifier = self.atom_id.rsplit("/", 1)[-1]

        # title
        provenance = cei.find(".//cei:provenance", NAMESPACES)
        assert provenance is not None
        text = normalize_string(provenance.xpath("./text()")[0])
        if text == "":
            title = cei.find(".//cei:title", NAMESPACES)
            if title is not None and title.text is not None:
                text = normalize_string(title.text)
            else:
                text = self.identifier
        self.title = text

        # oai_shared
        self.oai_shared = False

        # image_base
        address = cei.find(".//cei:image_server_address", NAMESPACES)
        folder = cei.find(".//cei:image_server_folder", NAMESPACES)
        if address is not None and address.text is not None:
            address_text = address.text
            if address_text == "images.monasterium.net":
                address_text = "http://images.monasterium.net"
            url = (
                join_url_parts(address_text, folder.text)
                if folder is not None and folder.text is not None
                else address.text
            )
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
