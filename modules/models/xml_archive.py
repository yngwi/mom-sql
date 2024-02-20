from typing import List, Optional

from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator
from modules.utils import normalize_string


class Oai:
    def __init__(self, record: etree._ElementTree):
        # fonds
        self.fonds: List[str] = [
            fond.text
            for fond in record.findall(".//oei:fond", NAMESPACES)
            if fond.text is not None
        ]

        # harvesters
        self.harvesters: List[str] = [
            harvester.text
            for harvester in record.findall(".//oei:harvester", NAMESPACES)
            if harvester.text is not None
        ]


class XmlArchive:
    def __init__(
        self,
        file: str,
        eag: etree._ElementTree,
        oai: Optional[etree._ElementTree] = None,
    ):
        # id
        self.id = SerialIDGenerator().get_serial_id(XmlArchive)

        # file
        self.file = file

        # atom_id
        self.atom_id = eag.findtext("./atom:id", "", NAMESPACES)
        assert self.atom_id != ""

        # repository_id
        id = eag.find(".//eag:repositorid", NAMESPACES)
        assert id is not None
        assert id.text is not None
        self.repository_id = str(id.text)

        # countrycode
        countrycode = str(id.attrib.get("countrycode", ""))
        assert countrycode != ""
        self.countrycode = countrycode

        # name
        self.name = normalize_string(eag.findtext(".//eag:autform", "", NAMESPACES))
        assert self.name != ""

        # oai
        self.oai = None if oai is None else Oai(oai)
