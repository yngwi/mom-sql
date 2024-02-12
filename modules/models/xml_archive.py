from typing import List, Optional

from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator
from modules.utils import normalize_string


class Oai:
    fonds: List[str]
    harvesters: List[str]

    def __init__(self, record: etree._ElementTree):
        # fonds
        self.fonds = [
            fond.text
            for fond in record.findall(".//oei:fond", NAMESPACES)
            if fond.text is not None
        ]

        # harvesters
        self.harvesters = [
            harvester.text
            for harvester in record.findall(".//oei:harvester", NAMESPACES)
            if harvester.text is not None
        ]


class XmlArchive:
    atom_id: str
    countrycode: str
    file: str
    id: int
    name: str
    oai: None | Oai = None
    repository_id: str

    def __init__(
        self,
        file: str,
        eag: etree._ElementTree,
        oai: Optional[etree._ElementTree] = None,
    ):
        # id
        self.id = SerialIDGenerator().get_serial_id(self.__class__)

        # file
        self.file = file

        # atom_id
        atom_id = eag.find("./atom:id", NAMESPACES)
        assert atom_id is not None
        assert atom_id.text is not None
        self.atom_id = atom_id.text

        # repository_id
        id = eag.find(".//eag:repositorid", NAMESPACES)
        assert id is not None
        assert id.text is not None
        self.repository_id = id.text

        # countrycode
        countrycode = id.attrib.get("countrycode")
        assert countrycode is not None
        self.countrycode = countrycode

        # name
        name = eag.find(".//eag:autform", NAMESPACES)
        assert name is not None
        assert name.text is not None
        self.name = normalize_string(name.text)

        # oai
        self.oai = None if oai is None else Oai(oai)
