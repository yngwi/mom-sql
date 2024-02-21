from lxml import etree

from modules.constants import NAMESPACES, IndexLocation
from modules.models.serial_id_generator import SerialIDGenerator
from modules.utils import normalize_string


class XmlPersonName:
    def __init__(self, charter_id: int, cei: etree._Element, location: IndexLocation):
        # id
        self.id = SerialIDGenerator().get_serial_id(XmlPersonName)

        # charter_id
        self.charter_id = charter_id

        # locations
        self.location = location

        # person_id
        self.person_id: None | int = None

        # text
        self.text = normalize_string(
            "".join(cei.xpath(".//text()", namespaces=NAMESPACES))
        )

        # reg
        self.reg = cei.attrib.get("reg", None)
        if self.reg is not None:
            self.reg = normalize_string(self.reg)

        # key
        self.key = cei.attrib.get("key", None)
        if self.key is not None:
            self.key = normalize_string(self.key)

        # wikidata_iri
        self.wikidata_iri = None
        if self.key is not None:
            if self.key.startswith("wikidata:Q"):
                self.wikidata_iri = (
                    f"http://www.wikidata.org/entity/{self.key.rsplit(":", 1)[-1]}"
                )
            elif self.key.startswith("P_wikidata_"):
                self.wikidata_iri = (
                    f"http://www.wikidata.org/entity/{self.key.rsplit("_", 1)[-1]}"
                )

    def set_person_id(self, person_id: int):
        self.person_id = person_id
