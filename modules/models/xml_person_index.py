from typing import List

from lxml import etree

from modules.constants import NAMESPACES
from modules.models.xml_index_person import XmlIndexPerson


class XmlPersonIndex:
    def __init__(self, tei: etree._ElementTree):
        # atom_id
        atom_id: str = tei.findtext("./atom:id", "", NAMESPACES)
        assert atom_id != ""
        self.atom_id = atom_id

        # identifier
        identifier: str = self.atom_id.split("/")[-1]
        assert identifier is not None
        self.identifier = identifier

        self.persons: List[XmlIndexPerson] = []
        for person_tei in tei.xpath(".//momtei:person", namespaces=NAMESPACES):
            self.persons.append(XmlIndexPerson(person_tei, self.identifier))
