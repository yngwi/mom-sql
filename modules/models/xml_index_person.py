from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator


class Name:
    def __init__(self, text: str, ref: None | str = None):
        self.text = text
        self.ref = ref


class XmlIndexPerson:
    def __init__(self, tei: etree._Element, index_identifier: str):
        # id
        self.id = SerialIDGenerator().get_serial_id(XmlIndexPerson)

        self.index_identifier = index_identifier

        # xml_id
        xml_id = tei.attrib.get("{http://www.w3.org/XML/1998/namespace}id", None)
        if xml_id is None:
            raise ValueError(f"IndexPerson has no xml:id: {etree.tostring(tei)}")
        self.xml_id: str = xml_id

        # mom_url
        self.mom_iri = (
            f"http://www.monasterium.net/mom/index/{index_identifier}/{self.xml_id}"
        )

        # wikidata_iri
        self.wikidata_iri = tei.findtext(
            ".//momtei:idno[@type='URI']", None, NAMESPACES
        )
        if self.wikidata_iri is not None:
            self.wikidata_iri = self.wikidata_iri.replace(
                "http://wikidata.org/", "http://www.wikidata.org/"
            )

        # names
        self.names: list[Name] = []
        for name in tei.findall(".//momtei:persName", NAMESPACES):
            text = name.text
            ref = name.attrib.get("ref", None)
            if text is not None:
                self.names.append(Name(text, ref))
