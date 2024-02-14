from enum import Enum
from typing import List

from lxml import etree

from modules.constants import NAMESPACES


def _correct_filename(filename: str) -> str:
    """Makes sure the filename in the XML matches the actual filename in the zip."""
    return filename.replace("&amp;", "&")


class ContentEntryType(Enum):
    COLLECTION = "collection"
    RESOURCE = "resource"


class ContentEntry:
    name: str
    file: str
    type: ContentEntryType

    def __init__(self, element: etree._Element, collection: bool = False):
        name = element.get("name")
        assert name is not None
        self.name = name
        file = element.get("filename")
        assert file is not None
        self.file = _correct_filename(file)
        self.type = (
            ContentEntryType.COLLECTION if collection else ContentEntryType.RESOURCE
        )

    def __str__(self):
        return f"{self.type.value}: name={self.name}; filename={self.file}"


class ContentEntryCollection(ContentEntry):
    def __init__(self, element: etree._Element):
        super().__init__(element, True)


class ContentEntryResource(ContentEntry):
    def __init__(self, element: etree._Element):
        super().__init__(element, False)


class ContentsXml:
    xml: etree._ElementTree
    collections: List[ContentEntryCollection] = []
    resources: List[ContentEntryResource] = []

    def __init__(self, xml: etree._ElementTree):
        self.xml = xml
        self.collections = [
            ContentEntryCollection(collection)
            for collection in xml.findall("/exist:subcollection", NAMESPACES)
        ]
        self.resources = [
            ContentEntryResource(resource)
            for resource in xml.findall(
                "/exist:resource[@type='XMLResource']", NAMESPACES
            )
        ]
