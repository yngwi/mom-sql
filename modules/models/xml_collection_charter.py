import validators
from lxml import etree

from modules.constants import NAMESPACES
from modules.models.xml_charter import XmlCharter
from modules.models.xml_collection import XmlCollection
from modules.utils import join_url_parts


class XmlCollectionCharter(XmlCharter):
    collection_file: str
    collection_id: int

    def __init__(
        self,
        file: str,
        collection: XmlCollection,
        cei: etree._ElementTree,
    ):
        # images
        images = []
        for graphic in cei.findall(".//cei:graphic", NAMESPACES):
            url = graphic.attrib.get("url")
            if url:
                full_url = (
                    url
                    if url.startswith("http")
                    else join_url_parts(collection.image_base, url)
                    if collection.image_base is not None
                    else None
                )
                if full_url and validators.url(full_url):
                    images.append(full_url)

        # url
        url = join_url_parts(
            "https://www.monasterium.net/mom",
            collection.file,
            file.split(".cei.xml")[0],
            "charter",
        )

        # init base charter
        super().__init__(file, cei, images, url)

        # collection_id
        self.collection_id = collection.id

        # collection_file
        self.collection_file = collection.file
