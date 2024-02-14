from typing import List

from lxml import etree

from modules.models.xml_charter import XmlCharter
from modules.models.xml_collection import XmlCollection
from modules.models.xml_user import XmlUser
from modules.utils import join_url_parts


class XmlCollectionCharter(XmlCharter):
    collection_file: str
    collection_id: int

    def __init__(
        self,
        file: str,
        collection: XmlCollection,
        cei: etree._ElementTree,
        users: List[XmlUser],
    ):
        # url
        url = join_url_parts(
            "https://www.monasterium.net/mom",
            collection.file,
            file.split(".cei.xml")[0],
            "charter",
        )

        # init base charter
        super().__init__(file, cei, collection.image_base, url, users)

        # collection_id
        self.collection_id = collection.id

        # collection_file
        self.collection_file = collection.file
