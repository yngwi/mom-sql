from typing import List

import validators
from lxml import etree

from modules.constants import NAMESPACES
from modules.models.xml_charter import XmlCharter
from modules.models.xml_fond import XmlFond
from modules.models.xml_user import XmlUser
from modules.utils import join_url_parts


class XmlFondCharter(XmlCharter):
    archive_file: str
    archive_id: int
    fond_file: str
    fond_id: int

    def __init__(
        self,
        file: str,
        fond: XmlFond,
        cei: etree._ElementTree,
        users: List[XmlUser],
    ):
        # images
        images = []
        for graphic in cei.findall(".//cei:graphic", NAMESPACES):
            url = graphic.attrib.get("url")
            if url:
                full_url = (
                    url
                    if url.startswith("http")
                    else join_url_parts(fond.image_base, url)
                    if fond.image_base is not None
                    else None
                )
                if full_url and validators.url(full_url):
                    images.append(full_url)

        # url
        url = join_url_parts(
            "https://www.monasterium.net/mom",
            fond.archive_file,
            fond.file,
            file.split(".cei.xml")[0],
            "charter",
        )

        # init base charter
        super().__init__(file, cei, images, url, users)

        # archive_id
        self.archive_id = fond.archive_id

        # archive_file
        self.archive_file = fond.archive_file

        # fond_id
        self.fond_id = fond.id

        # fond_file
        self.fond_file = fond.file
