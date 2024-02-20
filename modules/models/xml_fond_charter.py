from typing import List

from lxml import etree

from modules.models.xml_charter import XmlCharter
from modules.models.xml_fond import XmlFond
from modules.models.xml_user import XmlUser
from modules.utils import join_url_parts


class XmlFondCharter(XmlCharter):
    def __init__(
        self,
        file: str,
        fond: XmlFond,
        cei: etree._ElementTree,
        users: List[XmlUser],
    ):
        # url
        url = join_url_parts(
            "https://www.monasterium.net/mom",
            fond.archive_file,
            fond.file,
            file.split(".cei.xml")[0],
            "charter",
        )

        # init base charter
        super().__init__(file, cei, fond.image_base, url, users)

        # archive_id
        self.archive_id = fond.archive_id

        # archive_file
        self.archive_file = fond.archive_file

        # fond_id
        self.fond_id = fond.id

        # fond_file
        self.fond_file = fond.file
