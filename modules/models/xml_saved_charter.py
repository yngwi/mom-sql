import re
from datetime import datetime
from typing import List

import validators
from lxml import etree

from modules.constants import NAMESPACES
from modules.models.xml_charter import XmlCharter
from modules.models.xml_collection import XmlCollection
from modules.models.xml_fond import XmlFond
from modules.models.xml_user import XmlUser
from modules.utils import join_url_parts


class XmlSavedCharter(XmlCharter):
    editor_id: int = -1
    start_time: datetime = datetime.now()
    released: bool = False

    def __init__(
        self,
        file: str,
        cei: etree._ElementTree,
        users: List[XmlUser],
        fonds: List[XmlFond],
        collections: List[XmlCollection],
    ):
        parts = file.rsplit(".xml")[0].split("#")
        if len(parts) < 4 or len(parts) > 5:
            raise Exception(
                f"WARNING: Cannot extract url from saved charter file {file}"
            )
        parts = parts[2:]

        # url
        charter_url = f"https://www.monasterium.net/mom/saved-charter?id=tag:www.monasterium.net,2011:/charter/{"/".join(parts)}"

        # images
        images = []
        # collection charter
        if len(parts) == 2:
            collection_file = parts[0]
            collection = next(
                (
                    collection
                    for collection in collections
                    if collection.file == collection_file
                ),
                None,
            )
            if collection is None:
                raise Exception(
                    f"WARNING: Cannot find collection {collection_file} for saved charter file {file}"
                )
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
        # fond charter
        elif len(parts) == 3:
            archive_file = parts[0]
            fond_file = parts[1]
            fond = next(
                (
                    fond
                    for fond in fonds
                    if fond.archive_file == archive_file and fond.file == fond_file
                ),
                None,
            )
            if fond is None:
                raise Exception(
                    f"WARNING: Cannot find fond {archive_file}/{fond_file} for saved charter file {file}"
                )
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

        # init base charter
        super().__init__(file, cei, images, charter_url, users, XmlSavedCharter)
