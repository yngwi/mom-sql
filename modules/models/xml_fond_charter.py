from typing import List

import validators
from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator
from modules.models.xml_fond import XmlFond


def join_url_parts(*parts: str) -> str:
    # Ensure the first part ends with a slash if it's not the only part
    if len(parts) > 1 and not parts[0].endswith("/"):
        parts = (parts[0] + "/",) + parts[1:]

    # Remove any leading or trailing slashes from intermediate parts
    sanitized_parts = (
        [parts[0].rstrip("/")]
        + [part.strip("/") for part in parts[1:-1]]
        + [parts[-1].lstrip("/")]
    )

    # Join the parts with a single slash between them
    full_url = "/".join(sanitized_parts)
    return full_url


class XmlFondCharter:
    archive_file: str
    archive_id: int
    atom_id: str
    file: str
    fond_file: str
    fond_id: int
    id: int
    idno_norm: str
    idno_text: str
    images: List[str]
    url: str

    def __init__(
        self,
        file: str,
        fond: XmlFond,
        cei: etree._ElementTree,
    ):
        # id
        self.id = SerialIDGenerator().get_serial_id(self.__class__)

        # file
        self.file = file

        # atom_id
        atom_id = cei.findtext("./atom:id", None, NAMESPACES)
        assert atom_id is not None
        self.atom_id = atom_id

        # archive_id
        self.archive_id = fond.archive_id

        # archive_file
        self.archive_file = fond.archive_file

        # fond_id
        self.fond_id = fond.id

        # fond_file
        self.fond_file = fond.file

        # idno
        idno = cei.find(".//cei:idno", NAMESPACES)
        if idno is None:
            raise Exception(f"WARNING: No idno found for {self.atom_id}")
        idno_norm = idno.attrib.get("id")
        idno_text = idno.text
        if idno_text is None and idno_norm is not None:
            self.idno_norm = idno_norm
            self.idno_text = idno_norm
        elif idno_text is not None and idno_norm is None:
            self.idno_norm = idno_text
            self.idno_text = idno_text
        elif idno_text is not None and idno_norm is not None:
            self.idno_norm = idno_norm
            self.idno_text = idno_text
        else:
            raise Exception(f"WARNING: No idno parts found for {self.atom_id}")

        # images
        self.images = []
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
                    self.images.append(full_url)

        # url
        self.url = join_url_parts(
            "https://www.monasterium.net/mom",
            fond.archive_file,
            fond.file,
            self.idno_norm,
            "charter",
        )
