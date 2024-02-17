from typing import List, Type

import validators
from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator, T
from modules.models.xml_user import XmlUser
from modules.utils import join_url_parts, normalize_string


class XmlCharter:
    atom_id: str
    file: str
    last_editor_id: None | int = None
    last_editor_email: None | str = None
    id: int
    idno_id: None | str = None
    idno_text: None | str = None
    images: List[str]
    url: str

    def __init__(
        self,
        file: str,
        cei: etree._ElementTree,
        image_base: None | str,
        url: str,
        users: List[XmlUser] = [],
        override_id_gen_name: None | Type[T] = None,
    ):
        # id
        self.id = SerialIDGenerator().get_serial_id(
            XmlCharter if override_id_gen_name is None else override_id_gen_name
        )

        # file
        self.file = file

        # url
        self.url = url

        # images
        images = []
        for graphic in cei.findall(".//cei:graphic", NAMESPACES):
            url = graphic.attrib.get("url")
            if url:
                full_url = (
                    url
                    if url.startswith("http")
                    else join_url_parts(image_base, url)
                    if image_base is not None
                    else None
                )
                if full_url and validators.url(full_url):
                    images.append(full_url)
        self.images = images

        # atom_id
        atom_id = cei.findtext("./atom:id", "", NAMESPACES)
        if atom_id == "":
            raise Exception(f"No atom_id found for charter {file}")
        self.atom_id = atom_id

        # idno
        idno = cei.find(".//cei:idno", NAMESPACES)
        if idno is not None:
            idno_id = idno.attrib.get("id")
            idno_text = idno.text
            if idno_text is None and idno_id is not None:
                self.idno_id = idno_id
                self.idno_text = idno_id
            elif idno_text is not None and idno_id is None:
                self.idno_id = idno_text
                self.idno_text = idno_text
            elif idno_text is not None and idno_id is not None:
                self.idno_id = idno_id
                self.idno_text = idno_text

        email = normalize_string(cei.findtext(".//atom:email", "", NAMESPACES))
        if email != "" and email != "guest" and email != "admin":
            # last_editor
            self.last_editor_id = next(
                (user.id for user in users if user.email.lower() == email.lower()),
                None,
            )
            # last_editor_email
            self.last_editor_email = email
