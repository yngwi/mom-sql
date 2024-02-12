from typing import List

from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator
from modules.models.xml_user import XmlUser
from modules.utils import normalize_string


class XmlCharter:
    atom_id: str
    file: str
    last_editor_id: None | int = None
    id: int
    idno_norm: str
    idno_text: str
    images: List[str]
    url: str

    def __init__(
        self,
        file: str,
        cei: etree._ElementTree,
        images: List[str],
        url: str,
        users: List[XmlUser],
    ):
        # id
        self.id = SerialIDGenerator().get_serial_id(XmlCharter)

        # file
        self.file = file

        # images
        self.images = images

        # url
        self.url = url

        # atom_id
        atom_id = cei.findtext("./atom:id", None, NAMESPACES)
        assert atom_id is not None
        self.atom_id = atom_id

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

        # last_editor
        email = normalize_string(cei.findtext(".//atom:email", "", NAMESPACES))
        if email != "" and email != "guest" and email != "admin":
            self.last_editor_id = next(
                (user.id for user in users if user.email.lower() == email.lower()), None
            )
