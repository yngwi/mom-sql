from typing import List

from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator
from modules.utils import normalize_string


class XmlUser:
    email: str
    file: str
    first_name: None | str = None
    id: int
    moderater_email: None | str = None
    name: None | str = None
    bookmark_atom_ids: List[str]

    def __init__(self, file: str, xrx: etree._ElementTree):
        # id
        self.id = SerialIDGenerator().get_serial_id(XmlUser)

        # file
        self.file = file

        # email
        email = xrx.find("./xrx:email", NAMESPACES)
        assert email is not None
        assert email.text is not None
        self.email = normalize_string(email.text)

        # first_name
        first_name = xrx.find("./xrx:firstname", NAMESPACES)
        if first_name is not None and first_name.text is not None:
            self.first_name = normalize_string(first_name.text)

        # name
        name = xrx.find("./xrx:name", NAMESPACES)
        if name is not None and name.text is not None:
            self.name = normalize_string(name.text)

        # moderater_email
        moderater_mom_id = xrx.find("./xrx:moderator", NAMESPACES)
        if moderater_mom_id is not None and moderater_mom_id.text is not None:
            self.moderater_email = (
                normalize_string(moderater_mom_id.text)
                # replace wrong moderator addresses from Georg Vogeler
                .replace(
                    "g.vogeler@lrz.uni-muenchen.at", "g.vogeler@lrz.uni-muenchen.de"
                )
                .replace("g.vogeler@lrz.uni-graz.at", "g.vogeler@lrz.uni-muenchen.de")
            )

        # bookmark_atom_ids
        self.bookmark_atom_ids = []
