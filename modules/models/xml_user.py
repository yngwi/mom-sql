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
        email = normalize_string(xrx.findtext("./xrx:email", "", NAMESPACES))
        assert email != ""
        self.email = normalize_string(email)

        # first_name
        first_name = normalize_string(xrx.findtext("./xrx:firstname", "", NAMESPACES))
        if first_name != "":
            self.first_name = first_name

        # name
        name = normalize_string(xrx.findtext("./xrx:name", "", NAMESPACES))
        if name != "":
            self.name = name

        # moderater_email
        moderater_mom_id = normalize_string(
            xrx.findtext("./xrx:moderator", "", NAMESPACES)
        )
        if moderater_mom_id != "":
            self.moderater_email = (
                moderater_mom_id
                # replace wrong moderator addresses from Georg Vogeler
                .replace(
                    "g.vogeler@lrz.uni-muenchen.at", "g.vogeler@lrz.uni-muenchen.de"
                ).replace("g.vogeler@lrz.uni-graz.at", "g.vogeler@lrz.uni-muenchen.de")
            )

        # bookmark_atom_ids
        self.bookmark_atom_ids = [
            bookmark.text
            for bookmark in xrx.findall(".//xrx:bookmark", NAMESPACES)
            if bookmark.text
        ]
