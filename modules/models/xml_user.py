from datetime import datetime
from typing import List

from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator
from modules.utils import normalize_string, parse_date


class SavedCharter:
    atom_id: str
    start_date: datetime
    released: bool = False

    def __init__(self, saved: etree._Element):
        # atom_id
        atom_id = saved.findtext("./xrx:id", "", NAMESPACES)
        if atom_id == "":
            raise Exception("Cannot find atom_id for saved charter")
        self.atom_id = atom_id

        # start_date
        start_time = saved.findtext("./xrx:start_time", "", NAMESPACES)
        if start_time == "":
            raise Exception("Cannot find start_time for saved charter")
        self.start_time = parse_date(start_time)

        # released
        released = saved.findtext("./xrx:freigabe", "", NAMESPACES)
        self.released = released == "yes"


class XmlUser:
    bookmark_atom_ids: List[str]
    email: str
    file: str
    first_name: None | str = None
    id: int
    moderater_email: None | str = None
    name: None | str = None
    saved_charters: List[SavedCharter]

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

        # saved_charters
        self.saved_charters = []
        for saved in xrx.findall(".//xrx:saved", NAMESPACES):
            try:
                self.saved_charters.append(SavedCharter(saved))
            except Exception as e:
                print(f"Cannot parse saved charter for user {self.email}: {e}")
