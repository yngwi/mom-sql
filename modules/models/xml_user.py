from typing import Dict, List

from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator
from modules.utils import normalize_string, parse_date


class Bookmark:
    def __init__(self, atom_id: str, note: None | str = None):
        self.atom_id = atom_id
        self.note = note


class SavedCharter:
    def __init__(self, saved: etree._Element):
        # atom_id
        self.atom_id = saved.findtext("./xrx:id", "", NAMESPACES)
        if self.atom_id == "":
            raise Exception("Cannot find atom_id for saved charter")

        # start_date
        start_time = saved.findtext("./xrx:start_time", "", NAMESPACES)
        if start_time == "":
            raise Exception("Cannot find start_time for saved charter")
        self.start_time = parse_date(start_time)

        # released
        released = saved.findtext("./xrx:freigabe", "", NAMESPACES)
        self.released = released == "yes"


class XmlUser:
    def __init__(
        self,
        file: str,
        xrx: etree._ElementTree,
        bookmark_notes: List[etree._ElementTree],
    ):
        # id
        self.id = SerialIDGenerator().get_serial_id(XmlUser)

        # file
        self.file = file

        # email
        self.email = normalize_string(xrx.findtext("./xrx:email", "", NAMESPACES))
        assert self.email != ""

        # first_name
        self.first_name = None
        first_name = normalize_string(xrx.findtext("./xrx:firstname", "", NAMESPACES))
        if first_name != "":
            self.first_name = first_name

        # name
        self.name = None
        name = normalize_string(xrx.findtext("./xrx:name", "", NAMESPACES))
        if name != "":
            self.name = name

        # moderater_email
        self.moderater_email = None
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

        # bookmarks
        notes: Dict[str, str] = {}
        for note in bookmark_notes:
            note_id = note.findtext("./xrx:bookmark", "", NAMESPACES)
            note_text = note.findtext("./xrx:note", "", NAMESPACES)
            if note_id == "" or note_text == "":
                continue
            escaped_note_id = note_id.replace(
                "tag%3Awww.monasterium.net%2C2011%3A", "tag:www.monasterium.net,2011:"
            )
            notes[escaped_note_id] = note_text
        self.bookmarks = [
            Bookmark(bookmark.text, notes.get(bookmark.text, None))
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
