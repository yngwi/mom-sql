import validators
from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator
from modules.models.xml_archive import XmlArchive
from modules.utils import normalize_string


class XmlFond:
    def __init__(
        self,
        file: str,
        archive: XmlArchive,
        ead: etree._ElementTree,
        prefs: None | etree._ElementTree,
    ):
        # id
        self.id = SerialIDGenerator().get_serial_id(XmlFond)

        # file
        self.file = file

        # atom_id
        self.atom_id = ead.findtext("./atom:id", "", NAMESPACES)
        assert self.atom_id != ""

        # archive_id
        self.archive_id = archive.id

        # archive_file
        self.archive_file = archive.file

        # identifier
        unitid = ead.find(".//ead:unitid", NAMESPACES)
        assert unitid is not None
        if unitid.text is None:
            self.identifier = str(unitid.attrib.get("identifier", ""))
        else:
            self.identifier = str(unitid.text)
        assert self.identifier != ""

        # title
        unititle = ead.find(".//ead:unittitle", NAMESPACES)
        assert unititle is not None
        if unititle.text is None:
            title = self.identifier
        else:
            title = unititle.text
        self.title = normalize_string(title)

        self.oai_shared = False
        if archive.oai is not None:
            for fond in archive.oai.fonds:
                if fond == self.identifier:
                    self.oai_shared = True

        self.free_image_access = False
        self.image_base = None
        if prefs is not None:
            # free_image_access
            free_image_access = prefs.findtext(
                "./xrx:param[@name='image-access']", "", NAMESPACES
            )
            self.free_image_access = (
                False if (free_image_access == "") else free_image_access == "free"
            )

            # image_base
            image_base = prefs.findtext(
                "./xrx:param[@name='image-server-base-url']", "", NAMESPACES
            )
            self.image_base = (
                None
                if image_base == "" or not validators.url(image_base)
                else image_base
            )
