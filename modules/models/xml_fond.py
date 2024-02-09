import validators
from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator
from modules.models.xml_archive import XmlArchive
from modules.utils import normalize_string


class XmlFond:
    archive_file: str
    archive_id: int
    atom_id: str
    file: str
    free_image_access: bool = False
    id: int
    identifier: str
    image_base: None | str = None
    title: str

    def __init__(
        self,
        file: str,
        archive: XmlArchive,
        ead: etree._ElementTree,
        prefs: None | etree._ElementTree,
    ):
        # id
        self.id = SerialIDGenerator().get_serial_id(self.__class__.__name__)

        # file
        self.file = file

        # atom_id
        atom_id = ead.find("./atom:id", NAMESPACES)
        assert atom_id is not None
        assert atom_id.text is not None
        self.atom_id = atom_id.text

        # archive_id
        self.archive_id = archive.id

        # archive_file
        self.archive_file = archive.file

        # identifier
        unitid = ead.find(".//ead:unitid", NAMESPACES)
        assert unitid is not None
        if unitid.text is None:
            identifier = unitid.attrib.get("identifier")
        else:
            identifier = unitid.text
        assert identifier is not None
        self.identifier = identifier

        # title
        unititle = ead.find(".//ead:unittitle", NAMESPACES)
        assert unititle is not None
        if unititle.text is None:
            self.title = self.identifier
        else:
            self.title = unititle.text
        self.title = normalize_string(self.title)

        if prefs is not None:
            # free_image_access
            free_image_access = prefs.find(
                "./xrx:param[@name='image-access']", NAMESPACES
            )
            self.free_image_access = (
                False
                if (free_image_access is None or free_image_access.text is None)
                else free_image_access.text == "free"
            )

            # image_base
            image_base = prefs.find(
                "./xrx:param[@name='image-server-base-url']", NAMESPACES
            )
            self.image_base = (
                None
                if (image_base is None or image_base.text is None)
                or not validators.url(image_base.text)
                else image_base.text
            )
