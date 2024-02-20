from lxml import etree

from modules.models.xml_collection import XmlCollection
from modules.models.xml_user import XmlUser


class XmlMycollection(XmlCollection):
    def __init__(
        self,
        file: str,
        cei: etree._ElementTree,
        user: None | XmlUser,
        public: bool = False,
    ):
        super().__init__(file, cei, [], None if public else XmlMycollection)
        if user is not None:
            self.set_user(user)

        self.private_mycollection_id: None | int = None

    def set_user(self, user: XmlUser):
        self.owner_id = user.id
        self.owner_email = user.email
