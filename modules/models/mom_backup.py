import zipfile
from typing import List

from lxml import etree

from modules.models.contents_xml import ContentsXml
from modules.models.xml_archive import XmlArchive
from modules.models.xml_collection import XmlCollection
from modules.models.xml_collection_charter import XmlCollectionCharter
from modules.models.xml_fond import XmlFond
from modules.models.xml_fond_charter import XmlFondCharter


def correct_filename(filename: str) -> str:
    return filename.replace("&amp;", "&")


class MomBackup:
    path: str = ""
    zip: zipfile.ZipFile | None = None

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self.zip = zipfile.ZipFile(self.path, "r")
        return self

    def __exit__(self, __exc_type__, __exc_val__, __exc_tb__):
        if self.zip:
            self.zip.close()
            self.zip = None

    def list_archives(self) -> List[XmlArchive]:
        if not self.zip:
            return []
        archives: List[XmlArchive] = []
        contents_path = "db/mom-data/metadata.archive.public/__contents__.xml"
        with self.zip.open(contents_path) as archives_contents:
            archive_collections = ContentsXml(
                etree.parse(archives_contents)
            ).collections
            for archive_entry in archive_collections:
                file = correct_filename(archive_entry.file)
                eag_path = f"db/mom-data/metadata.archive.public/{file}/{file}.eag.xml"
                eag: None | etree._ElementTree = None
                try:
                    with self.zip.open(eag_path) as eag_contents:
                        eag = etree.parse(eag_contents)
                except KeyError:
                    continue
                oai_path = f"db/mom-data/metadata.archive.public/{file}/oai.xml"
                oai: None | etree._ElementTree = None
                try:
                    with self.zip.open(oai_path) as oai_contents:
                        oai = etree.parse(oai_contents)
                except KeyError:
                    pass
                assert eag is not None
                archives.append(XmlArchive(file, eag, oai))
        return archives

    def list_collections(self, fonds: List[XmlFond]) -> List[XmlCollection]:
        if not self.zip:
            return []
        collections: List[XmlCollection] = []
        contents_path = "db/mom-data/metadata.collection.public/__contents__.xml"
        with self.zip.open(contents_path) as collections_contents:
            collection_collections = ContentsXml(
                etree.parse(collections_contents)
            ).collections
            for collection_entry in collection_collections:
                file = correct_filename(collection_entry.file)
                cei_path = (
                    f"db/mom-data/metadata.collection.public/{file}/{file}.cei.xml"
                )
                cei: None | etree._ElementTree = None
                try:
                    with self.zip.open(cei_path) as eag_contents:
                        cei = etree.parse(eag_contents)
                except KeyError:
                    continue
                assert cei is not None
                collections.append(XmlCollection(file, cei, fonds))
        return collections

    def list_fonds(self, archives: List[XmlArchive]) -> List[XmlFond]:
        if not self.zip:
            return []
        fonds: List[XmlFond] = []
        for archive in archives:
            contents_path = (
                f"db/mom-data/metadata.fond.public/{archive.file}/__contents__.xml"
            )
            with self.zip.open(contents_path) as archive_contents:
                fond_collections = ContentsXml(
                    etree.parse(archive_contents)
                ).collections
                for fond_entry in fond_collections:
                    fond_file = correct_filename(fond_entry.file)
                    ead_file = f"db/mom-data/metadata.fond.public/{archive.file}/{fond_file}/{fond_file}.ead.xml"
                    preferences_file = f"db/mom-data/metadata.fond.public/{archive.file}/{fond_file}/{fond_file}.preferences.xml"
                    ead: None | etree._ElementTree = None
                    preferences: None | etree._ElementTree = None
                    try:
                        with self.zip.open(ead_file) as ead_contents:
                            ead = etree.parse(ead_contents)
                    except KeyError:
                        print(f"Failed to open ead {ead_file}")
                        continue
                    try:
                        with self.zip.open(preferences_file) as preferences_contents:
                            preferences = etree.parse(preferences_contents)
                    except KeyError:
                        pass
                    assert ead is not None
                    fonds.append(XmlFond(fond_file, archive, ead, preferences))
        return fonds

    def list_fond_charters(self, fonds: List[XmlFond]) -> List[XmlFondCharter]:
        if not self.zip:
            return []
        charters: List[XmlFondCharter] = []
        for fond in fonds:
            contents_path = f"db/mom-data/metadata.charter.public/{fond.archive_file}/{fond.file}/__contents__.xml"
            try:
                with self.zip.open(contents_path) as fond_contents:
                    charter_resources = ContentsXml(
                        etree.parse(fond_contents)
                    ).resources
                    for charter_entry in charter_resources:
                        charter_file = correct_filename(charter_entry.file)
                        contents_path = f"db/mom-data/metadata.charter.public/{fond.archive_file}/{fond.file}/{charter_file}"
                        cei: None | etree._ElementTree = None
                        try:
                            with self.zip.open(contents_path) as cei_contents:
                                cei = etree.parse(cei_contents)
                        except KeyError:
                            print(f"Failed to open charter {contents_path}")
                        assert cei is not None
                        try:
                            charters.append(XmlFondCharter(charter_file, fond, cei))
                        except Exception as e:
                            print(f"Failed to create charter {contents_path}: {e}")
            except KeyError:
                print(f"No content for fond {fond.archive_file}; {fond.identifier}")
                pass

        return charters

    def list_collection_charters(
        self, collections: List[XmlCollection]
    ) -> List[XmlCollectionCharter]:
        if not self.zip:
            return []
        charters: List[XmlCollectionCharter] = []
        for collection in collections:
            contents_path = f"db/mom-data/metadata.charter.public/{collection.file}/__contents__.xml"
            try:
                with self.zip.open(contents_path) as fond_contents:
                    charter_resources = ContentsXml(
                        etree.parse(fond_contents)
                    ).resources
                    for charter_entry in charter_resources:
                        charter_file = correct_filename(charter_entry.file)
                        contents_path = f"db/mom-data/metadata.charter.public/{collection.file}/{charter_file}"
                        cei: None | etree._ElementTree = None
                        try:
                            with self.zip.open(contents_path) as cei_contents:
                                cei = etree.parse(cei_contents)
                        except KeyError:
                            print(f"Failed to open charter {contents_path}")
                        assert cei is not None
                        try:
                            charters.append(
                                XmlCollectionCharter(charter_file, collection, cei)
                            )
                        except Exception as e:
                            print(f"Failed to create charter {contents_path}: {e}")
            except KeyError:
                print(f"No content for fond {collection.file}; {collection.identifier}")
                pass

        return charters
