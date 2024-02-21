from enum import Enum

NAMESPACES = {
    "app": "http://www.w3.org/2007/app",
    "atom": "http://www.w3.org/2005/Atom",
    "cei": "http://www.monasterium.net/NS/cei",
    "ead": "urn:isbn:1-931666-22-9",
    "eag": "http://www.archivgut-online.de/eag",
    "exist": "http://exist.sourceforge.net/NS/exist",
    "momtei": "http://www.tei-c.org/ns/1.0/",
    "oei": "http://www.monasterium.net/NS/oei",
    "tei": "http://www.tei-c.org/ns/1.0",
    "xrx": "http://www.monasterium.net/NS/xrx",
}


class IndexLocation(Enum):
    ABSTRACT = 1
    BACK = 2
    TENOR = 3
