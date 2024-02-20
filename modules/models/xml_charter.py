import calendar
import re
from datetime import date
from typing import List, Set, Type

import validators
from lxml import etree

from modules.constants import NAMESPACES
from modules.models.serial_id_generator import SerialIDGenerator, T
from modules.models.xml_user import XmlUser
from modules.utils import join_url_parts, normalize_string

MOM_DATE_REGEX = re.compile(
    r"^(?P<year>-?[0129]?[0-9][0-9][0-9])(?P<month>[019][0-9])(?P<day>[01239][0-9])$"
)

MIN_YEAR = 100
MAX_YEAR = year = date.today().year


def _parse_date(value: str) -> List[date]:
    if value == "99999999" or value == "00000000":
        return []
    match = re.search(MOM_DATE_REGEX, value)
    if match is None:
        raise ValueError("Invalid mom date value provided: '{}'".format(value))
    year = match.group("year")
    if not isinstance(year, str):
        raise ValueError("Invalid year in mom date value: {}".format(year))
    if int(year) < MIN_YEAR or int(year) > MAX_YEAR:
        return []
    month = match.group("month")
    if not isinstance(month, str):
        raise ValueError("Invalid month in mom date value: {}".format(month))
    day = match.group("day")
    if not isinstance(day, str):
        raise ValueError("Invalid day in mom date value: {}".format(day))
    if month == "99" or month == "00":
        return [
            date(int(year), 1, 1),
            date(
                int(year),
                12,
                31,
            ),
        ]
    if day == "99" or day == "00":
        return [
            date(int(year), int(month), 1),
            date(
                int(year),
                int(month),
                calendar.monthrange(int(year), int(month))[1],
            ),
        ]
    return [date(int(year), int(month), int(day))]


def _extract_date_attrib(element: None | etree._Element, name: str) -> List[date]:
    if element is None:
        return []
    value = element.attrib.get(name, None)
    if value is None:
        return []
    return _parse_date(value)


def _extract_opt_text(element: None | etree._Element) -> None | str:
    if element is None:
        return None
    return str(element.text) if element.text is not None else None


class XmlCharter:
    def __init__(
        self,
        file: str,
        cei: etree._ElementTree,
        image_base: None | str,
        url: str,
        users: List[XmlUser] = [],
        override_id_gen_name: None | Type[T] = None,
    ):
        # id
        self.id = SerialIDGenerator().get_serial_id(
            XmlCharter if override_id_gen_name is None else override_id_gen_name
        )

        # file
        self.file = file

        # url
        self.url = url

        # images
        self.images = []
        for graphic in cei.findall(".//cei:graphic", NAMESPACES):
            url = graphic.attrib.get("url")
            if url:
                full_url = (
                    url
                    if url.startswith("http")
                    else join_url_parts(image_base, url)
                    if image_base is not None
                    else None
                )
                if full_url and validators.url(full_url):
                    self.images.append(full_url)

        # atom_id
        self.atom_id = cei.findtext("./atom:id", "", NAMESPACES)
        if self.atom_id == "":
            raise Exception(f"No atom_id found for charter {file}")

        # idno
        self.idno_id = None
        self.idno_text = None
        idno_ele = cei.find(".//cei:idno", NAMESPACES)
        if idno_ele is not None:
            idno_id = idno_ele.attrib.get("id", None)
            idno_text = idno_ele.text
            if idno_text is None and idno_id is not None:
                self.idno_id = str(idno_id)
                self.idno_text = str(idno_id)
            elif idno_text is not None and idno_id is None:
                self.idno_id = str(idno_text)
                self.idno_text = str(idno_text)
            elif idno_text is not None and idno_id is not None:
                self.idno_id = str(idno_id)
                self.idno_text = str(idno_text)

        # date and date text
        self.sort_date = date.today()
        self.issued_date = None
        self.issued_date_text = None
        self.issued_date_is_exact = True
        date_single_element = cei.find(
            ".//cei:text/cei:body/cei:chDesc/cei:issued/cei:date", NAMESPACES
        )
        date_range_element = cei.find(
            ".//cei:text/cei:body/cei:chDesc/cei:issued/cei:dateRange", NAMESPACES
        )
        if date_single_element is not None or date_range_element is not None:
            try:
                date_set: Set[date] = set()
                date_set = date_set.union(
                    _extract_date_attrib(date_single_element, "value"),
                    _extract_date_attrib(date_range_element, "from"),
                    _extract_date_attrib(date_range_element, "to"),
                )
                dates: List[date] = list(date_set)
                dates.sort()
                if len(dates) == 0:
                    pass
                elif len(dates) <= 4:
                    self.sort_date = dates[-1]
                    self.issued_date = (dates[0], dates[-1])
                elif len(dates) > 2:
                    print(f"Too many dates found for charter {self.atom_id}")
            except ValueError as e:
                print(f"Error parsing date for charter {self.atom_id}: {e}")
            if self.issued_date is not None:
                self.issued_date_is_exact = self.issued_date[0] == self.issued_date[1]
            single_text = _extract_opt_text(date_single_element)
            range_text = _extract_opt_text(date_range_element)
            if single_text is None and range_text is None:
                if self.issued_date is not None:
                    if type(self.issued_date) is tuple:
                        self.issued_date_text = f"{self.issued_date[0].isoformat()} - {self.issued_date[1].isoformat()}"
                    elif type(self.issued_date) is date:
                        self.issued_date_text = self.issued_date.isoformat()
                else:
                    self.issued_date_text = None
            elif single_text is not None and range_text is not None:
                if single_text == range_text:
                    self.issued_date_text = single_text
                else:
                    print(f"Conflicting date texts found for charter {self.atom_id}")
                    self.issued_date_text = single_text
            else:
                self.issued_date_text = (
                    single_text if single_text is not None else range_text
                )
            if self.issued_date_text == "9999" or self.issued_date_text == "ohne Datum":
                self.issued_date_text = None

        self.last_editor_id = None
        self.last_editor_email = None
        email = normalize_string(cei.findtext(".//atom:email", "", NAMESPACES))
        if email != "" and email != "guest" and email != "admin":
            # last_editor
            self.last_editor_id = next(
                (user.id for user in users if user.email.lower() == email.lower()),
                None,
            )
            # last_editor_email
            self.last_editor_email = email

        # abstract
        self.abstract: None | etree._Element = None
        abstract_ele = cei.find(
            "./atom:content/cei:text/cei:body/cei:chDesc/cei:abstract",
            NAMESPACES,
        )
        if (
            abstract_ele is not None
            and abstract_ele.text != "Noch kein Regest vorhanden."
        ):
            self.abstract = abstract_ele

        # tenor
        self.tenor: None | etree._Element = cei.find(
            "./atom:content/cei:text/cei:body/cei:tenor",
            NAMESPACES,
        )
