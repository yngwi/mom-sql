from typing import Dict, List

from modules.models.serial_id_generator import SerialIDGenerator
from modules.models.xml_index_person import XmlIndexPerson
from modules.models.xml_person_name import XmlPersonName


class Person:
    def __init__(
        self,
        names: List[str],
        wikidata_iri: None | str,
        mom_id: None | str,
        mom_iri: None | str,
    ):
        self.id = SerialIDGenerator().get_serial_id(Person)
        self.mom_id = mom_id
        self.mom_iri = mom_iri
        self.names = names
        self.wikidata_iri = wikidata_iri


class PersonIndex:
    _mom_person_ids: Dict[str, int] = {}
    _persons: Dict[int, Person] = {}
    _wikidata_person_ids: Dict[str, int] = {}

    def _add_person(
        self,
        names: List[str],
        wikidata_iri: None | str,
        mom_id: None | str,
        mom_iri: None | str,
    ) -> int:
        person = self.find_person_by_ids(names, wikidata_iri, mom_id)
        if person is None:
            person = Person(names, wikidata_iri, mom_id, mom_iri)
            self._persons[person.id] = person
            if wikidata_iri is not None:
                self._wikidata_person_ids[wikidata_iri] = person.id
            if mom_id is not None:
                self._mom_person_ids[mom_id] = person.id
        return person.id

    def find_person_by_ids(
        self, names: str | List[str], wikidata_iri: None | str, mom_id: None | str
    ) -> None | Person:
        names = names if isinstance(names, str) else "; ".join(names)
        wikidata_person_id = None
        if wikidata_iri is not None:
            wikidata_person_id = self._wikidata_person_ids.get(wikidata_iri, None)
        mom_person_id = None
        if mom_id is not None:
            mom_person_id = self._mom_person_ids.get(mom_id, None)
        if (
            wikidata_person_id is not None
            and mom_person_id is not None
            and wikidata_person_id != mom_person_id
        ):
            raise ValueError(
                f"Person {names} exists multiple times in the index for wikidata/mom: {wikidata_iri}/{mom_id}"
            )
        person_id = (
            wikidata_person_id if wikidata_person_id is not None else mom_person_id
        )
        if person_id is not None:
            return self._persons[person_id]
        else:
            return None

    def add_xml_index_person(self, person: XmlIndexPerson) -> int:
        names = [name.text for name in person.names]
        wikidata_iri = person.wikidata_iri
        mom_id = person.xml_id
        mom_iri = person.mom_iri
        return self._add_person(names, wikidata_iri, mom_id, mom_iri)

    def add_all_xml_index_persons(self, persons: List[XmlIndexPerson]) -> List[int]:
        return [self.add_xml_index_person(person) for person in persons]

    def find_for_name(self, name: XmlPersonName) -> None | Person:
        return self.find_person_by_ids(name.text, name.wikidata_iri, name.key)

    def list_persons(self) -> List[Person]:
        return list(self._persons.values())

    def count_persons(self) -> int:
        return len(self._persons)
