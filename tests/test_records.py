import pytest

from utils.custom_types import RecordsContact
from utils.records import normalize_district, resolve_school_contact


class TestNormalizeDistrict:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            (None, ""),
            ("", ""),
            ("Charleston County School District", "charleston"),
            ("Berkeley County", "berkeley"),
            ("  Dorchester   School District  ", "dorchester"),
        ],
    )
    def test_normalize_district(self, name, expected):
        assert normalize_district(name) == expected


class TestResolveSchoolContact:
    @pytest.mark.parametrize(
        ("query", "expected_name"),
        [
            ("Charleston", "charleston"),
            ("ccsd", "charleston"),
            ("berkeley", None),
        ],
    )
    def test_resolve_school_contact(self, query, expected_name):
        contacts = {
            "charleston": RecordsContact(email="a@example.com", aliases=["CCSD"]),
        }
        name, contact = resolve_school_contact(query, contacts)
        assert name == expected_name
        assert contact is contacts.get(expected_name)
