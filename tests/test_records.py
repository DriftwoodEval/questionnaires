import pytest

from utils.custom_types import RecordsContact
from utils.records import (
    RecordsRequestError,
    classify_failure,
    normalize_district,
    resolve_school_contact,
)


class TestNormalizeDistrict:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            (None, ""),
            ("", ""),
            ("Charleston County School District", "charleston"),
            ("Berkeley County", "berkeley"),
            ("Berkley County", "berkeley"),
            ("Berkley County School District", "berkeley"),
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


class TestClassifyFailure:
    @pytest.mark.parametrize(
        ("exc", "expected_message", "expected_add_to_sheet"),
        [
            (Exception("portal not opened"), "portal not opened", False),
            (Exception("docs not signed"), "docs not signed", False),
            (
                RecordsRequestError(
                    "School district on consent form does not match client's "
                    "school district in DB, form is richland 2, DB is florence 1."
                ),
                "School district on consent form does not match client's "
                "school district in DB, form is richland 2, DB is florence 1.",
                True,
            ),
            (
                RecordsRequestError("No school found on consent to send"),
                "No school found on consent to send",
                True,
            ),
            (Exception("driver crashed"), "Unhandled error for Jane Doe", True),
        ],
    )
    def test_classify_failure(self, exc, expected_message, expected_add_to_sheet):
        message, add_to_sheet = classify_failure(exc, "Jane Doe")
        assert message == expected_message
        assert add_to_sheet is expected_add_to_sheet
