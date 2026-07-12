from datetime import date

import pytest

from utils.custom_types import (
    ClientFromDB,
    PieceworkConfig,
    PieceworkCosts,
    RecordsContact,
    validate_questionnaires,
)


def make_piecework_config(**cost_overrides: PieceworkCosts) -> PieceworkConfig:
    costs = {
        "default": PieceworkCosts(
            DA=10.0, ADHDDA=15.0, EVAL=20.0, DAEVAL=25.0, REPORT=5.0
        ),
    }
    costs.update(cost_overrides)
    return PieceworkConfig(
        costs=costs,
        name_map={"jd": "Jane Doe"},
        payroll_emails={"jd": "jane@example.com"},
    )


class TestGetUnitCost:
    @pytest.mark.parametrize(
        ("cost_overrides", "evaluator_name", "appointment_type", "expected"),
        [
            ({}, "unknown_evaluator", "DA", 10.0),
            ({"jd": PieceworkCosts(DA=99.0)}, "jd", "DA", 99.0),
            ({"jd": PieceworkCosts(DA=None)}, "jd", "DA", 10.0),
            ({}, "jd", "NOT_A_TYPE", 0.00),
        ],
    )
    def test_get_unit_cost(
        self, cost_overrides, evaluator_name, appointment_type, expected
    ):
        config = make_piecework_config(**cost_overrides)
        assert config.get_unit_cost(evaluator_name, appointment_type) == expected


class TestGetFullName:
    def test_case_insensitive_lookup(self):
        config = make_piecework_config()
        assert config.get_full_name("JD") == "Jane Doe"
        assert config.get_full_name("jd") == "Jane Doe"

    def test_unknown_initials_returns_empty_string(self):
        config = make_piecework_config()
        assert config.get_full_name("zz") == ""


def make_client_from_db(client_id: int, questionnaires=None) -> ClientFromDB:
    return ClientFromDB.model_validate(
        {
            "id": client_id,
            "dob": date(2015, 1, 1),
            "firstName": "Test",
            "lastName": "Client",
            "fullName": "Test Client",
            "status": True,
            "autismStop": False,
            "pause": False,
            "babyNetERNeeded": False,
            "babyNetERDownloaded": False,
            "language": "English",
            "questionnaires": questionnaires,
        }
    )


class TestValidateQuestionnaires:
    def test_client_with_questionnaires_is_kept(self, questionnaire_factory):
        client = make_client_from_db(1, questionnaires=[questionnaire_factory()])
        validated = validate_questionnaires({1: client})
        assert 1 in validated
        assert validated[1].id == 1

    def test_client_with_no_questionnaires_is_skipped(self):
        client = make_client_from_db(2, questionnaires=None)
        validated = validate_questionnaires({2: client})
        assert validated == {}

    def test_client_with_empty_questionnaires_list_is_skipped(self):
        client = make_client_from_db(3, questionnaires=[])
        validated = validate_questionnaires({3: client})
        assert validated == {}

    def test_mixed_clients_only_valid_ones_kept(self, questionnaire_factory):
        good = make_client_from_db(1, questionnaires=[questionnaire_factory()])
        bad = make_client_from_db(2, questionnaires=None)
        validated = validate_questionnaires({1: good, 2: bad})
        assert set(validated.keys()) == {1}


class TestRecordsContactEmailValidation:
    @pytest.mark.parametrize(
        "email",
        [
            "a@example.com",
            "a@example.com, b@example.com",
        ],
    )
    def test_valid_emails_are_kept_as_is(self, email):
        assert RecordsContact(email=email, aliases=[]).email == email

    @pytest.mark.parametrize(
        "email",
        [
            "not-an-email",
            "a@example.com, not-an-email",
        ],
    )
    def test_invalid_emails_raise(self, email):
        with pytest.raises(ValueError):
            RecordsContact(email=email, aliases=[])
