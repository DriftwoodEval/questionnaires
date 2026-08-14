import pytest

from qsend import get_questionnaires

RULES = [
    {
        "daeval": "DA",
        "diagnosis": "ASD",
        "minAge": 2,
        "maxAge": 17,
        "questionnaires": ["ASD Questionnaire"],
    },
    {
        "daeval": "DA",
        "diagnosis": "ADHD",
        "minAge": 5,
        "maxAge": 17,
        "questionnaires": ["ADHD Questionnaire"],
    },
]


class TestGetQuestionnaires:
    @pytest.mark.parametrize(
        ("age", "check", "expected"),
        [
            (1, "ASD", "Too young"),
            (18, "ASD", "Too old"),
            (10, "ASD", ["ASD Questionnaire"]),
            (4, "ADHD", "Too young"),
            (18, "ADHD", "Too old"),
        ],
    )
    def test_age_out_of_range(self, age, check, expected):
        assert get_questionnaires(age, check, "DA", RULES) == expected

    def test_no_rule_configured_for_diagnosis(self):
        assert get_questionnaires(10, "ASD", "EVAL", RULES) == "Unknown"
