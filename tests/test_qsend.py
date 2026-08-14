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
    {
        "daeval": "DAEVAL",
        "diagnosis": "ASD",
        "minAge": 18,
        "maxAge": 99,
        "questionnaires": ["Adult ASD Questionnaire"],
    },
    {
        "daeval": "DAEVAL",
        "diagnosis": "LD",
        "minAge": 6,
        "maxAge": 99,
        "questionnaires": ["LD Questionnaire"],
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

    def test_daeval_asd_uses_asd_diagnosis_rule(self):
        # DAEVAL rules are keyed on "ASD"/"LD" diagnoses, not None: a 53 year
        # old ASD client should match the DAEVAL/ASD rule, not fall through
        # to "Unknown" because of a None diagnosis lookup.
        assert get_questionnaires(53, "ASD", "DAEVAL", RULES) == [
            "Adult ASD Questionnaire"
        ]

    def test_daeval_asd_adhd_ignores_adhd_diagnosis(self):
        # ADHD isn't a valid DAEVAL diagnosis, so only the ASD rule applies.
        assert get_questionnaires(53, "ASD+ADHD", "DAEVAL", RULES) == [
            "Adult ASD Questionnaire"
        ]

    def test_daeval_asd_ld_unions_both_rules(self):
        assert get_questionnaires(53, "ASD+LD", "DAEVAL", RULES) == [
            "Adult ASD Questionnaire",
            "LD Questionnaire",
        ]

    def test_daeval_adhd_ld_uses_ld_diagnosis_rule(self):
        assert get_questionnaires(53, "ADHD+LD", "DAEVAL", RULES) == [
            "LD Questionnaire"
        ]

    def test_daeval_pure_adhd_has_no_valid_diagnosis(self):
        assert get_questionnaires(53, "ADHD", "DAEVAL", RULES) == "Unknown"
