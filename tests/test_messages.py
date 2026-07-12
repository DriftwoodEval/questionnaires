from datetime import date

import pytest

from utils.messages import build_q_message, format_ta_message


class TestFormatTaMessage:
    def test_formats_links_with_index(self):
        questionnaires = [
            {"type": "ASRS", "link": "https://a.example.com"},
            {"type": "BASC", "link": "https://b.example.com"},
        ]
        message = format_ta_message(questionnaires)
        assert message == ("1) https://a.example.com\n2) https://b.example.com\n")

    def test_marks_self_report_questionnaires(self):
        questionnaires = [{"type": "ASRS Self", "link": "https://a.example.com"}]
        message = format_ta_message(questionnaires)
        assert message == "1) https://a.example.com - For client being tested\n"

    def test_empty_list_is_empty_string(self):
        assert format_ta_message([]) == ""


class TestBuildQMessage:
    def test_no_sent_date_returns_none(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory()
        client = client_factory()
        q = questionnaire_factory(sent=None)
        assert build_q_message(config, client, q, 0) is None

    def test_first_reminder_mentions_today(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory(name="Jane")
        q = questionnaire_factory(sent=date.today(), reminded=0)
        client = client_factory(questionnaires=[q])
        message = build_q_message(config, client, q, 0)
        assert message is not None
        assert "Jane" in message
        assert "complete your questionnaire" in message

    @pytest.mark.parametrize(
        ("reminded", "distance", "expected_substring"),
        [
            (1, -1, "(yesterday)"),
            (1, 5, "5 days ago"),
            (2, 5, "close out your referral"),
        ],
    )
    def test_reminder_wording_by_count_and_distance(
        self,
        config_factory,
        client_factory,
        questionnaire_factory,
        reminded,
        distance,
        expected_substring,
    ):
        config = config_factory()
        q = questionnaire_factory(sent=date(2024, 1, 1), reminded=reminded)
        client = client_factory(questionnaires=[q])
        message = build_q_message(config, client, q, distance)
        assert message is not None
        assert expected_substring in message

    def test_unknown_reminded_count_returns_none(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory()
        q = questionnaire_factory(sent=date(2024, 1, 1), reminded=99)
        client = client_factory(questionnaires=[q])
        assert build_q_message(config, client, q, 5) is None

    def test_multiple_pending_questionnaires_use_plural(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory()
        q1 = questionnaire_factory(sent=date.today(), reminded=0, q_type="ASRS")
        q2 = questionnaire_factory(sent=date.today(), reminded=0, q_type="BASC")
        client = client_factory(questionnaires=[q1, q2])
        message = build_q_message(config, client, q1, 0)
        assert message is not None
        assert "complete your questionnaires" in message

    def test_postda_pending_alone_does_not_change_wording(
        self, config_factory, client_factory, questionnaire_factory
    ):
        """is_postda only changes wording when is_posteval is also true."""
        config = config_factory()
        q = questionnaire_factory(
            sent=date.today(), reminded=0, status="POSTDA_PENDING"
        )
        client = client_factory(questionnaires=[q])
        message = build_q_message(config, client, q, 0)
        assert message is not None
        assert "finalize our review" not in message
        assert "complete your questionnaire" in message

    def test_postda_and_posteval_pending_changes_wording(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory()
        q1 = questionnaire_factory(
            sent=date.today(), reminded=0, status="POSTDA_PENDING", q_type="ASRS"
        )
        q2 = questionnaire_factory(
            sent=date.today(), reminded=0, status="POSTEVAL_PENDING", q_type="BASC"
        )
        client = client_factory(questionnaires=[q1, q2])
        message = build_q_message(config, client, q1, 0)
        assert message is not None
        assert "finalize our review" in message

    def test_posteval_pending_without_postda_changes_wording(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory()
        q = questionnaire_factory(
            sent=date.today(), reminded=0, status="POSTEVAL_PENDING"
        )
        client = client_factory(questionnaires=[q])
        message = build_q_message(config, client, q, 0)
        assert message is not None
        assert "comprehensive report" in message
