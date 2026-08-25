from datetime import date

import pytest
from dateutil.relativedelta import relativedelta

from utils.messages import (
    DD4_SCHOOL_DISTRICT,
    DEFAULT_REMINDER_TEMPLATES,
    build_referral_message,
    format_ta_message,
    is_potential_private_pay,
    render_reminder_message,
)

DEFAULT_SETTINGS = {
    "stage2OffsetDays": 14,
    "stage3OffsetDays": 7,
    "escalationSilenceDays": 3,
}


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


class TestIsPotentialPrivatePay:
    @pytest.mark.parametrize(
        ("primary_insurance", "secondary_insurance", "private_pay", "school_district"),
        [
            ("Blue Cross", None, False, None),
            (None, ["Molina"], False, None),
            (None, None, True, None),
            (None, None, False, DD4_SCHOOL_DISTRICT),
        ],
    )
    def test_excluded_when_matched_and_no_private_pay_indicator(
        self,
        referral_client_factory,
        primary_insurance,
        secondary_insurance,
        private_pay,
        school_district,
    ):
        client = referral_client_factory(
            primary_insurance=primary_insurance,
            secondary_insurance=secondary_insurance,
            private_pay=private_pay,
            school_district=school_district,
        )
        assert is_potential_private_pay(client, has_matched_evaluator=True) is False

    def test_no_insurance_not_private_pay_not_dd4_is_potential_private_pay(
        self, referral_client_factory
    ):
        client = referral_client_factory(
            primary_insurance=None,
            secondary_insurance=None,
            private_pay=False,
            school_district="Some Other District",
        )
        assert is_potential_private_pay(client, has_matched_evaluator=True) is True

    def test_no_matched_evaluator_is_potential_private_pay_even_with_insurance(
        self, referral_client_factory
    ):
        client = referral_client_factory(primary_insurance="Blue Cross")
        assert is_potential_private_pay(client, has_matched_evaluator=False) is True


class TestBuildReferralMessage:
    @pytest.mark.parametrize(
        ("primary_insurance", "secondary_insurance", "private_pay", "school_district"),
        [
            ("Blue Cross", None, False, None),
            (None, ["Molina"], False, None),
            (None, None, True, None),
            (None, None, False, DD4_SCHOOL_DISTRICT),
        ],
    )
    def test_matched_non_potential_private_pay_gets_generic_message(
        self,
        *,
        config_factory,
        referral_client_factory,
        primary_insurance,
        secondary_insurance,
        private_pay,
        school_district,
    ):
        config = config_factory()
        client = referral_client_factory(
            primary_insurance=primary_insurance,
            secondary_insurance=secondary_insurance,
            private_pay=private_pay,
            school_district=school_district,
        )
        message = build_referral_message(config, client, has_matched_evaluator=True)
        assert "We have received your referral" in message
        assert "insurance" not in message.lower()

    def test_no_insurance_on_file_gets_private_pay_outreach(
        self, config_factory, referral_client_factory
    ):
        config = config_factory(name="Melissa")
        client = referral_client_factory(
            primary_insurance=None,
            private_pay=False,
            dob=date.today() - relativedelta(years=10),
        )
        message = build_referral_message(config, client, has_matched_evaluator=True)
        assert "Melissa" in message
        assert "not showing insurance on file" in message
        assert "private pay options" in message
        assert "Babynet" not in message

    def test_no_matched_evaluator_gets_private_pay_outreach_despite_insurance(
        self, config_factory, referral_client_factory
    ):
        config = config_factory(name="Melissa")
        client = referral_client_factory(
            primary_insurance="Blue Cross",
            dob=date.today() - relativedelta(years=10),
        )
        message = build_referral_message(config, client, has_matched_evaluator=False)
        assert "private pay options" in message

    def test_under_three_gets_babynet_question(
        self, config_factory, referral_client_factory
    ):
        config = config_factory()
        client = referral_client_factory(
            primary_insurance=None,
            dob=date.today() - relativedelta(years=2),
        )
        message = build_referral_message(config, client, has_matched_evaluator=True)
        assert "Babynet or Early Intervention" in message

    def test_three_or_older_does_not_get_babynet_question(
        self, config_factory, referral_client_factory
    ):
        config = config_factory()
        client = referral_client_factory(
            primary_insurance=None,
            dob=date.today() - relativedelta(years=3),
        )
        message = build_referral_message(config, client, has_matched_evaluator=True)
        assert "Babynet" not in message


class TestRenderReminderMessage:
    def test_no_sent_date_returns_none(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory()
        client = client_factory()
        q = questionnaire_factory(sent=None)
        assert (
            render_reminder_message(
                DEFAULT_REMINDER_TEMPLATES,
                DEFAULT_SETTINGS,
                config,
                client,
                most_recent_q=q,
                distance=0,
            )
            is None
        )

    def test_first_reminder_mentions_today(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory(name="Jane")
        q = questionnaire_factory(sent=date.today(), reminded=0)
        client = client_factory(questionnaires=[q])
        message = render_reminder_message(
            DEFAULT_REMINDER_TEMPLATES,
            DEFAULT_SETTINGS,
            config,
            client,
            most_recent_q=q,
            distance=0,
        )
        assert message is not None
        assert "Jane" in message
        assert "complete your questionnaire" in message

    @pytest.mark.parametrize(
        ("reminded", "distance", "expected_substring"),
        [
            (1, 1, "(yesterday)"),
            (1, 5, "5 days ago"),
            (2, 5, "close out your referral"),
        ],
    )
    def test_reminder_wording_by_count_and_distance(
        self,
        *,
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
        message = render_reminder_message(
            DEFAULT_REMINDER_TEMPLATES,
            DEFAULT_SETTINGS,
            config,
            client,
            most_recent_q=q,
            distance=distance,
        )
        assert message is not None
        assert expected_substring in message

    def test_unknown_reminded_count_returns_none(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory()
        q = questionnaire_factory(sent=date(2024, 1, 1), reminded=99)
        client = client_factory(questionnaires=[q])
        assert (
            render_reminder_message(
                DEFAULT_REMINDER_TEMPLATES,
                DEFAULT_SETTINGS,
                config,
                client,
                most_recent_q=q,
                distance=5,
            )
            is None
        )

    def test_multiple_pending_questionnaires_use_plural(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory()
        q1 = questionnaire_factory(sent=date.today(), reminded=0, q_type="ASRS")
        q2 = questionnaire_factory(sent=date.today(), reminded=0, q_type="BASC")
        client = client_factory(questionnaires=[q1, q2])
        message = render_reminder_message(
            DEFAULT_REMINDER_TEMPLATES,
            DEFAULT_SETTINGS,
            config,
            client,
            most_recent_q=q1,
            distance=0,
        )
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
        message = render_reminder_message(
            DEFAULT_REMINDER_TEMPLATES,
            DEFAULT_SETTINGS,
            config,
            client,
            most_recent_q=q,
            distance=0,
        )
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
        message = render_reminder_message(
            DEFAULT_REMINDER_TEMPLATES,
            DEFAULT_SETTINGS,
            config,
            client,
            most_recent_q=q1,
            distance=0,
        )
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
        message = render_reminder_message(
            DEFAULT_REMINDER_TEMPLATES,
            DEFAULT_SETTINGS,
            config,
            client,
            most_recent_q=q,
            distance=0,
        )
        assert message is not None
        assert "comprehensive report" in message

    def test_client_first_name_placeholder(
        self, config_factory, client_factory, questionnaire_factory
    ):
        templates = {(0, "DEFAULT"): "Hi $CLIENT_FIRST_NAME, please complete it."}
        config = config_factory()
        q = questionnaire_factory(sent=date.today(), reminded=0)
        client = client_factory(questionnaires=[q])
        message = render_reminder_message(
            templates, DEFAULT_SETTINGS, config, client, most_recent_q=q, distance=0
        )
        assert message == "Hi Test, please complete it."

    def test_override_is_used_instead_of_default_template(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory()
        q = questionnaire_factory(sent=date.today(), reminded=0)
        client = client_factory(questionnaires=[q])
        message = render_reminder_message(
            DEFAULT_REMINDER_TEMPLATES,
            DEFAULT_SETTINGS,
            config,
            client,
            most_recent_q=q,
            distance=0,
            override="This is a one-off message for $CLIENT_FIRST_NAME.",
        )
        assert message == "This is a one-off message for Test."

    def test_completed_and_remaining_count_placeholders(
        self, config_factory, client_factory, questionnaire_factory
    ):
        templates = {
            (0, "DEFAULT"): "Completed: $COMPLETED_COUNT, remaining: $REMAINING_COUNT."
        }
        config = config_factory()
        q1 = questionnaire_factory(
            sent=date.today(), reminded=0, status="PENDING", q_type="ASRS"
        )
        q2 = questionnaire_factory(
            sent=date.today(), reminded=0, status="COMPLETED", q_type="BASC"
        )
        q3 = questionnaire_factory(
            sent=date.today(), reminded=0, status="COMPLETED", q_type="Vineland"
        )
        client = client_factory(questionnaires=[q1, q2, q3])
        message = render_reminder_message(
            templates, DEFAULT_SETTINGS, config, client, most_recent_q=q1, distance=0
        )
        assert message == "Completed: 2, remaining: 1."

    def test_falls_back_to_hardcoded_default_when_db_template_missing(
        self, config_factory, client_factory, questionnaire_factory
    ):
        """If the emr_questionnaire_reminder_template table is missing a row
        (e.g. the seed script was never run), still send the hardcoded
        default instead of silently sending nothing."""
        config = config_factory(name="Jane")
        q = questionnaire_factory(sent=date.today(), reminded=0)
        client = client_factory(questionnaires=[q])
        message = render_reminder_message(
            {},
            DEFAULT_SETTINGS,
            config,
            client,
            most_recent_q=q,
            distance=0,
        )
        assert message is not None
        assert "Jane" in message
        assert "complete your questionnaire" in message

    def test_unknown_stage_with_empty_templates_returns_none(
        self, config_factory, client_factory, questionnaire_factory
    ):
        config = config_factory()
        q = questionnaire_factory(sent=date.today(), reminded=99)
        client = client_factory(questionnaires=[q])
        assert (
            render_reminder_message(
                {}, DEFAULT_SETTINGS, config, client, most_recent_q=q, distance=0
            )
            is None
        )
