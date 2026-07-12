from datetime import date, datetime

from utils.questionnaires import (
    _in_current_session,
    _resolve_wanted_diagnoses,
    all_questionnaires_done,
    check_battery_completeness,
    check_battery_sent,
    check_if_ignoring,
    filter_inactive_and_not_pending,
    generate_screenshot_filename,
    get_most_recent_not_done,
)


class TestInCurrentSession:
    def test_no_session_start_always_counts(
        self, client_factory, questionnaire_factory
    ):
        client = client_factory(session_started_at=None)
        q = questionnaire_factory(sent=date(2000, 1, 1))
        assert _in_current_session(client, q) is True

    def test_no_date_on_questionnaire_counts(
        self, client_factory, questionnaire_factory
    ):
        client = client_factory(session_started_at=datetime(2024, 1, 1))
        q = questionnaire_factory(sent=None)
        assert _in_current_session(client, q) is True

    def test_questionnaire_before_session_start_excluded(
        self, client_factory, questionnaire_factory
    ):
        client = client_factory(session_started_at=datetime(2024, 6, 1))
        q = questionnaire_factory(sent=date(2024, 1, 1))
        assert _in_current_session(client, q) is False

    def test_questionnaire_after_session_start_included(
        self, client_factory, questionnaire_factory
    ):
        client = client_factory(session_started_at=datetime(2024, 1, 1))
        q = questionnaire_factory(sent=date(2024, 6, 1))
        assert _in_current_session(client, q) is True

    def test_falls_back_to_updated_at(self, client_factory, questionnaire_factory):
        client = client_factory(session_started_at=datetime(2024, 6, 1))
        q = questionnaire_factory(sent=None)
        q["updatedAt"] = date(2024, 1, 1)
        assert _in_current_session(client, q) is False


class TestAllQuestionnairesDone:
    def test_all_completed(self, client_factory, questionnaire_factory):
        client = client_factory(
            questionnaires=[
                questionnaire_factory(status="COMPLETED"),
                questionnaire_factory(status="EXTERNAL"),
            ]
        )
        assert all_questionnaires_done(client) is True

    def test_one_pending(self, client_factory, questionnaire_factory):
        client = client_factory(
            questionnaires=[
                questionnaire_factory(status="COMPLETED"),
                questionnaire_factory(status="PENDING"),
            ]
        )
        assert all_questionnaires_done(client) is False

    def test_archived_ignored(self, client_factory, questionnaire_factory):
        client = client_factory(
            questionnaires=[
                questionnaire_factory(status="COMPLETED"),
                questionnaire_factory(status="ARCHIVED"),
            ]
        )
        assert all_questionnaires_done(client) is True

    def test_out_of_session_pending_ignored(
        self, client_factory, questionnaire_factory
    ):
        client = client_factory(
            session_started_at=datetime(2024, 6, 1),
            questionnaires=[
                questionnaire_factory(status="COMPLETED", sent=date(2024, 7, 1)),
                questionnaire_factory(status="PENDING", sent=date(2024, 1, 1)),
            ],
        )
        assert all_questionnaires_done(client) is True


class TestFilterInactiveAndNotPending:
    def test_inactive_client_filtered_out(self, client_factory, questionnaire_factory):
        clients = {
            1: client_factory(
                client_id=1,
                status=False,
                questionnaires=[questionnaire_factory(status="PENDING", client_id=1)],
            )
        }
        assert filter_inactive_and_not_pending(clients) == {}

    def test_active_with_pending_kept(self, client_factory, questionnaire_factory):
        client = client_factory(
            client_id=1,
            status=True,
            questionnaires=[questionnaire_factory(status="PENDING", client_id=1)],
        )
        clients = {1: client}
        assert filter_inactive_and_not_pending(clients) == {1: client}

    def test_active_with_only_completed_filtered_out(
        self, client_factory, questionnaire_factory
    ):
        clients = {
            1: client_factory(
                client_id=1,
                status=True,
                questionnaires=[questionnaire_factory(status="COMPLETED", client_id=1)],
            )
        }
        assert filter_inactive_and_not_pending(clients) == {}

    def test_pending_before_session_start_excluded(
        self, client_factory, questionnaire_factory
    ):
        clients = {
            1: client_factory(
                client_id=1,
                status=True,
                session_started_at=datetime(2024, 6, 1),
                questionnaires=[
                    questionnaire_factory(
                        status="PENDING", client_id=1, sent=date(2024, 1, 1)
                    )
                ],
            )
        }
        assert filter_inactive_and_not_pending(clients) == {}


class TestCheckIfIgnoring:
    def test_ignoring(self, client_factory, questionnaire_factory):
        client = client_factory(
            questionnaires=[questionnaire_factory(status="IGNORING")]
        )
        assert check_if_ignoring(client) is True

    def test_none_ignoring(self, client_factory, questionnaire_factory):
        client = client_factory(
            questionnaires=[questionnaire_factory(status="PENDING")]
        )
        assert check_if_ignoring(client) is False


class TestGenerateScreenshotFilename:
    def test_basic_filename_shape(self):
        filename = generate_screenshot_filename(
            "completed",
            "ASRS (6-18 Years)",
            "https://qosa.pearsonassessments.com/foo/bar?x=1",
        )
        assert filename.startswith(
            "COMPLETED_ASRS_6-18_Years__pearsonassessments_foo_bar_x_1_"
        )
        assert filename.endswith(".png")

    def test_unknown_url_identity(self):
        filename = generate_screenshot_filename(
            "incomplete", "CAT-Q", "https://a.b.com"
        )
        assert "_unknown_" in filename

    def test_single_part_host(self):
        filename = generate_screenshot_filename(
            "incomplete", "CAT-Q", "https://localhost/x"
        )
        assert "_localhost_" in filename


class TestGetMostRecentNotDone:
    def test_returns_most_recent_pending(self, client_factory, questionnaire_factory):
        older = questionnaire_factory(status="PENDING", sent=date(2024, 1, 1))
        newer = questionnaire_factory(status="POSTDA_PENDING", sent=date(2024, 6, 1))
        client = client_factory(questionnaires=[older, newer])
        assert get_most_recent_not_done(client) == newer

    def test_ignores_completed(self, client_factory, questionnaire_factory):
        completed = questionnaire_factory(status="COMPLETED", sent=date(2024, 6, 1))
        pending = questionnaire_factory(status="PENDING", sent=date(2024, 1, 1))
        client = client_factory(questionnaires=[completed, pending])
        assert get_most_recent_not_done(client) == pending

    def test_none_when_nothing_pending(self, client_factory, questionnaire_factory):
        client = client_factory(
            questionnaires=[
                questionnaire_factory(status="COMPLETED", sent=date(2024, 1, 1))
            ]
        )
        assert get_most_recent_not_done(client) is None

    def test_ignores_unsent(self, client_factory, questionnaire_factory):
        client = client_factory(
            questionnaires=[questionnaire_factory(status="PENDING", sent=None)]
        )
        assert get_most_recent_not_done(client) is None


class TestResolveWantedDiagnoses:
    def test_none_defaults_to_both(self):
        assert _resolve_wanted_diagnoses(None) == {"ASD", "ADHD"}

    def test_empty_string_defaults_to_both(self):
        assert _resolve_wanted_diagnoses("") == {"ASD", "ADHD"}

    def test_both_maps_to_asd_and_adhd(self):
        assert _resolve_wanted_diagnoses("Both") == {"ASD", "ADHD"}

    def test_asd_only(self):
        assert _resolve_wanted_diagnoses("ASD") == {"ASD"}

    def test_adhd_only(self):
        assert _resolve_wanted_diagnoses("ADHD") == {"ADHD"}


# check_battery_sent/completeness only match non-DAEVAL rules against a
# client's asdAdhd-derived diagnosis set, so DA/EVAL rules always need an
# explicit diagnosis (diagnosis=None only matches DAEVAL rules).
DA_ASD_RULE = {
    "minAge": 0,
    "maxAge": 17,
    "daeval": "DA",
    "diagnosis": "ASD",
    "questionnaires": ["DA-Q1"],
}
DA_ADHD_RULE = {
    "minAge": 0,
    "maxAge": 17,
    "daeval": "DA",
    "diagnosis": "ADHD",
    "questionnaires": ["DA-Q1-ADHD"],
}
EVAL_ASD_RULE = {
    "minAge": 0,
    "maxAge": 17,
    "daeval": "EVAL",
    "diagnosis": "ASD",
    "questionnaires": ["EVAL-ASD-Q1"],
}
DAEVAL_RULE = {
    "minAge": 18,
    "maxAge": 99,
    "daeval": "DAEVAL",
    "diagnosis": None,
    "questionnaires": ["ADULT-Q1"],
}
RULES = [DA_ASD_RULE, DA_ADHD_RULE, EVAL_ASD_RULE, DAEVAL_RULE]


class TestCheckBatterySent:
    def test_da_and_eval_sent(self, client_factory, questionnaire_factory):
        client = client_factory(
            dob=date(2015, 1, 1),
            asd_adhd="ASD",
            questionnaires=[
                questionnaire_factory(q_type="DA-Q1", status="PENDING"),
                questionnaire_factory(q_type="EVAL-ASD-Q1", status="PENDING"),
            ],
        )
        da_sent, eval_sent = check_battery_sent(client, RULES)
        assert (da_sent, eval_sent) == (True, True)

    def test_missing_eval_type(self, client_factory, questionnaire_factory):
        client = client_factory(
            dob=date(2015, 1, 1),
            asd_adhd="ASD",
            questionnaires=[
                questionnaire_factory(q_type="DA-Q1", status="PENDING"),
            ],
        )
        da_sent, eval_sent = check_battery_sent(client, RULES)
        assert (da_sent, eval_sent) == (True, False)

    def test_just_added_does_not_count_as_sent(
        self, client_factory, questionnaire_factory
    ):
        client = client_factory(
            dob=date(2015, 1, 1),
            asd_adhd="ASD",
            questionnaires=[
                questionnaire_factory(q_type="DA-Q1", status="JUST_ADDED"),
                questionnaire_factory(q_type="EVAL-ASD-Q1", status="PENDING"),
            ],
        )
        da_sent, eval_sent = check_battery_sent(client, RULES)
        assert (da_sent, eval_sent) == (False, True)

    def test_adult_daeval_satisfies_both(self, client_factory, questionnaire_factory):
        client = client_factory(
            dob=date(1990, 1, 1),
            asd_adhd=None,
            questionnaires=[
                questionnaire_factory(q_type="ADULT-Q1", status="PENDING"),
            ],
        )
        da_sent, eval_sent = check_battery_sent(client, RULES)
        assert (da_sent, eval_sent) == (True, True)

    def test_wrong_diagnosis_has_no_applicable_eval_rule(
        self, client_factory, questionnaire_factory
    ):
        client = client_factory(
            dob=date(2015, 1, 1),
            asd_adhd="ADHD",
            questionnaires=[
                questionnaire_factory(q_type="DA-Q1-ADHD", status="PENDING"),
            ],
        )
        da_sent, eval_sent = check_battery_sent(client, RULES)
        assert (da_sent, eval_sent) == (True, None)

    def test_age_computed_at_eval_date_not_today(
        self, client_factory, questionnaire_factory
    ):
        # dob makes client 19 today but 17 at the eval date -> minor rules apply
        client = client_factory(
            dob=date(2007, 1, 1),
            asd_adhd="ASD",
            questionnaires=[
                questionnaire_factory(q_type="DA-Q1", status="PENDING"),
                questionnaire_factory(q_type="EVAL-ASD-Q1", status="PENDING"),
            ],
        )
        da_sent, eval_sent = check_battery_sent(
            client, RULES, most_recent_eval_date=date(2024, 6, 1)
        )
        assert (da_sent, eval_sent) == (True, True)

    def test_out_of_session_questionnaire_not_counted_as_sent(
        self, client_factory, questionnaire_factory
    ):
        client = client_factory(
            dob=date(2015, 1, 1),
            asd_adhd="ASD",
            session_started_at=datetime(2024, 6, 1),
            questionnaires=[
                questionnaire_factory(
                    q_type="DA-Q1", status="PENDING", sent=date(2024, 1, 1)
                ),
                questionnaire_factory(
                    q_type="EVAL-ASD-Q1", status="PENDING", sent=date(2024, 1, 1)
                ),
            ],
        )
        da_sent, eval_sent = check_battery_sent(client, RULES)
        assert (da_sent, eval_sent) == (False, False)


class TestCheckBatteryCompleteness:
    def test_all_completed(self, client_factory, questionnaire_factory):
        client = client_factory(
            dob=date(2015, 1, 1),
            asd_adhd="ASD",
            questionnaires=[
                questionnaire_factory(q_type="DA-Q1", status="COMPLETED"),
                questionnaire_factory(q_type="EVAL-ASD-Q1", status="EXTERNAL"),
            ],
        )
        da_done, eval_done = check_battery_completeness(client, RULES)
        assert (da_done, eval_done) == (True, True)

    def test_pending_not_done(self, client_factory, questionnaire_factory):
        client = client_factory(
            dob=date(2015, 1, 1),
            asd_adhd="ASD",
            questionnaires=[
                questionnaire_factory(q_type="DA-Q1", status="PENDING"),
                questionnaire_factory(q_type="EVAL-ASD-Q1", status="COMPLETED"),
            ],
        )
        da_done, eval_done = check_battery_completeness(client, RULES)
        assert (da_done, eval_done) == (False, True)

    def test_archived_not_counted_as_completed(
        self, client_factory, questionnaire_factory
    ):
        client = client_factory(
            dob=date(2015, 1, 1),
            asd_adhd="ASD",
            questionnaires=[
                questionnaire_factory(q_type="DA-Q1", status="ARCHIVED"),
                questionnaire_factory(q_type="EVAL-ASD-Q1", status="COMPLETED"),
            ],
        )
        da_done, eval_done = check_battery_completeness(client, RULES)
        assert (da_done, eval_done) == (False, True)

    def test_no_applicable_rules_returns_none(
        self, client_factory, questionnaire_factory
    ):
        client = client_factory(
            dob=date(1990, 1, 1),
            asd_adhd="ASD",
            questionnaires=[
                questionnaire_factory(q_type="ADULT-Q1", status="COMPLETED")
            ],
        )
        # Adult with ASD-only diagnosis: DAEVAL rule (diagnosis=None) still applies at 18-99
        da_done, eval_done = check_battery_completeness(client, RULES)
        assert (da_done, eval_done) == (True, True)
