from datetime import date, datetime
from pathlib import Path
from typing import cast

import pandas as pd
import pytest
from selenium.webdriver.remote.webdriver import WebDriver

from utils.custom_types import ClientFromDB, FailedClientFromDB
from utils.questionnaires import (
    _in_current_session,
    _resolve_wanted_diagnoses,
    all_questionnaires_done,
    check_battery_completeness,
    check_battery_sent,
    check_client_failed,
    check_client_previous,
    check_if_ignoring,
    filter_inactive_and_not_pending,
    generate_screenshot_filename,
    get_most_recent_not_done,
    normalize_q_name,
    save_screenshot_deduped,
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


class _FakeDriver:
    """Stands in for a WebDriver's save_screenshot, writing a real file to disk."""

    def save_screenshot(self, filepath) -> None:
        Path(filepath).write_bytes(b"fake-png-bytes")


def make_fake_driver() -> WebDriver:
    return cast(WebDriver, _FakeDriver())


class TestSaveScreenshotDeduped:
    def test_saves_the_new_screenshot(self, tmp_path):
        save_screenshot_deduped(
            make_fake_driver(), tmp_path, "COMPLETED_ASRS_x_20240101_120000.png"
        )
        assert (tmp_path / "COMPLETED_ASRS_x_20240101_120000.png").exists()

    def test_removes_prior_screenshot_with_same_identity_prefix(self, tmp_path):
        old = tmp_path / "COMPLETED_ASRS_x_20240101_090000.png"
        old.write_bytes(b"old-bytes")

        save_screenshot_deduped(
            make_fake_driver(), tmp_path, "COMPLETED_ASRS_x_20240101_120000.png"
        )

        assert not old.exists()
        assert (tmp_path / "COMPLETED_ASRS_x_20240101_120000.png").exists()

    def test_leaves_screenshots_with_different_identity_alone(self, tmp_path):
        unrelated = tmp_path / "PENDING_BASC_y_20240101_090000.png"
        unrelated.write_bytes(b"unrelated-bytes")

        save_screenshot_deduped(
            make_fake_driver(), tmp_path, "COMPLETED_ASRS_x_20240101_120000.png"
        )

        assert unrelated.exists()


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
    @pytest.mark.parametrize(
        ("asd_adhd", "expected"),
        [
            (None, {"ASD", "ADHD"}),
            ("", {"ASD", "ADHD"}),
            ("Both", {"ASD", "ADHD"}),
            ("ASD", {"ASD"}),
            ("ADHD", {"ADHD"}),
        ],
    )
    def test_resolve_wanted_diagnoses(self, asd_adhd, expected):
        assert _resolve_wanted_diagnoses(asd_adhd) == expected


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


def make_failed_client(client_id: int = 1, failure: list[dict] | None = None):
    return FailedClientFromDB.model_validate(
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
            "failure": failure
            if failure is not None
            else [
                {
                    "failedDate": date(2024, 1, 1),
                    "reason": "Failed randomly",
                    "daEval": "DA",
                    "reminded": 0,
                    "lastReminded": None,
                }
            ],
        }
    )


def make_client_from_db(client_id: int = 1, questionnaires=None):
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


class TestNormalizeQName:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("ASRS Self", "ASRS"),
            ("BASC", "BASC"),
        ],
    )
    def test_normalize_q_name(self, name, expected):
        assert normalize_q_name(name) == expected


class TestCheckClientFailed:
    def test_no_previous_failures_is_false(self):
        client_info = pd.Series({"Client ID": "1", "daeval": "DA"})
        assert check_client_failed({}, client_info) == (False, None)

    def test_missing_client_id_is_false(self):
        client_info = pd.Series({"Client ID": None, "daeval": "DA"})
        assert check_client_failed({1: make_failed_client(1)}, client_info) == (
            False,
            None,
        )

    def test_non_integer_client_id_is_false(self):
        client_info = pd.Series({"Client ID": "not-a-number", "daeval": "DA"})
        assert check_client_failed({1: make_failed_client(1)}, client_info) == (
            False,
            None,
        )

    def test_client_id_not_in_failures_is_false(self):
        client_info = pd.Series({"Client ID": "2", "daeval": "DA"})
        assert check_client_failed({1: make_failed_client(1)}, client_info) == (
            False,
            None,
        )

    def test_fully_reminded_failure_is_ignored(self):
        failure = [
            {
                "failedDate": date(2024, 1, 1),
                "reason": "No show",
                "daEval": "DA",
                "reminded": 100,
                "lastReminded": None,
            }
        ]
        client_info = pd.Series({"Client ID": "1", "daeval": "DA"})
        failed_clients = {1: make_failed_client(1, failure=failure)}
        assert check_client_failed(failed_clients, client_info) == (False, None)

    def test_records_failure_is_ignored_for_qsend(self):
        failure = [
            {
                "failedDate": date(2024, 1, 1),
                "reason": "Missing records",
                "daEval": "Records",
                "reminded": 0,
                "lastReminded": None,
            }
        ]
        client_info = pd.Series({"Client ID": "1", "daeval": "DA"})
        failed_clients = {1: make_failed_client(1, failure=failure)}
        assert check_client_failed(failed_clients, client_info) == (False, None)

    def test_da_current_matches_da_failure(self):
        client_info = pd.Series({"Client ID": "1", "daeval": "DA"})
        failed_clients = {1: make_failed_client(1)}
        assert check_client_failed(failed_clients, client_info) == (
            True,
            "failed randomly",
        )

    def test_eval_current_skips_da_only_failure(self):
        client_info = pd.Series({"Client ID": "1", "daeval": "EVAL"})
        failed_clients = {1: make_failed_client(1)}
        assert check_client_failed(failed_clients, client_info) == (False, None)

    def test_eval_current_matches_eval_failure(self):
        failure = [
            {
                "failedDate": date(2024, 1, 1),
                "reason": "No show",
                "daEval": "EVAL",
                "reminded": 0,
                "lastReminded": None,
            }
        ]
        client_info = pd.Series({"Client ID": "1", "daeval": "EVAL"})
        failed_clients = {1: make_failed_client(1, failure=failure)}
        assert check_client_failed(failed_clients, client_info) == (True, "no show")

    def test_daeval_current_always_matches(self):
        client_info = pd.Series({"Client ID": "1", "daeval": "DAEVAL"})
        failed_clients = {1: make_failed_client(1)}
        assert check_client_failed(failed_clients, client_info) == (
            True,
            "failed randomly",
        )


class TestCheckClientPrevious:
    def test_no_previous_clients_returns_none(self):
        client_info = pd.Series({"Client ID": "1"})
        assert check_client_previous({}, client_info) is None

    def test_client_not_found_returns_none(self):
        client_info = pd.Series({"Client ID": "2"})
        prev_clients = {1: make_client_from_db(1, questionnaires=[])}
        assert check_client_previous(prev_clients, client_info) is None

    def test_client_found_returns_questionnaires(self):
        client_info = pd.Series({"Client ID": "1"})
        questionnaires = [
            {
                "clientId": 1,
                "questionnaireType": "ASRS",
                "link": None,
                "sent": None,
                "status": "PENDING",
                "reminded": 0,
                "lastReminded": None,
            }
        ]
        prev_clients = {1: make_client_from_db(1, questionnaires=questionnaires)}
        assert check_client_previous(prev_clients, client_info) == questionnaires
