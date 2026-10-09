from datetime import UTC, date, datetime
from typing import cast
from unittest.mock import MagicMock

import pytest

from qreceive import (
    _deserialize_email_info,
    _merge_email_infos,
    _serialize_email_info,
    build_failure_message,
    mark_questionnaires_reminded,
    reconcile_reminders_from_history,
    should_send_reminder,
)
from utils.custom_types import AdminEmailInfo, FailedClientFromDB, Questionnaire


def make_empty_email_info() -> AdminEmailInfo:
    return {"ignoring": [], "completed": [], "call": [], "failed": [], "errors": []}


def make_failed_client(client_id: int, reason: str = "portal not opened"):
    return FailedClientFromDB.model_validate(
        {
            "id": client_id,
            "hash": "testhash",
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
            "failure": [
                {
                    "failedDate": date(2024, 1, 1),
                    "reason": reason,
                    "daEval": "EVAL",
                    "reminded": 0,
                    "lastReminded": None,
                }
            ],
        }
    )


class TestShouldSendReminder:
    @pytest.mark.parametrize(
        ("reminded_count", "distance", "expected"),
        [
            (0, 0, True),
            (0, 5, True),
            (1, 13, False),
            (1, 14, True),
            (1, 20, True),
            (2, 6, False),
            (2, 7, True),
            (3, 100, False),
        ],
    )
    def test_should_send_reminder(self, reminded_count, distance, expected):
        settings = {
            "stage2OffsetDays": 14,
            "stage3OffsetDays": 7,
            "escalationSilenceDays": 3,
        }
        assert should_send_reminder(reminded_count, distance, settings) == expected


class TestBuildFailureMessage:
    @pytest.mark.parametrize(
        ("reason", "expected_substring"),
        [
            ("portal not opened", "patient portal"),
            ("docs not signed", "Forms"),
            ("too young for asd", None),
        ],
    )
    def test_build_failure_message(self, config_factory, reason, expected_substring):
        client = make_failed_client(1, reason=reason)
        message = build_failure_message(config_factory(), client)
        if expected_substring is None:
            assert message is None
        else:
            assert message is not None
            assert expected_substring in message


class TestMergeEmailInfos:
    def test_dedupes_by_client_id_across_runs(self, client_factory):
        client = client_factory(client_id=1)
        info_a: AdminEmailInfo = {**make_empty_email_info(), "completed": [client]}
        info_b: AdminEmailInfo = {**make_empty_email_info(), "completed": [client]}
        merged = _merge_email_infos([info_a, info_b])
        assert len(merged["completed"]) == 1

    def test_completed_client_removed_from_call(self, client_factory):
        client = client_factory(client_id=1)
        info: AdminEmailInfo = {
            **make_empty_email_info(),
            "call": [client],
            "completed": [client],
        }
        merged = _merge_email_infos([info])
        assert merged["completed"] == [client]
        assert merged["call"] == []

    def test_errors_deduped(self):
        info_a: AdminEmailInfo = {**make_empty_email_info(), "errors": ["boom"]}
        info_b: AdminEmailInfo = {
            **make_empty_email_info(),
            "errors": ["boom", "other"],
        }
        merged = _merge_email_infos([info_a, info_b])
        assert merged["errors"] == ["boom", "other"]

    def test_empty_infos_list(self):
        assert _merge_email_infos([]) == make_empty_email_info()


class TestEmailInfoSerializationRoundTrip:
    def test_round_trips_completed_and_ignoring(self, client_factory):
        info: AdminEmailInfo = {
            **make_empty_email_info(),
            "ignoring": [client_factory(client_id=1)],
            "completed": [client_factory(client_id=2)],
        }
        round_tripped = _deserialize_email_info(_serialize_email_info(info))
        assert round_tripped["ignoring"][0].id == 1
        assert round_tripped["completed"][0].id == 2

    def test_round_trips_call_and_failed_with_mixed_client_types(self, client_factory):
        failed_client = make_failed_client(3)
        info: AdminEmailInfo = {
            **make_empty_email_info(),
            "call": [client_factory(client_id=1), failed_client],
            "failed": [(failed_client, "portal not opened")],
            "errors": ["some error"],
        }
        round_tripped = _deserialize_email_info(_serialize_email_info(info))
        assert [c.id for c in round_tripped["call"]] == [1, 3]
        assert isinstance(round_tripped["call"][1], FailedClientFromDB)
        assert round_tripped["failed"] == [(failed_client, "portal not opened")]
        assert round_tripped["errors"] == ["some error"]


class TestMarkQuestionnairesReminded:
    def test_only_pending_questionnaires_are_counted(self):
        client = MagicMock()
        client.questionnaires = [
            {"status": "PENDING", "reminded": 1, "lastReminded": None},
            {"status": "COMPLETED", "reminded": 1, "lastReminded": None},
        ]
        mark_questionnaires_reminded(client)
        assert client.questionnaires[0]["reminded"] == 2
        assert client.questionnaires[0]["lastReminded"] == date.today()
        assert client.questionnaires[1]["reminded"] == 1
        assert client.questionnaires[1]["lastReminded"] is None


class TestReconcileRemindersFromHistory:
    @staticmethod
    def _setup(reminded, last_reminded, sent_texts):
        client = MagicMock()
        client.id = 7
        client.phoneNumber = "8435550100"
        q = cast(
            "Questionnaire",
            {
                "status": "PENDING",
                "sent": date(2026, 1, 1),
                "reminded": reminded,
                "lastReminded": last_reminded,
            },
        )
        client.questionnaires = [q]
        quo = MagicMock()
        quo.get_sent_texts.return_value = sent_texts
        config = MagicMock(business_timezone="America/New_York")
        return config, quo, client, q

    @staticmethod
    def _stage_one_text():
        return (
            "Hello, this is Dr A with Driftwood Evaluation Center. We are waiting "
            "for you to complete the questionnaire sent to you on 01/01 (5 days "
            "ago). We are unable to schedule your appointment until it is "
            "completed in its entirety. You can find it in the messages tab in "
            "our patient portal: https://portal.therapyappointment.com Please "
            "reply to this text with any questions. Thank you for your help."
        )

    def test_raises_count_from_template_not_text_count(self, monkeypatch):
        monkeypatch.setattr("qreceive.update_questionnaires_in_db", MagicMock())
        text = self._stage_one_text()
        sent = datetime(2026, 1, 6, 15, tzinfo=UTC)
        # The same stage-2 reminder went out three times: the count is still 2.
        config, quo, client, q = self._setup(1, date(2026, 1, 1), [(text, sent)] * 3)
        assert reconcile_reminders_from_history(
            config, quo, client, q, templates={}, overrides={}, dry_run=False
        )
        assert q["reminded"] == 2
        assert q["lastReminded"] == date(2026, 1, 6)

    def test_never_lowers_a_higher_stored_count(self, monkeypatch):
        update = MagicMock()
        monkeypatch.setattr("qreceive.update_questionnaires_in_db", update)
        sent = datetime(2026, 1, 6, 15, tzinfo=UTC)
        config, quo, client, q = self._setup(
            2, date(2026, 1, 8), [(self._stage_one_text(), sent)]
        )
        assert not reconcile_reminders_from_history(
            config, quo, client, q, templates={}, overrides={}, dry_run=False
        )
        assert q["reminded"] == 2
        assert q["lastReminded"] == date(2026, 1, 8)
        update.assert_not_called()

    def test_unrelated_texts_change_nothing(self):
        sent = datetime(2026, 1, 6, 15, tzinfo=UTC)
        config, quo, client, q = self._setup(0, None, [("See you Tuesday!", sent)])
        assert not reconcile_reminders_from_history(
            config, quo, client, q, templates={}, overrides={}, dry_run=False
        )
        assert q["reminded"] == 0

    def test_skips_quo_lookup_when_count_already_maxed(self):
        config, quo, client, q = self._setup(3, date(2026, 1, 8), [])
        assert not reconcile_reminders_from_history(
            config, quo, client, q, templates={}, overrides={}, dry_run=False
        )
        quo.get_sent_texts.assert_not_called()
