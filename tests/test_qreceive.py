from datetime import date

import pytest

from qreceive import (
    _deserialize_email_info,
    _merge_email_infos,
    _serialize_email_info,
    build_failure_message,
    should_send_reminder,
)
from utils.custom_types import AdminEmailInfo, FailedClientFromDB


def make_empty_email_info() -> AdminEmailInfo:
    return {"ignoring": [], "completed": [], "call": [], "failed": [], "errors": []}


def make_failed_client(client_id: int, reason: str = "portal not opened"):
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
        assert should_send_reminder(reminded_count, distance) == expected


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
