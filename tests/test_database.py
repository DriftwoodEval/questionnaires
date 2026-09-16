from datetime import date
from typing import cast
from unittest.mock import MagicMock

import pytest

from utils.custom_types import ClientWithQuestionnaires, Config, FailedClientFromDB
from utils.database import (
    get_most_recent_failure,
    update_failure_in_db,
    update_questionnaires_in_db,
)


def make_failed_client(failures: list[dict]) -> FailedClientFromDB:
    return FailedClientFromDB.model_validate(
        {
            "id": 1,
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
            "failure": failures,
        }
    )


def make_client_with_questionnaires(
    questionnaires: list[dict],
) -> ClientWithQuestionnaires:
    return ClientWithQuestionnaires.model_validate(
        {
            "id": 1,
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


def make_questionnaire(
    questionnaire_type: str = "CAT-Q",
    sent: date | None = date(2024, 1, 1),
    status: str = "PENDING",
    reminded: int = 0,
    last_reminded: date | None = None,
) -> dict:
    return {
        "clientId": 1,
        "questionnaireType": questionnaire_type,
        "link": "https://example.com",
        "sent": sent,
        "status": status,
        "reminded": reminded,
        "lastReminded": last_reminded,
    }


def make_failure(
    failed_date: date | None,
    reason: str = "portal not opened",
    reminded: int = 0,
) -> dict:
    return {
        "failedDate": failed_date,
        "reason": reason,
        "daEval": "EVAL",
        "reminded": reminded,
        "lastReminded": None,
    }


class TestGetMostRecentFailure:
    @pytest.mark.parametrize(
        ("failures", "expected_reason"),
        [
            ([make_failure(date(2024, 1, 1))], "portal not opened"),
            (
                [
                    make_failure(date(2024, 1, 1), reason="a"),
                    make_failure(date(2024, 6, 1), reason="b"),
                    make_failure(date(2024, 3, 1), reason="c"),
                ],
                "b",
            ),
            (
                [
                    make_failure(date(2024, 1, 1), reason="old", reminded=100),
                    make_failure(date(2023, 1, 1), reason="older", reminded=0),
                ],
                "older",
            ),
            ([make_failure(date(2024, 1, 1), reminded=100)], None),
        ],
    )
    def test_get_most_recent_failure(self, failures, expected_reason):
        result = get_most_recent_failure(make_failed_client(failures))
        if expected_reason is None:
            assert result is None
        else:
            assert result is not None
            assert result["reason"] == expected_reason


class TestUpdateFailureInDb:
    def _connection(self):
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        conn.cursor.return_value.__exit__.return_value = False
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        return conn, cursor

    def test_touches_updated_at_without_audit_log_when_nothing_changed(
        self, monkeypatch
    ):
        conn, cursor = self._connection()
        audit_calls = []
        monkeypatch.setattr("utils.database.get_db", lambda _config: conn)
        monkeypatch.setattr(
            "utils.database.record_audit_log",
            lambda *args, **kwargs: audit_calls.append((args, kwargs)),
        )

        update_failure_in_db(
            config=cast("Config", None), client_id=1, reason="portal not opened"
        )

        sql = cursor.execute.call_args[0][0]
        assert "updatedAt = NOW()" in sql
        assert audit_calls == []
        conn.commit.assert_called_once()

    def test_writes_audit_log_when_something_actually_changed(self, monkeypatch):
        conn, cursor = self._connection()
        audit_calls = []
        monkeypatch.setattr("utils.database.get_db", lambda _config: conn)
        monkeypatch.setattr(
            "utils.database.record_audit_log",
            lambda *args, **kwargs: audit_calls.append((args, kwargs)),
        )

        update_failure_in_db(
            config=cast("Config", None),
            client_id=1,
            reason="portal not opened",
            resolved=True,
        )

        sql = cursor.execute.call_args[0][0]
        assert "updatedAt = NOW()" not in sql
        assert len(audit_calls) == 1
        args, _ = audit_calls[0]
        assert args[1] == "internal.failure.update"
        conn.commit.assert_called_once()


class TestUpdateQuestionnairesInDb:
    def _connection(self, previous_rows: list[dict | None]):
        cursor = MagicMock()
        cursor.fetchone.side_effect = previous_rows
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        conn.cursor.return_value.__exit__.return_value = False
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        return conn, cursor

    def test_no_audit_log_when_nothing_changed(self, monkeypatch):
        conn, _cursor = self._connection(
            previous_rows=[{"status": "PENDING", "reminded": 0, "lastReminded": None}]
        )
        audit_calls = []
        monkeypatch.setattr("utils.database.get_db", lambda _config: conn)
        monkeypatch.setattr(
            "utils.database.record_audit_log",
            lambda *args, **kwargs: audit_calls.append((args, kwargs)),
        )

        client = make_client_with_questionnaires([make_questionnaire()])
        update_questionnaires_in_db(cast("Config", None), [client])

        assert audit_calls == []
        conn.commit.assert_called_once()

    def test_audit_log_only_includes_the_changed_questionnaire_and_fields(
        self, monkeypatch
    ):
        conn, _cursor = self._connection(
            previous_rows=[
                {"status": "PENDING", "reminded": 0, "lastReminded": None},
                {
                    "status": "COMPLETED",
                    "reminded": 2,
                    "lastReminded": date(2024, 1, 5),
                },
            ]
        )
        audit_calls = []
        monkeypatch.setattr("utils.database.get_db", lambda _config: conn)
        monkeypatch.setattr(
            "utils.database.record_audit_log",
            lambda *args, **kwargs: audit_calls.append((args, kwargs)),
        )

        client = make_client_with_questionnaires(
            [
                make_questionnaire(
                    questionnaire_type="CAT-Q", status="COMPLETED", reminded=0
                ),
                make_questionnaire(
                    questionnaire_type="ASRS",
                    status="COMPLETED",
                    reminded=2,
                    last_reminded=date(2024, 1, 5),
                ),
            ]
        )
        update_questionnaires_in_db(cast("Config", None), [client])

        assert len(audit_calls) == 1
        args, kwargs = audit_calls[0]
        assert args[1] == "internal.questionnaire.bulkUpdate"
        questionnaires = kwargs["detail"]["questionnaires"]
        assert len(questionnaires) == 1
        assert questionnaires[0]["questionnaireType"] == "CAT-Q"
        assert questionnaires[0]["status"] == {"from": "PENDING", "to": "COMPLETED"}
        assert "reminded" not in questionnaires[0]
        assert "lastReminded" not in questionnaires[0]
        conn.commit.assert_called_once()
