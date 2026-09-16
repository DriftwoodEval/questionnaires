from datetime import date
from typing import cast
from unittest.mock import MagicMock

import pytest

from utils.custom_types import Config, FailedClientFromDB
from utils.database import get_most_recent_failure, update_failure_in_db


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
