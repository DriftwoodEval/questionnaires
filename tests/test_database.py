from datetime import date

import pytest

from utils.custom_types import FailedClientFromDB
from utils.database import get_most_recent_failure


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
