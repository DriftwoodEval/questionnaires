import json
from datetime import date, datetime, timedelta
from typing import cast

import loguru
import pytest

from utils.custom_types import Config
from utils.misc import (
    MAX_FAILURE_REASON_LENGTH,
    add_failure,
    check_distance,
    clean_failure_reason,
    json_log_format,
    stderr_log_format,
)


class TestAddFailure:
    @pytest.mark.parametrize(
        ("error", "expected_length"),
        [
            ("short error", 11),
            ("x" * 2000, MAX_FAILURE_REASON_LENGTH),
        ],
    )
    def test_truncates_error_before_persisting(
        self, monkeypatch, error, expected_length
    ):
        captured = {}

        def fake_add_to_failure_sheet(_config, _client_id, error, *_args, **_kwargs):
            captured["sheet_error"] = error

        def fake_add_failure_to_db(_config, _client_id, error, *_args, **_kwargs):
            captured["db_error"] = error

        monkeypatch.setattr(
            "utils.misc.add_to_failure_sheet", fake_add_to_failure_sheet
        )
        monkeypatch.setattr("utils.misc.add_failure_to_db", fake_add_failure_to_db)

        add_failure(
            config=cast("Config", None),
            client_id=1,
            error=error,
            failed_date=date(2024, 1, 1),
            full_name="Test Client",
        )

        assert len(captured["sheet_error"]) == expected_length
        assert len(captured["db_error"]) == expected_length
        assert captured["sheet_error"] == captured["db_error"]


class TestCleanFailureReason:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            # Selenium's empty-message exception string.
            ("Message: \n", "browser automation error (no detail)"),
            ("Message: None\n", "browser automation error (no detail)"),
            # Real message plus a stacktrace tail that must be dropped.
            (
                "Message: element not interactable\nStacktrace:\n\tat foo (bar.js:1)\n"
                * 3,
                "element not interactable",
            ),
            # Case-insensitive: qsend re-adds prior reasons lowercased.
            ("message: \n", "browser automation error (no detail)"),
            # Ordinary reasons pass through untouched.
            ("portal not opened", "portal not opened"),
            ("too young", "too young"),
        ],
    )
    def test_strips_selenium_wrapper(self, raw, expected):
        assert clean_failure_reason(raw) == expected


class TestCheckDistance:
    @pytest.mark.parametrize(
        ("offset_days", "expected"),
        [
            (0, 0),
            (5, 5),
            (-3, -3),
        ],
    )
    def test_check_distance(self, offset_days, expected):
        assert check_distance(date.today() - timedelta(days=offset_days)) == expected


def make_record(message: str) -> "loguru.Record":
    return cast(
        "loguru.Record",
        {
            "time": datetime(2024, 1, 1, 12, 0, 0),
            "level": type("Level", (), {"name": "INFO"})(),
            "name": "some.module",
            "function": "some_function",
            "line": 42,
            "message": message,
            "exception": None,
        },
    )


class TestStderrLogFormat:
    @pytest.mark.parametrize(
        ("message", "expected_substring"),
        [
            ("value <injected>", r"value \<injected>"),
            ("value {not_a_placeholder}", "value {{not_a_placeholder}}"),
            ("plain message", "plain message"),
        ],
    )
    def test_stderr_log_format(self, message, expected_substring):
        assert expected_substring in stderr_log_format(make_record(message))


def _undo_loguru_escaping(formatted: str) -> str:
    """Reverse the escaping json_log_format applies for loguru's markup/format_map parsing."""
    return formatted.replace(r"\<", "<").replace("{{", "{").replace("}}", "}")


class TestJsonLogFormat:
    def test_produces_json_line_once_unescaped(self):
        formatted = json_log_format(make_record("hello world"))
        assert formatted.endswith("\n")
        parsed = json.loads(_undo_loguru_escaping(formatted).strip())
        assert parsed["message"] == "hello world"
        assert parsed["level"] == "INFO"
        assert parsed["module"] == "some.module"
        assert parsed["function"] == "some_function"
        assert parsed["line"] == 42
        assert parsed["time"] == "2024-01-01T12:00:00"

    def test_braces_and_angle_brackets_in_message_are_escaped(self):
        formatted = json_log_format(make_record("value {foo} <bar>"))
        assert "{{foo}}" in formatted
        assert r"\<bar>" in formatted
        parsed = json.loads(_undo_loguru_escaping(formatted).strip())
        assert parsed["message"] == "value {foo} <bar>"
