import json
from datetime import date, datetime, timedelta
from typing import cast

import loguru
import pytest

from utils.misc import check_distance, json_log_format, stderr_log_format


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
