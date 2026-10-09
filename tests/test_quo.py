from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import pytest
import requests
from ratelimit import RateLimitException

from utils.quo import Quo, is_transient_error, should_continue_polling


class TestShouldContinuePolling:
    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            ("queued", True),
            ("sent", True),
            ("delivered", False),
            ("undelivered", False),
            ("unknown", False),
        ],
    )
    def test_should_continue_polling(self, status, expected):
        assert should_continue_polling(status) == expected


class TestIsTransientError:
    @pytest.mark.parametrize("status_code", [400, 401, 403, 404, 422])
    def test_non_transient_http_errors_are_not_retried(self, status_code):
        response = requests.Response()
        response.status_code = status_code
        error = requests.HTTPError(response=response)
        assert is_transient_error(error) is False

    @pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503])
    def test_transient_http_errors_are_retried(self, status_code):
        response = requests.Response()
        response.status_code = status_code
        error = requests.HTTPError(response=response)
        assert is_transient_error(error) is True

    def test_http_error_without_response_is_not_retried(self):
        assert is_transient_error(requests.HTTPError()) is False

    def test_connection_error_is_retried(self):
        assert is_transient_error(requests.ConnectionError()) is True

    def test_rate_limit_exception_is_retried(self):
        assert is_transient_error(RateLimitException("slow down", 1)) is True

    def test_unrelated_exception_is_not_retried(self):
        assert is_transient_error(ValueError("nope")) is False


class TestHasSentMessage:
    @staticmethod
    def _quo(messages):
        quo = Quo.__new__(Quo)
        quo.config = MagicMock(business_timezone="America/New_York")
        quo._get_phone_number_id = MagicMock(return_value="PN1")  # noqa: SLF001
        quo.session = MagicMock()
        quo.session.get.return_value.json.return_value = {"data": messages}
        return quo

    @staticmethod
    def _msg(text):
        return {"text": text, "createdAt": datetime.now(UTC).isoformat()}

    def test_matches_identical_text_ignoring_whitespace(self):
        quo = self._quo([self._msg("Hello  there,\nfriend")])
        assert quo.has_sent_message("8435550100", "Hello there, friend", date.today())

    def test_different_text_does_not_match(self):
        quo = self._quo([self._msg("A different reminder")])
        assert not quo.has_sent_message("8435550100", "Hello there", date.today())

    def test_matching_text_before_since_does_not_match(self):
        old = {"text": "Hello", "createdAt": "2000-01-01T00:00:00+00:00"}
        quo = self._quo([old])
        assert not quo.has_sent_message("8435550100", "Hello", date.today())
