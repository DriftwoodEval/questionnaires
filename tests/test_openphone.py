import pytest
import requests
from ratelimit import RateLimitException

from utils.openphone import is_transient_error, should_continue_polling


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
