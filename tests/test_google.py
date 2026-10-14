import io
from unittest.mock import MagicMock

import pytest

from utils import google
from utils.google import SCOPES, col_index_to_a1, google_authenticate


class TestColIndexToA1:
    @pytest.mark.parametrize(
        ("col_index", "expected"),
        [
            (0, "A"),
            (25, "Z"),
            (26, "AA"),
            (51, "AZ"),
            (52, "BA"),
        ],
    )
    def test_col_index_to_a1(self, col_index, expected):
        assert col_index_to_a1(col_index) == expected


@pytest.fixture
def _clear_auth_cache():
    google_authenticate.cache_clear()
    yield
    google_authenticate.cache_clear()


@pytest.mark.usefixtures("_clear_auth_cache")
def test_google_authenticate_uses_this_projects_scopes_and_files(monkeypatch):
    creds = object()
    shared = MagicMock(return_value=creds)
    monkeypatch.setattr(google, "authenticate", shared)

    assert google_authenticate() is creds
    assert google_authenticate() is creds

    shared.assert_called_once_with(
        SCOPES,
        "./config/credentials.json",
        "config/token.json",
        require_exact_scopes=True,
    )


class TestGmailWrappers:
    def test_gmail_service_uses_the_bounded_builder_with_the_shared_credentials(
        self, monkeypatch
    ):
        creds = object()
        built = []
        monkeypatch.setattr(google, "google_authenticate", lambda: creds)
        monkeypatch.setattr(
            google,
            "build_google_service",
            lambda api, version, c: built.append((api, version, c)) or "service",
        )

        assert google._gmail_service() == "service"  # noqa: SLF001
        assert built == [("gmail", "v1", creds)]

    def test_send_gmail_passes_stream_attachments_as_named_bytes(self, monkeypatch):
        sent = {}

        def fake_send(service, *args, **kwargs):
            sent.update(service=service, args=args, kwargs=kwargs)
            return {"id": "m1"}

        monkeypatch.setattr(google, "_gmail_service", lambda: "service")
        monkeypatch.setattr(google.gmail, "send_email", fake_send)

        result = google.send_gmail(
            "Body",
            "Subject",
            "to@example.com",
            "from@example.com",
            cc_addr="cc@example.com",
            attachments=[
                {"stream": io.BytesIO(b"%PDF"), "filename": "report.pdf"},
                {"stream": None, "filename": "skipped.pdf"},
                {"stream": io.BytesIO(b"x"), "filename": ""},
            ],
        )

        assert result == {"id": "m1"}
        assert sent["service"] == "service"
        assert sent["args"] == ("Body", "Subject", "to@example.com", "from@example.com")
        assert sent["kwargs"]["cc_addr"] == "cc@example.com"
        assert sent["kwargs"]["attachments"] == [(b"%PDF", "report.pdf")]

    def test_send_gmail_returns_none_when_sending_fails(self, monkeypatch):
        monkeypatch.setattr(google, "_gmail_service", lambda: "service")
        monkeypatch.setattr(
            google.gmail, "send_email", MagicMock(side_effect=RuntimeError("quota"))
        )

        assert (
            google.send_gmail("Body", "Subject", "to@example.com", "from@example.com")
            is None
        )

    def test_listing_lets_api_errors_raise(self, monkeypatch):
        monkeypatch.setattr(google, "_gmail_service", lambda: "service")
        monkeypatch.setattr(
            google.gmail,
            "list_message_stubs",
            MagicMock(side_effect=RuntimeError("quota")),
        )

        with pytest.raises(RuntimeError, match="quota"):
            google.list_gmail_messages("is:unread")
