import io

import pytest
from google.auth.exceptions import RefreshError

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


class FakeCreds:
    def __init__(self, *, refresh_failures=0):
        self.scopes = list(SCOPES)
        self.refresh_token = "refresh-token"
        self.expired = True
        self.valid = False
        self._refresh_failures = refresh_failures
        self.refresh_calls = 0

    def refresh(self, _request):
        self.refresh_calls += 1
        if self.refresh_calls <= self._refresh_failures:
            raise RefreshError("transient")
        self.valid = True

    def to_json(self):
        return "{}"


@pytest.fixture
def _auth_env(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "token.json").write_text("{}")
    monkeypatch.setattr(google.time, "sleep", lambda _s: None)
    google_authenticate.cache_clear()
    yield
    google_authenticate.cache_clear()


@pytest.mark.usefixtures("_auth_env")
def test_google_authenticate_retries_transient_refresh_failure(monkeypatch):
    creds = FakeCreds(refresh_failures=2)
    monkeypatch.setattr(
        google.Credentials, "from_authorized_user_file", lambda *_a, **_k: creds
    )

    assert google_authenticate() is creds
    assert creds.refresh_calls == 3


@pytest.mark.usefixtures("_auth_env")
def test_google_authenticate_headless_raises_when_refresh_exhausted(monkeypatch):
    creds = FakeCreds(refresh_failures=99)
    monkeypatch.setattr(
        google.Credentials, "from_authorized_user_file", lambda *_a, **_k: creds
    )
    monkeypatch.setenv("HEADLESS", "true")

    with pytest.raises(RuntimeError, match="headless"):
        google_authenticate()


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
        def fail(*args, **kwargs):
            raise RuntimeError("quota")

        monkeypatch.setattr(google, "_gmail_service", lambda: "service")
        monkeypatch.setattr(google.gmail, "send_email", fail)

        assert (
            google.send_gmail("Body", "Subject", "to@example.com", "from@example.com")
            is None
        )

    def test_listing_lets_api_errors_raise(self, monkeypatch):
        def fail(*args, **kwargs):
            raise RuntimeError("quota")

        monkeypatch.setattr(google, "_gmail_service", lambda: "service")
        monkeypatch.setattr(google.gmail, "list_message_stubs", fail)

        with pytest.raises(RuntimeError, match="quota"):
            google.list_gmail_messages("is:unread")
