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
