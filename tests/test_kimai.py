from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from utils.custom_types import KimaiService
from utils.kimai import export_timesheets

KIMAI = KimaiService(url="https://kimai.example.com/", token="secret")


def _response(payload):
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def _session_for(routes: dict[str, list]) -> MagicMock:
    """Build a fake requests.Session whose .get dispatches on the URL path."""
    session = MagicMock()

    def get(url, **_kwargs):
        for path, payload in routes.items():
            if url.endswith(path):
                return _response(payload)
        return _response([])

    session.get.side_effect = get
    return session


@pytest.fixture
def routes():
    return {
        "/api/timesheets": [
            {
                "id": 1,
                "begin": "2026-08-10T09:00:00-0400",
                "duration": 3600,
                "description": "Report writing",
                "rate": 50.0,
                "project": 7,
                "activity": 3,
                "user": 2,
                "tags": ["billable"],
            }
        ],
        "/api/users": [{"id": 2, "alias": "Jane Doe", "username": "jane"}],
        "/api/activities": [{"id": 3, "name": "Writing"}],
        "/api/customers": [{"id": 9, "name": "Driftwood"}],
        "/api/projects": [{"id": 7, "name": "Evaluations", "customer": 9}],
    }


def test_export_writes_enriched_spreadsheet(tmp_path, routes):
    with patch("utils.kimai._session", return_value=_session_for(routes)):
        result = export_timesheets(
            KIMAI, date(2026, 8, 10), date(2026, 8, 16), tmp_path
        )

    assert result is not None
    assert result.parent == tmp_path

    df = pd.read_excel(result, sheet_name="Timesheets")
    row = df.iloc[0]
    assert row["User"] == "Jane Doe"
    assert "Project" not in df.columns
    assert row["Activity"] == "Writing"
    assert row["Customer"] == "Driftwood"
    assert row["Hours"] == 1.0

    summary = pd.read_excel(result, sheet_name="Hours by Person")
    assert summary.columns.tolist() == ["Person", "Hours"]
    assert summary.iloc[0]["Person"] == "Jane Doe"
    assert summary.iloc[0]["Hours"] == 1.0


def test_export_returns_none_when_no_entries(tmp_path):
    with patch(
        "utils.kimai._session", return_value=_session_for({"/api/timesheets": []})
    ):
        result = export_timesheets(
            KIMAI, date(2026, 8, 10), date(2026, 8, 16), tmp_path
        )

    assert result is None


def test_export_returns_none_on_timesheet_fetch_error(tmp_path):
    session = MagicMock()
    session.get.side_effect = requests.ConnectionError("boom")

    with patch("utils.kimai._session", return_value=session):
        result = export_timesheets(
            KIMAI, date(2026, 8, 10), date(2026, 8, 16), tmp_path
        )

    assert result is None


def test_export_tolerates_lookup_failures(tmp_path, routes):
    session = _session_for(routes)
    original = session.get.side_effect

    def get(url, **kwargs):
        if url.endswith("/api/projects"):
            raise requests.ConnectionError("no projects")
        return original(url, **kwargs)

    session.get.side_effect = get

    with patch("utils.kimai._session", return_value=session):
        result = export_timesheets(
            KIMAI, date(2026, 8, 10), date(2026, 8, 16), tmp_path
        )

    assert result is not None
    df = pd.read_excel(result, sheet_name="Timesheets")
    assert "Project" not in df.columns
    assert df.iloc[0]["User"] == "Jane Doe"
