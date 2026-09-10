import urllib.parse
from datetime import datetime

import pytest
from selenium.common.exceptions import NoSuchElementException

from utils.platforms.therapyappointment import find_form_link_for_session
from utils.selenium import initialize_selenium

LINK_TEXT = "Receiving Consent to Release of Information"


def _row(name: str, assigned: str, status: str, href: str | None) -> str:
    name_cell = (
        f'<td aria-label="Assigned online form name">{name}</td>'
        if href is None
        else f'<td><a aria-label="Assigned online form name: {name}" href="{href}">{name}</a></td>'
    )
    return (
        "<tr>"
        f"{name_cell}"
        f'<td aria-label="Assigned online form date">{assigned}<br>for Someone, LPES</td>'
        '<td aria-label="Clinical content">No</td>'
        f'<td aria-label="Status">{status}</td>'
        "</tr>"
    )


def _page(*rows: str) -> str:
    html = f"<html><body><table><tbody>{''.join(rows)}</tbody></table></body></html>"
    return "data:text/html;charset=utf-8," + urllib.parse.quote(html)


COMPLETED = "Completed on 6/18/26 at 11:48 PM"


@pytest.fixture(autouse=True)
def _headless(monkeypatch):
    monkeypatch.setenv("HEADLESS", "true")


@pytest.fixture
def driver():
    d = initialize_selenium()
    yield d
    d.quit()


def test_returns_the_only_completed_copy(driver):
    driver.get(_page(_row(LINK_TEXT, "02/25/2026 11:31 AM", COMPLETED, "/forms/only")))
    link = find_form_link_for_session(driver, LINK_TEXT, None)
    assert link.get_attribute("href").endswith("/forms/only")


def test_prefers_the_newest_assigned_copy(driver):
    driver.get(
        _page(
            _row(LINK_TEXT, "02/25/2026 11:31 AM", COMPLETED, "/forms/old"),
            _row(LINK_TEXT, "06/01/2026 09:00 AM", COMPLETED, "/forms/new"),
        )
    )
    link = find_form_link_for_session(driver, LINK_TEXT, None)
    assert link.get_attribute("href").endswith("/forms/new")


def test_raises_when_newest_copy_is_not_completed(driver):
    driver.get(
        _page(
            _row(LINK_TEXT, "02/25/2026 11:31 AM", COMPLETED, "/forms/old"),
            _row(LINK_TEXT, "06/01/2026 09:00 AM", "Not Started", None),
        )
    )
    with pytest.raises(NoSuchElementException, match="is not completed"):
        find_form_link_for_session(driver, LINK_TEXT, None)


def test_raises_when_newest_completion_predates_session(driver):
    driver.get(_page(_row(LINK_TEXT, "02/25/2026 11:31 AM", COMPLETED, "/forms/old")))
    with pytest.raises(NoSuchElementException, match="before session start"):
        find_form_link_for_session(driver, LINK_TEXT, datetime(2027, 1, 1))
