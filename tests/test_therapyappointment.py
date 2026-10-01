import urllib.parse
from datetime import datetime

import pytest
from selenium.common.exceptions import NoSuchElementException

from utils.platforms.therapyappointment import (
    CHARTER_RECEIVING,
    CHARTER_SENDING,
    STANDARD_RECEIVING,
    STANDARD_SENDING,
    check_if_docs_signed,
    find_form_link_for_session,
)
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


def _docs_signed_page(*, registration_complete: bool, rows: tuple[str, ...]) -> str:
    registration_text = (
        "Client has completed registration"
        if registration_complete
        else "Client has not completed registration"
    )
    html = (
        "<html><body>"
        f"<div>{registration_text}</div>"
        '<a href="#">Docs & Forms</a>'
        f"<table><tbody>{''.join(rows)}</tbody></table>"
        "</body></html>"
    )
    return "data:text/html;charset=utf-8," + urllib.parse.quote(html)


COMPLETED = "Completed on 6/18/26 at 11:48 PM"
NOT_STARTED = "Not Started"


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
    href = link.get_attribute("href")
    assert href is not None
    assert href.endswith("/forms/only")


def test_prefers_the_newest_assigned_copy(driver):
    driver.get(
        _page(
            _row(LINK_TEXT, "02/25/2026 11:31 AM", COMPLETED, "/forms/old"),
            _row(LINK_TEXT, "06/01/2026 09:00 AM", COMPLETED, "/forms/new"),
        )
    )
    link = find_form_link_for_session(driver, LINK_TEXT, None)
    href = link.get_attribute("href")
    assert href is not None
    assert href.endswith("/forms/new")


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


def test_docs_signed_when_all_forms_completed(driver):
    driver.get(
        _docs_signed_page(
            registration_complete=True,
            rows=(_row("Some Form", "02/25/2026 11:31 AM", COMPLETED, "/forms/x"),),
        )
    )
    assert check_if_docs_signed(driver) is True


def test_docs_not_signed_when_registration_incomplete(driver):
    driver.get(_docs_signed_page(registration_complete=False, rows=()))
    assert check_if_docs_signed(driver) is False


def test_docs_not_signed_when_a_form_is_unsigned(driver):
    driver.get(
        _docs_signed_page(
            registration_complete=True,
            rows=(_row("Some Form", "02/25/2026 11:31 AM", NOT_STARTED, None),),
        )
    )
    assert check_if_docs_signed(driver) is False


@pytest.mark.parametrize(
    "school_form_name",
    [STANDARD_RECEIVING, STANDARD_SENDING, CHARTER_RECEIVING, CHARTER_SENDING],
)
def test_unsigned_school_form_ignored_for_client_22_or_older(driver, school_form_name):
    driver.get(
        _docs_signed_page(
            registration_complete=True,
            rows=(_row(school_form_name, "02/25/2026 11:31 AM", NOT_STARTED, None),),
        )
    )
    assert check_if_docs_signed(driver, age=22) is True


def test_unsigned_school_form_still_blocks_under_22(driver):
    driver.get(
        _docs_signed_page(
            registration_complete=True,
            rows=(_row(STANDARD_RECEIVING, "02/25/2026 11:31 AM", NOT_STARTED, None),),
        )
    )
    assert check_if_docs_signed(driver, age=21) is False


def test_unsigned_non_school_form_still_blocks_at_22(driver):
    driver.get(
        _docs_signed_page(
            registration_complete=True,
            rows=(_row("Some Other Form", "02/25/2026 11:31 AM", NOT_STARTED, None),),
        )
    )
    assert check_if_docs_signed(driver, age=22) is False
