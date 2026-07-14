"""Fixtures for integration tests that hit real third-party sites.

These tests never touch client data or the punch list. They drive a real
Selenium session against the live MHS/QGlobal/WPS sites using real service
credentials (pulled from the same remote config API as production).

The one deliberate exception is `questionnaire_age_ranges`, which does a
read-only query of the emr_assessment_type table - the canonical
per-instrument age range that questionnaire_rule itself is built from - to
find a real, valid age for each questionnaire. That's the only reliable
source for "what age does this platform actually accept for this
questionnaire," so it's worth the DB read.

Each test case gets its own fake client (via `fake_client_factory`) rather
than sharing one and blocking on manual deletion between cases, since a
full matrix of questionnaires x states would mean stopping constantly.
Instead, every client created during a run is tracked and reported in a
summary at the end for a human to go delete.

Not collected/run by default, see the `integration` marker in pyproject.toml.
"""

import itertools
from collections import defaultdict
from collections.abc import Callable, Iterator
from datetime import date

import pandas as pd
import pytest
from dateutil.relativedelta import relativedelta
from loguru import logger
from selenium.webdriver.remote.webdriver import WebDriver

from utils.custom_types import Config, Services
from utils.database import get_assessment_types
from utils.misc import load_config
from utils.platforms.mhs import delete_client_from_mhs, empty_mhs_deleted_items
from utils.platforms.qglobal import search_by_name_qglobal
from utils.selenium import initialize_selenium

FAKE_CLIENT_FIELDS = {
    "TA First Name": "ZZZTEST",
    "TA Last Name": "ZZZTEST",
    "Gender": "Male",
    "Language": "English",
}

# IDs are drawn from a block (9999xxxxx) reserved for these tests so they're
# easy to recognize and never collide with a real client.
_id_counter = itertools.count(1)

# site name -> list of clients created during this run, for the end-of-run
# cleanup summary.
_pending_cleanup: dict[str, list[pd.Series]] = defaultdict(list)


@pytest.fixture(autouse=True)
def _log_test_start(request: pytest.FixtureRequest) -> None:
    """Logs which test is starting, so it's visible in the streamed
    per-step logs from the platform modules instead of only showing up in
    pytest's own summary at the end.
    """
    logger.info(f"Starting {request.node.name}")


@pytest.fixture(scope="session")
def real_config() -> tuple[Services, Config]:
    """Loads real service credentials and config from the remote API.

    Requires config/local_config.yml to be present, same as running the
    scripts for real.
    """
    return load_config()


@pytest.fixture(scope="module")
def driver() -> Iterator[WebDriver]:
    """One browser per test module (one per site), so logging in - which
    for some sites requires a human - only happens once per run.
    """
    d = initialize_selenium()
    yield d
    d.quit()


@pytest.fixture(scope="module")
def mhs_cleanup(
    driver: WebDriver, real_config: tuple[Services, Config]
) -> Iterator[None]:
    """After MHS integration tests finish, delete every client created
    during this run and empty MHS's Deleted Items.

    Runs while `driver` is still open (it depends on `driver`, so this
    tears down before that fixture quits the browser). Clients a human
    skips during confirmation are left in `_pending_cleanup["mhs"]` for the
    end-of-run summary.
    """
    yield
    services, _config = real_config
    pending = _pending_cleanup["mhs"]
    if not pending:
        return
    deleted_any = False
    remaining = []
    for client in pending:
        if delete_client_from_mhs(driver, services, client):
            deleted_any = True
        else:
            remaining.append(client)
    pending[:] = remaining
    if deleted_any:
        empty_mhs_deleted_items(driver, services)


@pytest.fixture(scope="module")
def qglobal_cleanup(
    driver: WebDriver, real_config: tuple[Services, Config]
) -> Iterator[None]:
    """After QGlobal integration tests finish, surface every test client
    created during this run for a human to delete in one pass.

    QGlobal has no verified, stable selector for its delete-examinee action
    (see delete_client_from_qglobal), so unlike MHS this can't be automated
    end to end. Searching by the shared "ZZZTEST" username instead of one
    search per client ID brings every test client from this run into the
    same results grid at once, so a human deletes from a single page
    instead of navigating to each client individually.
    """
    yield
    services, _config = real_config
    pending = _pending_cleanup["qglobal"]
    if not pending:
        return
    search_by_name_qglobal(driver, services, "zzz")
    listing = "\n".join(
        f"  - {c['TA First Name']} {c['TA Last Name']} ({c['Human Friendly ID']})"
        for c in pending
    )
    input(
        "QGlobal: please delete the following examinees from the search "
        f"results and press enter...\n{listing}\n"
    )
    pending.clear()


@pytest.fixture(scope="session")
def questionnaire_age_ranges(real_config) -> dict[str, tuple[int, int]]:
    """Min/max age per questionnaire, from the real emr_assessment_type
    table - one row per instrument, so there's no cross-rule ambiguity like
    there would be reading questionnaire_rule directly (a questionnaire can
    appear in several daeval/diagnosis rules with different, not
    necessarily contiguous, age ranges).
    """
    _services, config = real_config
    return {
        row["name"]: (row["minAge"], row["maxAge"])
        for row in get_assessment_types(config)
    }


@pytest.fixture(scope="session")
def age_for_questionnaire(
    questionnaire_age_ranges: dict[str, tuple[int, int]],
) -> Callable[[str], int]:
    """Picks an age (years) safely within a questionnaire's real min/max age
    range, from the assessment_type table.
    """

    def _age(questionnaire: str) -> int:
        min_age, max_age = questionnaire_age_ranges[questionnaire]
        return (min_age + max_age) // 2

    return _age


@pytest.fixture(scope="session")
def dob_for_age() -> Callable[[int], str]:
    """Returns a "YYYY/MM/DD" DOB string for a client who is N years old today."""

    def _dob(years: int) -> str:
        return (date.today() - relativedelta(years=years)).strftime("%Y/%m/%d")

    return _dob


@pytest.fixture(scope="session")
def fake_client_factory() -> Callable[[str, dict[str, object]], pd.Series]:
    """Builds a fake client with a fresh, unique ID and registers it for cleanup.

    `overrides` must at least set "Date of Birth" (and "Age" for MHS) - see
    age_for_questionnaire/dob_for_age, since which age is valid depends on
    the questionnaire being generated.
    """

    def _make(site: str, overrides: dict[str, object]) -> pd.Series:
        n = next(_id_counter)
        client_id = f"9999{n:05d}"
        fields = {
            **FAKE_CLIENT_FIELDS,
            "Client ID": client_id,
            "Human Friendly ID": f"C{client_id}",
            **overrides,
        }
        client = pd.Series(fields)
        _pending_cleanup[site].append(client)
        return client

    return _make


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:  # noqa: ARG001
    if not _pending_cleanup:
        return
    terminalreporter.write_sep("=", "integration test clients need manual deletion")
    for site in sorted(_pending_cleanup):
        terminalreporter.write_line(f"{site}:")
        for client in _pending_cleanup[site]:
            terminalreporter.write_line(
                f"  - {client['TA First Name']} {client['TA Last Name']} "
                f"({client['Human Friendly ID']})"
            )
