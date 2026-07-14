"""Integration tests for QGlobal questionnaire generation.

Hits the live QGlobal site. Every assessment type is generated in both
states our code actually branches on (this mirrors the check qsend.py does
before calling any gen_basc_*/gen_vineland function):
  - "new": client has no QGlobal account yet, so generation must create
    one first (add_client_to_qglobal).
  - "existing": client already has a QGlobal account, so generation should
    go straight to search-and-select.

A real "existing" QGlobal client is one that got an account on an earlier
visit - so the "existing" case for a questionnaire reuses the exact client
the "new" case for that same questionnaire already created, rather than a
fresh one that's never actually been added to QGlobal. This relies on
pytest running every "new" case before any "existing" one (state is the
outer parametrize), so each "new" case's client exists by the time its
"existing" counterpart runs.

See the end-of-run summary for which clients need deleting from QGlobal.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from utils.platforms.qglobal import (
    add_client_to_qglobal,
    check_and_login_qglobal,
    check_for_qglobal_account,
    gen_basc_adolescent,
    gen_basc_child,
    gen_basc_preschool,
    gen_vineland,
)

pytestmark = [pytest.mark.integration, pytest.mark.qglobal]

# Keyed by the same questionnaire type names used in QUESTIONNAIRE_AGES.
GEN_FUNCS: dict[str, Callable] = {
    "BASC Preschool": gen_basc_preschool,
    "BASC Child": gen_basc_child,
    "BASC Adolescent": gen_basc_adolescent,
    "Vineland": gen_vineland,
}

# questionnaire -> the client the "new" case created for it, so the
# "existing" case for the same questionnaire can reuse it.
_new_state_clients: dict[str, pd.Series] = {}


@pytest.fixture(scope="module")
def logged_in_qglobal(driver, real_config, qglobal_cleanup):  # noqa: ARG001
    services, _config = real_config
    check_and_login_qglobal(driver, services, first_time=True)
    return driver


@pytest.mark.parametrize("questionnaire", sorted(GEN_FUNCS))
@pytest.mark.parametrize("state", ["new", "existing"])
def test_gen_assessment(
    logged_in_qglobal,
    real_config,
    fake_client_factory,
    age_for_questionnaire,
    dob_for_age,
    state,
    questionnaire,
):
    services, config = real_config
    gen_func = GEN_FUNCS[questionnaire]

    if state == "existing":
        # Reuse the client the "new" case for this questionnaire already
        # created and added to QGlobal - a real existing client, not a
        # fresh ID that's never been added at all.
        client = _new_state_clients[questionnaire]
        assert check_for_qglobal_account(logged_in_qglobal, services, client)
        just_created = False
    else:
        client = fake_client_factory(
            "qglobal",
            {"Date of Birth": dob_for_age(age_for_questionnaire(questionnaire))},
        )
        _new_state_clients[questionnaire] = client
        # qsend.py always adds the account once it's confirmed missing,
        # regardless of whether the client is "new" or "existing" to us.
        assert not check_for_qglobal_account(logged_in_qglobal, services, client)
        assert add_client_to_qglobal(logged_in_qglobal, services, client)
        just_created = True

    link = gen_func(logged_in_qglobal, services, config, client, just_created)

    assert link
    assert link.startswith("http")
