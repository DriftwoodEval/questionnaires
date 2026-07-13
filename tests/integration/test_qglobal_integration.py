"""Integration tests for QGlobal questionnaire generation.

Hits the live QGlobal site. Every assessment type is generated in both
states our code actually branches on (this mirrors the check qsend.py does
before calling any gen_basc_*/gen_vineland function):
  - "new": client has no QGlobal account yet, so generation must create
    one first (add_client_to_qglobal).
  - "existing": client already has a QGlobal account, so generation should
    go straight to search-and-select.

Each case uses its own fake client; see the end-of-run summary for which
clients need deleting from QGlobal.
"""

from collections.abc import Callable

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


@pytest.fixture(scope="module")
def logged_in_qglobal(driver):
    check_and_login_qglobal(driver, first_time=True)
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
    _services, config = real_config
    client = fake_client_factory(
        "qglobal",
        {"Date of Birth": dob_for_age(age_for_questionnaire(questionnaire))},
    )
    gen_func = GEN_FUNCS[questionnaire]

    if state == "existing":
        # Pre-create the account, same as qsend.py does before generating
        # for a client it already knows about.
        assert add_client_to_qglobal(logged_in_qglobal, client)
    else:
        assert not check_for_qglobal_account(logged_in_qglobal, client)

    link = gen_func(logged_in_qglobal, config, client)

    assert link
    assert link.startswith("http")
