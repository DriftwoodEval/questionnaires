"""Integration tests for MHS questionnaire generation.

Hits the live MHS site. Every assessment type shares one
`add_client_to_mhs` flow that branches on `accounts_created["mhs"]`:
  - "new": nothing created yet, so generation goes through the full
    add-client form.
  - "existing": the client already has an MHS account (from a prior
    assessment), so generation goes straight to the "add to existing
    client" search flow.

The "new" cases each get their own fake client, with a DOB matched to that
questionnaire's typical age range. The "existing" cases share one client
seeded ahead of time via `mhs_existing_client` (since that's what actually
produces the "already has an account" state on the real site), but each
case still sends the age appropriate for its own questionnaire - MHS's
add_client_to_mhs already handles reconciling an age that doesn't match
what's on file (see the "Making sure age matches" branch), so this is safe
even though the underlying account was created with a different age.

See the end-of-run summary for which clients need deleting from MHS.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from utils.platforms.mhs import (
    check_and_login_mhs,
    gen_asrs_2_5,
    gen_asrs_6_18,
    gen_caars_2,
    gen_conners_4,
    gen_conners_ec,
)

pytestmark = [pytest.mark.integration, pytest.mark.mhs]

GEN_FUNCS: dict[str, Callable] = {
    "Conners EC": gen_conners_ec,
    "Conners 4": gen_conners_4,
    "Conners 4 Self": lambda driver, services, client, accounts_created: gen_conners_4(
        driver, services, client, accounts_created, self_report=True
    ),
    "ASRS (2-5 Years)": gen_asrs_2_5,
    "ASRS (6-18 Years)": gen_asrs_6_18,
    "CAARS 2": gen_caars_2,
}


@pytest.fixture(scope="module")
def logged_in_mhs(driver, real_config, mhs_cleanup):  # noqa: ARG001
    services, _config = real_config
    check_and_login_mhs(driver, services, first_time=True)
    return driver


@pytest.fixture(scope="module")
def mhs_existing_client(
    logged_in_mhs, real_config, fake_client_factory, age_for_questionnaire, dob_for_age
):
    """A client with a real, pre-existing MHS account, for the "existing" cases."""
    services, _config = real_config
    age = age_for_questionnaire("Conners EC")
    client = fake_client_factory("mhs", {"Date of Birth": dob_for_age(age), "Age": age})
    _link, accounts_created = gen_conners_ec(logged_in_mhs, services, client, {})
    assert accounts_created.get("mhs")
    return client


@pytest.mark.parametrize("questionnaire", sorted(GEN_FUNCS))
def test_gen_new_client(
    logged_in_mhs,
    real_config,
    fake_client_factory,
    age_for_questionnaire,
    dob_for_age,
    questionnaire,
):
    services, _config = real_config
    age = age_for_questionnaire(questionnaire)
    client = fake_client_factory("mhs", {"Date of Birth": dob_for_age(age), "Age": age})
    gen_func = GEN_FUNCS[questionnaire]

    link, accounts_created = gen_func(logged_in_mhs, services, client, {})

    assert accounts_created.get("mhs")
    assert link
    assert link.startswith("http")


@pytest.mark.parametrize("questionnaire", sorted(GEN_FUNCS))
def test_gen_existing_client(
    logged_in_mhs,
    real_config,
    mhs_existing_client,
    age_for_questionnaire,
    dob_for_age,
    questionnaire,
):
    services, _config = real_config
    age = age_for_questionnaire(questionnaire)
    client: pd.Series = mhs_existing_client.copy()
    client["Date of Birth"] = dob_for_age(age)
    client["Age"] = age
    gen_func = GEN_FUNCS[questionnaire]

    link, accounts_created = gen_func(logged_in_mhs, services, client, {"mhs": True})

    assert accounts_created.get("mhs")
    assert link
    assert link.startswith("http")
