"""Integration tests for WPS questionnaire generation (DP-4).

Hits the live WPS site. `gen_dp4` always creates a fresh client (there's no
"client already exists" check in our WPS code), so there's only one
client-state path. What gen_dp4 does branch on is language (which form gets
selected) and gender (which UI option gets clicked), so the matrix below
covers those instead.

Each test uses its own fake client; see the end-of-run summary for which
clients need deleting from WPS.
"""

import pytest

from utils.platforms.wps import check_and_login_wps, gen_dp4

pytestmark = [pytest.mark.integration, pytest.mark.wps]


@pytest.fixture(scope="module")
def logged_in_wps(driver, real_config):
    services, _config = real_config
    check_and_login_wps(driver, services, first_time=True)
    return driver


@pytest.mark.parametrize(
    ("language", "gender"),
    [
        ("English", "Male"),
        ("English", "Female"),
        ("Spanish", "Male"),
        ("Spanish", "Female"),
    ],
)
def test_gen_dp4(
    logged_in_wps,
    real_config,
    fake_client_factory,
    age_for_questionnaire,
    dob_for_age,
    language,
    gender,
):
    _services, config = real_config
    client = fake_client_factory(
        "wps",
        {
            "Language": language,
            "Gender": gender,
            "Date of Birth": dob_for_age(age_for_questionnaire("DP-4")),
        },
    )

    link = gen_dp4(logged_in_wps, config, client)

    assert link
    assert link.startswith("http")
