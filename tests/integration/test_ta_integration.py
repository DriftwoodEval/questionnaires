"""Integration test for sending a TherapyAppointment message.

Hits the live TherapyAppointment site against a known fake client
(C006813026) rather than creating one, since TA client creation isn't
automated here. No cleanup needed: this only sends a message, it doesn't
create any new TA state.
"""

import os

import pytest
from selenium.webdriver.common.by import By

from utils.platforms.therapyappointment import (
    assign_online_forms,
    check_and_login_ta,
    form_row_present,
    go_to_client,
    send_message_ta,
)
from utils.selenium import click_element

pytestmark = [pytest.mark.integration, pytest.mark.ta]

FAKE_CLIENT_ID = "6813026"

PRIVATE_SCHOOL_FORMS = [
    "Private School Receiving Release of Information",
    "Private School Sending Release of Information",
]


def test_send_message(driver, real_config):
    services, _config = real_config
    check_and_login_ta(driver, services, first_time=True)
    client_url = go_to_client(driver, services, FAKE_CLIENT_ID)
    assert client_url

    send_message_ta(driver, client_url, "Integration test message, please ignore.")


def test_assign_private_school_forms(driver, real_config):
    """Assigns the private-school consent forms to the fake client.

    Ignores referral data / DB / client-existence checks: it just drives the
    TA UI directly against the fake client (or $TA_TEST_CLIENT_ID, if set, to
    point it at a specific known client). Idempotent - only assigns forms that
    aren't already on the client. To re-test the assign flow from scratch,
    remove the forms from the client in TA first.
    """
    client_id = os.getenv("TA_TEST_CLIENT_ID", FAKE_CLIENT_ID)
    services, _config = real_config
    check_and_login_ta(driver, services, first_time=True)
    assert go_to_client(driver, services, client_id)

    click_element(driver, By.LINK_TEXT, "Docs & Forms")
    missing = [n for n in PRIVATE_SCHOOL_FORMS if not form_row_present(driver, n)]

    if missing:
        assign_online_forms(driver, missing)

    for name in PRIVATE_SCHOOL_FORMS:
        assert form_row_present(driver, name), f"{name} was not assigned"
