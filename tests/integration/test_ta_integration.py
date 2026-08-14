"""Integration test for sending a TherapyAppointment message.

Hits the live TherapyAppointment site against a known fake client
(C006813026) rather than creating one, since TA client creation isn't
automated here. No cleanup needed: this only sends a message, it doesn't
create any new TA state.
"""

import pytest

from utils.platforms.therapyappointment import (
    check_and_login_ta,
    go_to_client,
    send_message_ta,
)

pytestmark = [pytest.mark.integration, pytest.mark.ta]

FAKE_CLIENT_ID = "6813026"


def test_send_message(driver, real_config):
    services, _config = real_config
    check_and_login_ta(driver, services, first_time=True)
    client_url = go_to_client(driver, services, FAKE_CLIENT_ID)
    assert client_url

    send_message_ta(driver, client_url, "Integration test message, please ignore.")
