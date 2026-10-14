from types import SimpleNamespace

import pytest

from utils.quo import Quo

USERS = {
    "Andrew Smith": SimpleNamespace(id="US_ANDREW"),
    "Jordan Lee": SimpleNamespace(id="US_JORDAN"),
}


def make_quo(config_factory, **config_overrides):
    config = config_factory(**config_overrides)
    services = SimpleNamespace(
        openphone=SimpleNamespace(key="key", main_number="+15551230000", users=USERS)
    )
    return Quo(config, services), config  # type: ignore[arg-type]


class TestQuoSetup:
    def test_uses_the_services_number_key_and_config_timezone(self, config_factory):
        quo, config = make_quo(config_factory)
        assert quo.main_number == "+15551230000"
        assert quo.session.headers["Authorization"] == "key"
        assert quo.business_timezone == config.business_timezone

    @pytest.mark.parametrize(
        ("config_name", "expected"),
        [("andrew", "US_ANDREW"), ("Jordan", "US_JORDAN"), ("nobody", None)],
    )
    def test_resolves_users_by_first_name(self, config_name, expected):
        assert Quo._resolve_user_id(config_name, USERS) == expected  # noqa: SLF001

    def test_resolves_the_default_and_referral_users_from_config(self, config_factory):
        quo, _ = make_quo(
            config_factory,
            name="andrew",
            referral_sender_name="jordan",
            private_pay_sender_name="nobody",
        )
        assert quo.default_user == "US_ANDREW"
        assert quo.referral_user == "US_JORDAN"
        assert quo.private_pay_referral_user is None
