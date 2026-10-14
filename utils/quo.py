"""Quo (OpenPhone) client for this project.

The API logic lives in `driftwood_common.quo_client`. This subclass reads the
sending number and the Quo users from `Config` and `Services`.
"""

from driftwood_common.quo_client import QuoClient
from loguru import logger

from utils.custom_types import Config, Services


class Quo(QuoClient):
    """The shared Quo client, set up from this project's config and services."""

    def __init__(self, config: Config, services: Services):
        users = services.openphone.users
        super().__init__(
            services.openphone.key,
            main_number=services.openphone.main_number,
            default_user=self._resolve_user_id(config.name, users),
            business_timezone=config.business_timezone,
        )
        self.config = config
        self.referral_user = self._resolve_user_id(config.referral_sender_name, users)
        self.private_pay_referral_user = self._resolve_user_id(
            config.private_pay_sender_name, users
        )

    @staticmethod
    def _resolve_user_id(config_name: str, users: dict) -> str | None:
        """Matches config name to Quo user ID by first name."""
        target_name = config_name.lower()
        for name, user in users.items():
            if name.lower().split()[0] == target_name:
                return user.id
        logger.error(f"User '{config_name} not found in Quo. Using number owner.")
        return None
