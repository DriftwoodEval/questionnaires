from typing import Optional

import requests
from backoff import expo, on_exception, on_predicate
from loguru import logger
from ratelimit import RateLimitException, limits

from utils.custom_types import Config, Services


def log_backoff(details):
    """Logging function for backoff library."""
    logger.debug(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


def log_giveup(details):
    """Logging function for giving up with backoff library."""
    logger.error(
        "Gave up after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


def _decorated_request(func):
    """Applies the common backoff and rate-limiting deorators."""
    return on_exception(
        expo,
        RateLimitException,
        max_tries=5,
        on_backoff=log_backoff,
        on_giveup=log_giveup,
    )(limits(calls=10, period=1)(func))


class LimitedRequest:
    """Custom request class with rate limiting."""

    @_decorated_request
    def get(self, url: str, params=None, headers=None, **kwargs) -> requests.Response:
        """Custom get request with rate limiting."""
        return requests.get(url, params, headers=headers, **kwargs)

    @_decorated_request
    def post(self, url: str, data=None, headers=None, **kwargs) -> requests.Response:
        """Custom post request with rate limiting."""
        return requests.post(url, data, headers=headers, **kwargs)


class NotEnoughCreditsError(requests.HTTPError):
    """Custom exception for when not enough credits are available."""

    def __init__(self, *args, **kwargs):
        """Initializes the NotEnoughCreditsError exception."""
        default_message = (
            "The organization does not have enough prepaid credits to send the message."
        )

        # If the user provides a custom message, use it; otherwise, use the default.
        if not args:
            args = (default_message,)

        super().__init__(*args, **kwargs)


class OpenPhone:
    """Custom class for interacting with the OpenPhone API."""

    def __init__(self, config: Config, services: Services):
        self.config = config
        self.services = services
        self.main_number = services.openphone.main_number
        self.default_user = services.openphone.users[config.name.lower()].id
        self.limited_request = LimitedRequest()

    def _get_auth_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Authorization": self.services.openphone.key,
        }

    @on_exception(
        expo,
        (ConnectionError, requests.HTTPError),
        factor=2,
        base=2,
        max_tries=5,
        on_backoff=log_backoff,
        on_giveup=log_giveup,
    )
    def get_text_info(self, message_id: str) -> dict:
        """Retrieves information about a text message, retrying exponentially on failure."""
        url = f"https://api.openphone.com/v1/messages/{message_id}"
        response = self.limited_request.get(url, headers=self._get_auth_headers())

        if response is None:
            raise ConnectionError("Failed to retrieve response from OpenPhone API")

        if response.status_code >= 400:
            raise requests.HTTPError("API response: {}".format(response.status_code))

        response_data = response.json().get("data")
        return response_data

    @on_predicate(
        expo,
        factor=2,
        base=2,
        max_tries=5,
        on_backoff=log_backoff,
        on_giveup=log_giveup,
    )
    def check_text_delivered(self, message_id: str) -> bool:
        """Checks if a text message has been delivered, retrying exponentially on failure."""
        message_info = self.get_text_info(message_id)
        message_status = message_info["status"]
        return message_status == "delivered"

    def send_text(
        self,
        message: str,
        to_number: str,
        from_number: Optional[str] = None,
        user_blame: Optional[str] = None,
    ) -> Optional[dict]:
        """Sends a text message, retrying exponentially on failure."""
        if from_number is None:
            from_number = self.main_number
        if user_blame is None:
            user_blame = self.default_user

        to_number = "+1" + "".join(filter(str.isdigit, to_number))
        url = "https://api.openphone.com/v1/messages"
        data = {
            "content": message,
            "from": from_number,
            "to": [to_number],
            "userId": user_blame,
        }
        try:
            logger.info(f"Attempting to send message '{message}' to {to_number}")
            response = self.limited_request.post(
                url, headers=self._get_auth_headers(), json=data
            )

            if response is None:
                raise ConnectionError("Failed to retrieve response from OpenPhone API")

            if response.status_code == 402:  # Payment Required
                raise NotEnoughCreditsError()

            if response.status_code >= 400:
                raise requests.HTTPError(
                    "API response: {}".format(response.status_code)
                )

            response_data = response.json().get("data")
            return response_data

        except NotEnoughCreditsError:
            logger.error("Not enough credits to send message")
            raise
        except Exception:
            logger.exception("Failed to get message info")
            return None
