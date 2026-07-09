from datetime import UTC, date, datetime

import requests
from loguru import logger
from ratelimit import RateLimitException, limits
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_exponential,
)

from utils.custom_types import Config, Services

API_BASE = "https://api.openphone.com/v1/"
RATE_LIMIT_CALLS = 10
RATE_LIMIT_PERIOD = 1


def before_sleep_loguru(retry_state: RetryCallState) -> None:
    """Custom callback to log retries using Loguru."""
    if retry_state.outcome is None:
        return

    fn_name = retry_state.fn.__name__ if retry_state.fn else "unknown_function"

    if retry_state.outcome.failed:
        verb = "raised"
        value = retry_state.outcome.exception()
    else:
        verb = "returned"
        value = retry_state.outcome.result()

    sleep_time = retry_state.next_action.sleep if retry_state.next_action else 0.0

    logger.debug(
        f"Retrying {fn_name} "
        f"in {sleep_time:0.2f}s "
        f"after attempt {retry_state.attempt_number}. "
        f"Last attempt {verb}: {value}"
    )


def should_continue_polling(status: str) -> bool:
    """
    Retry Condition:
    - Return True (Retry) if status is pending (queued, sent).
    - Return False (Stop) if status is final (delivered, undelivered).
    """
    pending_statuses = {"queued", "sent"}
    return status in pending_statuses


class NotEnoughCreditsError(requests.HTTPError):
    """Custom exception for payment/credit limits."""

    def __init__(self, message="Organization has insufficient prepaid credits."):
        super().__init__(message)


class InvalidPhoneNumberError(ValueError):
    """Custom exception for invalid phone numbers."""

    def __init__(self, message="Invalid phone number format."):
        super().__init__(message)


def is_transient_error(exception: Exception) -> bool:
    """Check if the exception is a transient error that should be retried."""
    if isinstance(exception, requests.HTTPError) and exception.response is not None:
        # Do not retry on 400, 401, 403, 404, 422
        return exception.response.status_code not in [400, 401, 403, 404, 422]
    return isinstance(exception, (requests.ConnectionError, RateLimitException))


class OpenPhone:
    """Custom class for interacting with the OpenPhone API."""

    def __init__(self, config: Config, services: Services):
        self.config = config
        self.main_number = services.openphone.main_number
        self.default_user = self._resolve_user_id(config.name, services.openphone.users)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Authorization": services.openphone.key,
            }
        )

    def _resolve_user_id(self, config_name: str, users: dict) -> str | None:
        """Matches config name to OpenPhone user ID by first name."""
        target_name = config_name.lower()
        for name, user in users.items():
            if name.lower().split()[0] == target_name:
                return user.id
        logger.error(f"User '{config_name} not found in OpenPhone. Using number owner.")
        return None

    _retry_network = retry(
        retry=retry_if_exception_type(Exception) & retry_if_result(is_transient_error),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_loguru,
    )

    _retry_poll_delivery = retry(
        retry=retry_if_result(should_continue_polling),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(10),
        before_sleep=before_sleep_loguru,
    )

    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    @_retry_network
    def get_text_info(self, message_id: str) -> dict:
        """Retrieves raw info dict. Retries only on network errors."""
        url = f"{API_BASE}messages/{message_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json().get("data", {})

    @_retry_poll_delivery
    def _poll_delivery_status(self, message_id: str) -> str:
        """
        Internal helper: Fetches status.
        Tenacity retries if status is 'queued'/'sent'.
        Stops immediately if 'delivered' or 'undelivered'.
        """
        info = self.get_text_info(message_id)
        return info.get("status", "unknown")

    def check_text_delivered(self, message_id: str) -> bool:
        """
        Blocks until message is delivered, failed, or times out.
        Returns True only if strictly 'delivered'.
        """
        try:
            final_status = self._poll_delivery_status(message_id)
            if final_status == "delivered":
                return True

            logger.warning(f"Message {message_id} ended with status: {final_status}")
            return False

        except Exception as e:
            logger.error(f"Failed to verify delivery for {message_id}: {e}")
            return False

    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    @_retry_network
    def _fetch_phone_number_id(self) -> str | None:
        """Fetch the OpenPhone phone number ID for the main number."""
        url = f"{API_BASE}phone-numbers"
        response = self.session.get(url)
        response.raise_for_status()
        for pn in response.json().get("data", []):
            if pn.get("phoneNumber") == self.main_number:
                return pn["id"]
        logger.warning(f"Could not find phone number ID for {self.main_number}")
        return None

    def _get_phone_number_id(self) -> str | None:
        """Return the cached phone number ID, fetching it if needed."""
        if not hasattr(self, "_phone_number_id"):
            self._phone_number_id = self._fetch_phone_number_id()
        return self._phone_number_id

    def has_client_replied(self, client_phone: str, since: date | None = None) -> bool:
        """Return True if the client has sent us an incoming message, optionally since a given date."""
        phone_number_id = self._get_phone_number_id()
        if not phone_number_id:
            return False

        digits = "".join(filter(str.isdigit, client_phone))
        if len(digits) == 10:
            clean_phone = "+1" + digits
        elif len(digits) == 11 and digits.startswith("1"):
            clean_phone = "+" + digits
        else:
            logger.warning(f"Cannot check replies for malformed number: {client_phone}")
            return False

        try:
            url = f"{API_BASE}messages"
            params: list[tuple[str, str]] = [
                ("phoneNumberId", phone_number_id),
                ("participants[]", clean_phone),
                ("direction", "incoming"),
                ("maxResults", "25"),
            ]
            if since is not None:
                since_dt = datetime.combine(since, datetime.min.time()).replace(tzinfo=UTC)
                params.append(("createdAfter", since_dt.isoformat()))

            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json().get("data", [])

            if since is None:
                return len(data) > 0

            # Client-side filter as a fallback in case the API ignores createdAfter
            since_dt = datetime.combine(since, datetime.min.time()).replace(tzinfo=UTC)
            for msg in data:
                created_at = msg.get("createdAt") or msg.get("createdAtMs")
                if created_at is None:
                    continue
                if isinstance(created_at, (int, float)):
                    msg_dt = datetime.fromtimestamp(created_at / 1000, tz=UTC)
                else:
                    msg_dt = datetime.fromisoformat(str(created_at))
                if msg_dt >= since_dt:
                    return True
            return False
        except Exception as e:
            logger.error(f"Failed to check incoming messages for {client_phone}: {e}")
            return False

    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    @_retry_network
    def send_text(
        self,
        message: str,
        to_number: str,
        from_number: str | None = None,
        user_blame: str | None = None,
        mark_done: bool = False,
    ) -> dict | None:
        """Sends text. Retries on network errors. Fails fast on payment errors."""
        if from_number is None:
            from_number = self.main_number
        if user_blame is None:
            user_blame = self.default_user

        digits = "".join(filter(str.isdigit, to_number))

        if len(digits) == 10:
            if digits.startswith("1"):
                raise InvalidPhoneNumberError(
                    f"10-digit number starts with 1: {to_number}"
                )
            to_number_clean = "+1" + digits
        elif len(digits) == 11:
            if not digits.startswith("1"):
                raise InvalidPhoneNumberError(
                    f"11-digit number does not start with 1: {to_number}"
                )
            to_number_clean = "+" + digits
        else:
            raise InvalidPhoneNumberError(
                f"Phone number has invalid length ({len(digits)}): {to_number}"
            )

        url = f"{API_BASE}messages"

        payload = {
            "content": message,
            "from": from_number,
            "to": [to_number_clean],
            "userId": user_blame,
        }

        if mark_done:
            payload["setInboxStatus"] = "done"

        try:
            logger.info(f"Sending message to {to_number_clean}...")
            response = self.session.post(url, json=payload)

            if response.status_code == 402:
                raise NotEnoughCreditsError

            if response.status_code == 400:
                logger.error(f"Bad Request to OpenPhone: {response.text}")

            response.raise_for_status()
            return response.json().get("data")

        except NotEnoughCreditsError:
            # Catch explicitly to avoid the Retry decorator handling it
            logger.error("Organization has insufficient credits. Cannot send.")
            raise
