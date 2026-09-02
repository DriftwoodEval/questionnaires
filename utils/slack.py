import requests
from loguru import logger

from utils.misc import load_local_settings


def send_slack_alert(message: str) -> None:
    """Post an alert to the configured Slack webhook.

    A fallback channel for failures that might mean email itself is
    unreliable (e.g. Google auth being broken), so it never raises: a
    problem here is logged and swallowed rather than allowed to interrupt
    whatever run triggered the alert.
    """
    webhook_url = load_local_settings().slack_webhook_url
    if not webhook_url:
        logger.warning(f"Slack webhook not configured, dropping alert: {message}")
        return

    try:
        response = requests.post(webhook_url, json={"text": message}, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        logger.exception("Failed to send Slack alert")
