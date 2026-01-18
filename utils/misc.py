import os
import re
import socket
import sys
from datetime import date
from functools import cache
from typing import Literal, Optional
from urllib.parse import urlparse

import requests
import yaml
from loguru import logger

from utils.custom_types import (
    Config,
    LocalSettings,
    Services,
)
from utils.database import add_failure_to_db
from utils.google import add_to_failure_sheet


@cache
def load_local_settings() -> LocalSettings:
    """Load local settings from local_config.yml."""
    local_config_path = "./config/local_config.yml"
    if not os.path.exists(local_config_path):
        logger.error(
            f"Local config file not found at {local_config_path}. Cannot determine API URL."
        )
        sys.exit(1)

    with open(local_config_path) as f:
        local_data = yaml.safe_load(f)

    try:
        local_settings = LocalSettings.model_validate(local_data)
        logger.debug(f"Local settings loaded. API URL: {local_settings.api_url}")
        return local_settings
    except Exception:
        logger.exception("Invalid local config file")
        sys.exit(1)


def load_config() -> tuple[Services, Config]:
    """Load config from API and apply local overrides."""
    local_settings = load_local_settings()
    api_url = local_settings.api_url + "/api/config"

    logger.debug(f"Fetching config from {api_url}")
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        remote_data = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch config from API: {e}")
        sys.exit(1)

    services_data = remote_data.get("services", {})
    config_data = remote_data.get("config", {})

    overrides = local_settings.config_overrides.model_dump(exclude_none=True)
    config_data.update(overrides)

    try:
        services = Services.model_validate(services_data)
        config = Config.model_validate(config_data)

    except Exception:
        logger.exception("Final merged config failed Pydantic validation.")
        sys.exit(1)

    logger.info("Configuration successfully loaded, merged, and validated.")
    return services, config


class NetworkSink:
    """Class for sending log data to a network socket."""

    def __init__(self, api_url, port):
        self.ip = urlparse(api_url).hostname
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.ip, port))
        except (OSError, ConnectionRefusedError, TimeoutError) as e:
            logger.error(f"Failed to connect to log server at {self.ip}:{port}: {e}")
            self.sock = None
            exit(1)

    def write(self, message):
        """Write a message to the network socket."""
        if self.sock and message.strip():
            self.sock.sendall(message.encode("utf-8"))


def add_failure(
    config: Config,
    client_id: int,
    error: str,
    failed_date: date,
    full_name: str,
    add_to_sheet: bool | None = True,
    add_to_db: bool | None = True,
    asd_adhd: str | None = None,
    daeval: Literal["DA", "EVAL", "DAEVAL", "Records"] | None = None,
    questionnaires_needed: list[str] | None = None,
    questionnaires_generated: list[dict[str, str]] | None = None,
) -> None:
    """Add a client to the failure sheet and database."""
    logger.debug(
        f"Failure information: {client_id}, {error}, {failed_date}, {full_name}, {asd_adhd}, {daeval}, {questionnaires_needed}, {questionnaires_generated}"
    )

    if add_to_sheet:
        add_to_failure_sheet(
            config,
            client_id,
            error,
            failed_date,
            full_name,
            asd_adhd,
            daeval,
            questionnaires_needed,
            questionnaires_generated,
        )

    if add_to_db:
        add_failure_to_db(config, client_id, error, failed_date, daeval)


### FORMATTING ###
def format_phone_number(phone_number: str) -> str:
    """Format a phone number string into (XXX) XXX-XXXX format.

    Args:
        phone_number (str): The phone number string to format.

    Returns:
        str: The formatted phone number string.
    """
    phone_number = re.sub(r"\D", "", phone_number)
    return f"({phone_number[:3]}) {phone_number[3:6]}-{phone_number[6:]}"


def check_distance(x: date) -> int:
    """Calculate the number of days between the given date and today.

    Args:
        x (date): The date to calculate the distance from.

    Returns:
        int: The number of days between x and today.
    """
    today = date.today()
    delta = today - x
    return delta.days
