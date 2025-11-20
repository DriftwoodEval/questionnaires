import os
import re
import sys
from datetime import date
from typing import Literal, Optional

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


def load_local_settings() -> LocalSettings:
    """Load local settings from local_config.yml."""
    local_config_path = "./config/local_config.yml"
    if not os.path.exists(local_config_path):
        logger.error(
            f"Local config file not found at {local_config_path}. Cannot determine API URL."
        )
        sys.exit(1)

    with open(local_config_path, "r") as f:
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
        response = requests.get(api_url)
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


def add_failure(
    config: Config,
    client_id: int,
    error: str,
    failed_date: date,
    full_name: str,
    asd_adhd: Optional[str] = None,
    daeval: Optional[Literal["DA", "EVAL", "DAEVAL", "Records"]] = None,
    questionnaires_needed: Optional[list[str]] = None,
    questionnaires_generated: Optional[list[dict[str, str]]] = None,
) -> None:
    """Add a client to the failure sheet and database."""
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
