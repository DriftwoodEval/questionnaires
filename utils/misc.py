import re
from datetime import date
from typing import Literal, Optional

import yaml
from loguru import logger

from utils.database import add_failure_to_db
from utils.google import add_to_failure_sheet
from utils.types import (
    Config,
    Services,
)


def load_config() -> tuple[Services, Config]:
    """Load and parse the configuration from the 'info.yml' file.

    Returns:
        tuple[Services, Config]: A tuple containing the initialized `Services`
        and `Config` instances.
    """
    with open("./config/info.yml", "r") as file:
        logger.debug("Loading config info file")
        info = yaml.safe_load(file)
        services = info["services"]
        config = info["config"]
        # Validate as Services and Config types
        try:
            services = Services(**services)
            config = Config(**config)
        except Exception:
            logger.exception("Invalid config info file")
            exit(1)
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

    if daeval != "Records":
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
