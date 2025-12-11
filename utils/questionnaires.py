import os
import re
from datetime import date, datetime
from typing import Optional, Tuple, cast
from urllib.parse import urlparse

from loguru import logger
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.custom_types import (
    ClientWithQuestionnaires,
    Config,
    Questionnaire,
)
from utils.database import update_questionnaires_in_db
from utils.selenium import (
    save_screenshot_to_path,
    wait_for_page_load,
    wait_for_url_stability,
)


def all_questionnaires_done(client: ClientWithQuestionnaires) -> bool:
    """Check if all questionnaires for the given client are completed.

    Args:
        client (ClientWithQuestionnaires): The client to check.

    Returns:
        bool: True if all questionnaires are completed, False otherwise.
    """
    return all(
        q["status"] == "COMPLETED" for q in client.questionnaires if isinstance(q, dict)
    )


def filter_inactive_and_not_pending(
    clients: dict[int, ClientWithQuestionnaires],
) -> dict[int, ClientWithQuestionnaires]:
    """Filters clients that are not active and have no pending questionnaires."""
    filtered_clients = {
        client.id: client
        for client in clients.values()
        if client.status is True
        and any(
            q.get("status")
            in [
                "PENDING",
                # "SPANISH",
                "POSTEVAL_PENDING",
                "IGNORING",
                "RESCHEDULED",
            ]
            for q in client.questionnaires
            if isinstance(q, dict)
        )
    }
    return filtered_clients


def check_if_ignoring(client: ClientWithQuestionnaires) -> bool:
    """Check if any questionnaire for the given client is being ignored."""
    return any(
        q["status"] == "RESCHEDULED" or q["status"] == "IGNORING"
        for q in client.questionnaires
        if isinstance(q, dict)
    )


def get_id_from_path(path: str, max_length: int = 50) -> str:
    """Extracts a sanitized identifier from a URL path."""
    clean_path = path.split("?")[0].strip("/")
    sanitized = re.sub(r"[^\w\-]+", "_", clean_path)
    return sanitized[:max_length].strip("_")


def generate_screenshot_filename(
    status: str, q_type: str, host: str, unique_id: str
) -> str:
    """Creates a filename for a screenshot based on information about the questionnaire."""
    safe_type = "".join(c for c in q_type if c.isalnum() or c in ("_", "-"))
    safe_host = host.replace(".", "_").replace(":", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{status.upper()}_{safe_type}_{safe_host}_{unique_id}_{timestamp}.png"


def check_q_done(driver: WebDriver, q_link: str, q_type: str) -> bool:
    """Checks questionnaire completion status and captures evidence."""
    url_patterns = {
        "ASRS (2-5 Years)": "/asrs_web/",
        "ASRS (6-18 Years)": "/asrs_web/",
        "Conners EC": "/CEC/",
        "Conners 4": "/conners4/",
        "DP-4": "respondent.wpspublish.com",
    }

    raw_completion_texts = {
        "mhs.com": [
            "Thank you for completing",
            "Gracias por contestar",
            "This link has already been used",
            "We have received your answers",
            "Hemos recibido sus respuestas",
        ],
        "pearsonassessments.com": [
            "Test Completed!",
            "¡Prueba completada!",
        ],
        "wpspublish.com": [
            "This assessment is not available at this time",
            "Esta evaluación no está disponible en este momento",
        ],
    }

    completion_xpaths = {
        host: " | ".join(f"//*[contains(text(), '{text}')]" for text in texts)
        for host, texts in raw_completion_texts.items()
    }

    wait = WebDriverWait(driver, 15)

    final_url = ""
    parsed_url = urlparse(q_link)
    link_host = parsed_url.netloc
    unique_id = get_id_from_path(parsed_url.path)

    def capture_outcome(status: str):
        filename = generate_screenshot_filename(status, q_type, link_host, unique_id)
        save_screenshot_to_path(driver, os.path.join("logs/screenshots", filename))

    try:
        driver.get(q_link)
        final_url = wait_for_url_stability(driver)

        if not wait_for_page_load(driver):
            return False

        if q_type in url_patterns:
            expected_pattern = url_patterns[q_type]

            if expected_pattern not in final_url:
                error_msg = f"URL mismatch: Expected '{expected_pattern}' in URL for type '{q_type}', but got '{final_url}'"
                raise Exception(error_msg)

        for host_key, xpath in completion_xpaths.items():
            if host_key in link_host:
                logger.info(f"Checking {host_key} completion for {q_link}")
                wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                logger.info(f"Completion found for {q_link}: {xpath}")
                capture_outcome("COMPLETED")
                return True

        logger.warning(f"Unknown or unsupported questionnaire host in link: {q_link}")
        capture_outcome("UNKNOWN_HOST")
        return False

    except (TimeoutException, NoSuchElementException):
        logger.info(
            f"Questionnaire at {q_link} is likely not completed (Timeout waiting for completion message)."
        )
        capture_outcome("INCOMPLETE")
        return False

    except WebDriverException:
        logger.exception(f"WebDriver error checking questionnaire at {q_link}")
        capture_outcome("WEBDRIVER_ERROR")
        return False

    except Exception:
        logger.exception(f"{q_link}")
        capture_outcome("UNKNOWN_ERROR")
        raise


def check_questionnaires(
    driver: WebDriver,
    config: Config,
    clients: dict[int, ClientWithQuestionnaires],
) -> Tuple[
    list[ClientWithQuestionnaires],
    list[str],
]:
    """Check if all questionnaires for the given clients are completed. This function will navigate to each questionnaire link and look for specific text on the page based on the URL.

    Args:
        driver (WebDriver): The Selenium WebDriver instance used for browser automation.
        config (Config): The configuration object.
        clients (dict[int, ClientWithQuestionnaires]): A dictionary of clients with their IDs as keys and ClientWithQuestionnaires objects as values.

    Returns:
        list[ClientWithQuestionnaires]: A list of clients whose questionnaires are all completed.
    """
    if not clients:
        return [], []
    completed_clients = []
    updated_clients = []
    error_clients: list[str] = []
    for id in clients:
        client = clients[id]
        if all_questionnaires_done(client):
            logger.info(f"{client.fullName} has already completed their questionnaires")
            continue
        client_updated = False
        try:
            for questionnaire in client.questionnaires:
                if questionnaire["status"] == "COMPLETED":
                    logger.info(
                        f"{client.fullName}'s {questionnaire['questionnaireType']} is already done"
                    )
                    continue
                if not questionnaire["link"]:
                    logger.warning(
                        f"No link found for {client.fullName}'s {questionnaire['questionnaireType']}"
                    )
                    continue
                logger.info(
                    f"Checking {client.fullName}'s {questionnaire['questionnaireType']}"
                )
                if check_q_done(
                    driver, questionnaire["link"], questionnaire["questionnaireType"]
                ):
                    questionnaire["status"] = "COMPLETED"
                    logger.info(
                        f"{client.fullName}'s {questionnaire['questionnaireType']} is {questionnaire['status']}"
                    )
                    client_updated = True
                else:
                    logger.warning(
                        f"{client.fullName}'s {questionnaire['questionnaireType']} is {questionnaire['status']}"
                    )

            if client_updated:
                updated_clients.append(client)

            if all_questionnaires_done(client):
                completed_clients.append(client)

        except Exception as e:
            logger.error(f"Error checking questionnaires for {client.fullName}: {e}")
            error_clients.append(f"{client.fullName}: {e}")

    if updated_clients:
        update_questionnaires_in_db(config, updated_clients)
    return completed_clients, error_clients


def get_most_recent_not_done(
    client: ClientWithQuestionnaires,
) -> Optional[Questionnaire]:
    """Get the most recent questionnaire that is still PENDING, POSTEVAL_PENDING or SPANISH from the given client by taking max of q["sent"].

    Args:
        client (ClientWithQuestionnaires): The client with questionnaires to check.

    Returns:
        Questionnaire: The most recent questionnaire that is still PENDING, POSTEVAL_PENDING or SPANISH.
    """
    pending_and_sent = (
        q
        for q in client.questionnaires
        if (
            q["status"] == "PENDING" or q["status"] == "POSTEVAL_PENDING"
            # or q["status"] == "SPANISH"
        )
        and q["sent"] is not None
    )

    return max(pending_and_sent, key=lambda q: cast(date, q["sent"]), default=None)


def get_reminded_ever(client: ClientWithQuestionnaires) -> bool:
    """Check if the client has ever been reminded of a questionnaire.

    Args:
        client (ClientWithQuestionnaires): The client with questionnaires to check.

    Returns:
        bool: True if the client has ever been reminded of a questionnaire, False otherwise.
    """
    return any(
        q["reminded"] != 0
        and (
            q["status"] == "PENDING" or q["status"] == "POSTEVAL_PENDING"
            # or q["status"] == "SPANISH"
        )
        for q in client.questionnaires
        if isinstance(q, dict)
    )
