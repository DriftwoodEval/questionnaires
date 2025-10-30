from typing import Tuple
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

from utils.database import update_questionnaires_in_db
from utils.selenium import wait_for_page_load
from utils.types import (
    ClientWithQuestionnaires,
    Config,
    Questionnaire,
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
            q.get("status") in ["PENDING", "IGNORING", "RESCHEDULED"]
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


def check_q_done(driver: WebDriver, q_link: str, q_type: str) -> bool:
    """Check if a questionnaire linked by `q_link` is completed.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
        q_link (str): The URL of the questionnaire.
        q_type (str): The type of the questionnaire.

    Returns:
        bool: True if the questionnaire is completed, False otherwise.

    Raises:
        Exception: If the questionnaire type does not match the URL's expected pattern.
    """
    url_patterns = {
        "ASRS (2-5 Years)": "/asrs_web/",
        "ASRS (6-18 Years)": "/asrs_web/",
        "Conners EC": "/CEC/",
        "Conners 4": "/conners4/",
        "DP-4": "respondent.wpspublish.com",
    }

    completion_criteria = {
        "mhs.com": "//*[contains(text(), 'Thank you for completing')] | "
        "//*[contains(text(), 'This link has already been used')] | "
        "//*[contains(text(), 'We have received your answers')]",
        "pearsonassessments.com": "//*[contains(text(), 'Test Completed!')]",
        "wpspublish.com": "//*[contains(text(), 'This assessment is not available at this time')]",
    }

    wait = WebDriverWait(driver, 15)

    try:
        driver.get(q_link)
        if not wait_for_page_load(driver):
            return False
        current_url = driver.current_url
        # logger.debug(f"Current URL: {current_url}")

        if q_type in url_patterns:
            expected_pattern = url_patterns[q_type]

            if expected_pattern not in current_url:
                error_msg = f"URL mismatch: Expected '{expected_pattern}' in URL for type '{q_type}', but got '{current_url}'"
                raise Exception(error_msg)
            # logger.debug(f"URL validation passed for type '{q_type}'")

        link_host = urlparse(q_link).netloc

        for host_substring, xpath in completion_criteria.items():
            if host_substring in link_host:
                logger.info(f"Checking {host_substring} completion for {q_link}")
                wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                logger.info(f"Found completion criteria for {q_link}")
                return True

        logger.warning(f"Unknown or unsupported questionnaire host in link: {q_link}")
        return False

    except (TimeoutException, NoSuchElementException) as e:
        logger.info(
            f"Questionnaire at {q_link} is likely not completed (Timeout waiting for completion message)."
        )
        return False

    except WebDriverException as e:
        logger.error(f"WebDriver error checking questionnaire at {q_link}: {e}")
        return False

    except Exception as e:
        logger.error(f"{q_link}: {e}")
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
                    questionnaire["status"] = "PENDING"
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


def get_most_recent_not_done(client: ClientWithQuestionnaires) -> Questionnaire:
    """Get the most recent questionnaire that is still PENDING from the given client by taking max of q["sent"].

    Args:
        client (ClientWithQuestionnaires): The client with questionnaires to check.

    Returns:
        Questionnaire: The most recent questionnaire that is still PENDING.
    """
    return max(
        (q for q in client.questionnaires if q["status"] == "PENDING"),
        key=lambda q: q["sent"],
    )


def get_reminded_ever(client: ClientWithQuestionnaires) -> bool:
    """Check if the client has ever been reminded of a questionnaire.

    Args:
        client (ClientWithQuestionnaires): The client with questionnaires to check.

    Returns:
        bool: True if the client has ever been reminded of a questionnaire, False otherwise.
    """
    return any(
        q["reminded"] != 0 and q["status"] == "PENDING"
        for q in client.questionnaires
        if isinstance(q, dict)
    )
