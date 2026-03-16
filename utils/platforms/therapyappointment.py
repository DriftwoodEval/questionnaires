import re
import sys
from datetime import date, datetime
from time import sleep, strftime, strptime

import pandas as pd
import typer
from dateutil.relativedelta import relativedelta
from loguru import logger
from rich import print
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import Select

from utils.custom_types import ClientFromDB, Config, FailedClientFromDB, Services
from utils.database import (
    get_clients_needing_records,
    get_previous_clients,
    get_record_ready_client_ids,
    insert_basic_client,
    put_questionnaire_in_db,
    update_failure_in_db,
    update_questionnaire_in_db,
)
from utils.google import get_punch_list, update_punch_list
from utils.misc import NetworkSink, add_failure, load_config, load_local_settings
from utils.selenium import (
    click_element,
    find_element,
    initialize_selenium,
)


def login_ta(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    admin: bool = False,
) -> None:
    """Log in to TherapyAppointment.

    Args:
        driver (WebDriver): The Selenium WebDriver instance used for browser automation.
        actions (ActionChains): The ActionChains instance used for simulating user actions.
        services (Services): The configuration object containing the TherapyAppointment credentials.
        admin (bool, optional): Whether to log in as an admin user. Defaults to False.
    """
    logger.debug("Entering username")
    username_field = find_element(driver, By.NAME, "user_username")
    username_field.send_keys(
        services.therapyappointment.admin_username
        if admin
        else services.therapyappointment.username
    )

    logger.debug("Entering password")
    password_field = find_element(driver, By.NAME, "user_password")
    password_field.send_keys(
        services.therapyappointment.admin_password
        if admin
        else services.therapyappointment.password
    )

    logger.debug("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def check_and_login_ta(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    first_time: bool = False,
    admin: bool = False,
) -> None:
    """Check if logged in to TherapyAppointment and log in if not."""
    ta_url = "https://portal.therapyappointment.com"
    if first_time:
        logger.debug("First time login to TherapyAppointment, logging in now.")
        driver.get(ta_url)
        login_ta(driver, actions, services, admin)
        return
    try:
        logger.debug("Checking if logged in to TherapyAppointment")
        driver.get(ta_url)
        find_element(driver, By.XPATH, "//*[contains(text(), 'Clients')]", timeout=2)
        logger.debug("Already logged in to TherapyAppointment")
    except (NoSuchElementException, TimeoutException):
        logger.debug("Not logged in to TherapyAppointment, logging in now.")
        login_ta(driver, actions, services, admin)


def go_to_client(
    driver: WebDriver, actions: ActionChains, services: Services, client_id: str
) -> str | None:
    """Navigates to the given client in TA and returns the client's URL."""

    def _search_clients(
        driver: WebDriver, actions: ActionChains, client_id: str
    ) -> None:
        logger.info(f"Searching for {client_id} on TA")
        sleep(2)

        logger.debug("Trying to escape random popups")
        actions.send_keys(Keys.ESCAPE)
        actions.perform()

        logger.debug("Entering client ID")
        client_id_label = find_element(
            driver, By.XPATH, "//label[text()='Account Number']"
        )
        client_id_field = client_id_label.find_element(
            By.XPATH, "./following-sibling::input"
        )
        client_id_field.send_keys(client_id)

        logger.debug("Clicking search")
        click_element(driver, By.CSS_SELECTOR, "button[aria-label='Search'")

    def _go_to_client_loop(
        driver: WebDriver, actions: ActionChains, services: Services, client_id: str
    ) -> str:
        check_and_login_ta(driver, actions, services)
        sleep(1)
        logger.debug("Navigating to Clients section")
        click_element(driver, By.XPATH, "//*[contains(text(), 'Clients')]")

        for attempt in range(3):
            try:
                _search_clients(driver, actions, client_id)
                break
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Failed to search after 3 attempts: {e}")
                    raise e
                else:
                    logger.warning(f"Failed to search: {e}, trying again")
                    driver.refresh()

        sleep(1)

        logger.debug("Selecting client profile")

        click_element(
            driver,
            By.CSS_SELECTOR,
            "a[aria-description*='Press Enter to view the profile of",
            max_attempts=1,
        )

        current_url = driver.current_url
        logger.success(f"Navigated to client profile: {current_url}")
        return current_url

    for attempt in range(3):
        try:
            return _go_to_client_loop(driver, actions, services, client_id)
        except Exception as e:
            if attempt == 2:
                logger.error(f"Failed to go to client after 3 attempts: {e}")
                return
            else:
                logger.error(f"Failed to go to client, trying again: {e}")
    return


def check_if_opened_portal(driver: WebDriver) -> bool:
    """Check if the TA portal has been opened by the client."""
    logger.info("Checking if portal has been opened...")
    try:
        xpath = "//*[contains(normalize-space(.), 'Send Portal Invitation') or contains(normalize-space(.), 'Resend Portal Invitation') or contains(normalize-space(.), 'Username:')]"
        element = find_element(driver, By.XPATH, xpath, 3)
        element_text = element.text
        if (
            "Send Portal Invitation" in element_text
            or "Resend Portal Invitation" in element_text
        ):
            return False
        elif "Username:" in element_text:
            return True
        else:  # Unknown element
            return False
    except TimeoutException:
        return False


def check_if_docs_signed(driver: WebDriver) -> bool:
    """Check if the TA docs have been signed by the client."""
    logger.info("Checking if docs have been signed...")
    try:
        xpath = "//div[contains(normalize-space(.), 'has completed registration') or contains(normalize-space(.), 'has not completed registration')]"
        element = find_element(driver, By.XPATH, xpath, 3)
        element_text = element.text
        if "has completed registration" in element_text:
            return True
        else:
            return False
    except TimeoutException:
        return False


def resend_portal_invite(
    driver: WebDriver, actions: ActionChains, services: Services, client_id: str
) -> None:
    """Resend the TA portal invite to the client."""
    go_to_client(driver, actions, services, client_id)
    try:
        click_element(
            driver,
            By.XPATH,
            "//span[contains(normalize-space(text()), 'Resend Portal Invitation')]",
        )
    except Exception:
        raise


def send_message_ta(
    driver: WebDriver,
    client_url: str,
    message: str,
    subject: str = "Please complete the link(s) below. Thank you.",
) -> None:
    """Sends a message in TherapyAppointment to the client.

    Args:
        driver (WebDriver): The Selenium WebDriver instance used for browser automation.
        client_url (str): The URL of the client's profile page in TherapyAppointment.
        message (str): The message to be sent to the client, formatted as a string with newlines.
        subject (str, optional): The subject of the message. Defaults to "Please complete the link(s) below. Thank you."
    """
    logger.info("Navigating to client URL")
    driver.get(client_url)

    logger.debug("Accessing Messages section")
    click_element(
        driver, By.XPATH, "//a[contains(normalize-space(text()), 'Messages')]"
    )

    logger.debug("Initiating new message")
    click_element(
        driver,
        By.XPATH,
        "//div[2]/section/div/a/span/span",
    )
    sleep(1)

    logger.debug("Setting message subject")
    find_element(driver, By.ID, "message_thread_subject").send_keys(subject)
    sleep(1)

    logger.debug("Entering message content")
    text_field = find_element(driver, By.XPATH, "//section/div/div[3]")
    text_field.click()
    sleep(1)
    text_field.send_keys(message)
    sleep(1)

    text_field.click()
    click_element(driver, By.CSS_SELECTOR, "button[type='submit']")
    logger.success("Submitted TA message")
