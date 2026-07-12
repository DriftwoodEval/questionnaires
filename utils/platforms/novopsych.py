from loguru import logger
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver

from utils.custom_types import Services
from utils.selenium import find_element, wait_for_page_load

NOVOPSYCH_URL = "https://app.novopsych.com"
NOVOPSYCH_LOGIN_URL = f"{NOVOPSYCH_URL}/login"


def login_novopsych(driver: WebDriver, services: Services) -> None:
    driver.get(NOVOPSYCH_LOGIN_URL)
    wait_for_page_load(driver)

    email_field = find_element(
        driver, By.CSS_SELECTOR, "input[name='email']", timeout=10
    )
    email_field.send_keys(services.novopsych.username)

    password_field = find_element(driver, By.CSS_SELECTOR, "input[name='password']")
    password_field.send_keys(services.novopsych.password)
    password_field.send_keys(Keys.ENTER)
    wait_for_page_load(driver)


def check_and_login_novopsych(
    driver: WebDriver,
    services: Services,
    first_time: bool = False,
) -> None:
    """Check if logged in to NovoPsych and log in if not."""
    if first_time:
        logger.debug("First time login to NovoPsych, logging in now.")
        login_novopsych(driver, services)
        return
    try:
        logger.debug("Checking if logged in to NovoPsych")
        driver.get(NOVOPSYCH_URL)
        find_element(
            driver,
            By.XPATH,
            "//h4[normalize-space()='Recent Activity']",
            timeout=2,
        )
        logger.debug("Already logged in to NovoPsych")
    except (NoSuchElementException, TimeoutException):
        logger.debug("Not logged in to NovoPsych, logging in now.")
        login_novopsych(driver, services)


def check_novopsych_completed(
    driver: WebDriver,
    services: Services,
    first_name: str,
    last_name: str,
) -> bool:
    """Check NovoPsych Recent Activity for a CAT-Q completion by this client.

    Returns True if a matching completed CAT-Q entry is found.
    """
    full_name = f"{first_name} {last_name}"
    logger.info(f"NovoPsych check: looking for '{full_name}' CAT-Q completion")

    try:
        check_and_login_novopsych(driver, services)

        find_element(
            driver,
            By.XPATH,
            "//div[contains(@class,'col-md-6')]//h4[normalize-space()='Recent Activity']",
            timeout=10,
        )

        # The activity list uses Ionic components; match by normalized name + CAT-Q span
        xpath = (
            "//div[contains(@class,'activity')]"
            "//ion-label["
            "normalize-space(strong)='" + full_name + "'"
            " and contains(span,'CAT-Q')]"
        )
        find_element(driver, By.XPATH, xpath, timeout=5)
        logger.info(f"Found '{full_name}' CAT-Q completion on NovoPsych")
        return True

    except (NoSuchElementException, TimeoutException):
        logger.info(f"Did not find '{full_name}' CAT-Q completion on NovoPsych")
        return False
    except Exception:
        logger.exception(f"Error during NovoPsych check for '{full_name}'")
        return False
