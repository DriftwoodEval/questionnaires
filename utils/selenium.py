import os
from time import sleep

from loguru import logger
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait

from utils.types import Services


def initialize_selenium(save_profile: bool = False) -> tuple[WebDriver, ActionChains]:
    """Initialize a Selenium WebDriver with the given options.

    Args:
        save_profile (bool, optional): If true, save the browser profile to the
            `./config/chrome_profile` directory. Defaults to False.

    Returns:
        tuple[WebDriver, ActionChains]: A tuple containing the initialized WebDriver
        and ActionChains instances.
    """
    logger.info("Initializing Selenium")
    chrome_options: Options = Options()
    chrome_options.add_argument("--no-sandbox")
    if os.getenv("HEADLESS") == "true":
        chrome_options.add_argument("--headless")
    # /dev/shm partition can be too small in VMs, causing Chrome to crash, make a temp dir instead
    chrome_options.add_argument("--disable-dev-shm-usage")
    if save_profile:
        chrome_options.add_argument("--user-data-dir=./config/chrome_profile")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": f"{os.getcwd()}/put/downloads",
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    driver = webdriver.Chrome(options=chrome_options)
    actions = ActionChains(driver)
    driver.implicitly_wait(5)
    driver.set_window_size(1920, 1080)
    return driver, actions


def click_element(
    driver: WebDriver,
    by: str,
    locator: str,
    max_attempts: int = 3,
    delay: int = 1,
    refresh: bool = False,
) -> None:
    """Click on a web element located by the specified method within the given attempts.

    Raises:
        NoSuchElementException: If the element is not found after the specified
            number of attempts.
    """
    for attempt in range(max_attempts):
        try:
            element = driver.find_element(by, locator)
            element.click()
            return
        except (StaleElementReferenceException, NoSuchElementException) as e:
            f"Attempt {attempt + 1}/{max_attempts} failed: {type(e).__name__}. Retrying in {delay} seconds..."
            sleep(delay)
            if refresh:
                logger.info("Refreshing page")
                driver.refresh()
                sleep(delay)
    raise NoSuchElementException(f"Element not found after {max_attempts} attempts")


def find_element(
    driver: WebDriver,
    by: str,
    locator: str,
    max_attempts: int = 3,
    delay: int = 1,
) -> WebElement:
    """Find a web element with retries.

    Raises:
        NoSuchElementException: If the element is not found.
    """
    for attempt in range(max_attempts):
        try:
            element = driver.find_element(by, locator)
            return element
        except (StaleElementReferenceException, NoSuchElementException) as e:
            logger.warning(
                f"Attempt {attempt + 1}/{max_attempts} failed: {type(e).__name__}. Retrying in {delay} seconds..."
            )
            sleep(delay)

    raise NoSuchElementException(f"Element not found after {max_attempts} attempts")


def find_element_exists(
    driver: WebDriver,
    by: str,
    locator: str,
    max_attempts: int = 3,
    delay: int = 1,
) -> bool:
    """Check if a web element exists with retries.

    Returns:
        bool: True if the element is found, False otherwise.
    """
    try:
        find_element(driver, by, locator, max_attempts, delay)
        return True
    except NoSuchElementException:
        return False


def wait_for_page_load(driver: WebDriver, timeout: int = 15) -> bool:
    """Waits for the page to reach 'complete' readyState."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda driver: driver.execute_script("return document.readyState")
            == "complete"
        )
        return True
    except TimeoutException:
        logger.warning("Timeout waiting for document.readyState == 'complete'.")
        return False


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
    logger.info("Logging in to TherapyAppointment")

    logger.debug("Going to login page")
    driver.get("https://portal.therapyappointment.com")

    logger.debug("Entering username")
    username_field = find_element(driver, By.NAME, "user_username")
    username_field.send_keys(
        services["therapyappointment"]["admin_username" if admin else "username"]
    )

    logger.debug("Entering password")
    password_field = find_element(driver, By.NAME, "user_password")
    password_field.send_keys(
        services["therapyappointment"]["admin_password" if admin else "password"]
    )

    logger.debug("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def go_to_client(
    driver: WebDriver, actions: ActionChains, client_id: str
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
        driver: WebDriver, actions: ActionChains, client_id: str
    ) -> str:
        driver.get("https://portal.therapyappointment.com")
        sleep(1)
        logger.debug("Navigating to Clients section")
        click_element(driver, By.XPATH, "//*[contains(text(), 'Clients')]")

        for attempt in range(3):
            try:
                _search_clients(driver, actions, client_id)
                break
            except Exception as e:
                if attempt == 2:
                    logger.exception(f"Failed to search after 3 attempts: {e}")
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
            return _go_to_client_loop(driver, actions, client_id)
        except Exception as e:
            if attempt == 2:
                logger.exception(f"Failed to go to client after 3 attempts: {e}")
                return
            else:
                logger.warning(f"Failed to go to client: {e}, trying again")
                driver.refresh()
    return


def check_if_opened_portal(driver: WebDriver) -> bool:
    """Check if the TA portal has been opened by the client."""
    try:
        find_element(driver, By.CSS_SELECTOR, "input[aria-checked='true']")
        return True
    except NoSuchElementException:
        return False


def check_if_docs_signed(driver: WebDriver) -> bool:
    """Check if the TA docs have been signed by the client."""
    try:
        find_element(
            driver,
            By.XPATH,
            "//div[contains(normalize-space(text()), 'has completed registration')]",
        )
        return True
    except NoSuchElementException:
        return False


def resend_portal_invite(
    driver: WebDriver, actions: ActionChains, client_id: str
) -> None:
    """Resend the TA portal invite to the client."""
    go_to_client(driver, actions, client_id)
    click_element(
        driver,
        By.XPATH,
        "//span[contains(normalize-space(text()), 'Resend Portal Invitation')]",
    )
