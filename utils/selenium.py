import os
import time
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
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.types import Services


def initialize_selenium() -> tuple[WebDriver, ActionChains]:
    """Initialize a Selenium WebDriver with the given options.

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


def find_element(
    driver: WebDriver,
    by: str,
    locator: str,
    timeout: int = 5,
    condition=EC.presence_of_element_located,
) -> WebElement:
    """Find a web element using an explicit wait."""
    try:
        element = WebDriverWait(driver, timeout).until(condition((by, locator)))
        return element
    except TimeoutException as e:
        logger.warning(
            f"Timeout ({timeout}s) waiting for element with {by}='{locator}'."
        )
        raise e


def find_element_exists(
    driver: WebDriver,
    by: str,
    locator: str,
    timeout: int = 5,
) -> bool:
    """Check if a web element exists using an explicit wait."""
    try:
        find_element(driver, by, locator, timeout)
        return True
    except (NoSuchElementException, TimeoutException):
        return False


def click_element(
    driver: WebDriver,
    by: str,
    locator: str,
    max_attempts: int = 3,
    timeout: int = 5,
    refresh: bool = False,
) -> None:
    """Click on a web element located by the specified method within the given attempts."""
    for attempt in range(max_attempts):
        try:
            element = find_element(
                driver, by, locator, timeout, condition=EC.element_to_be_clickable
            )
            element.click()
            return
        except StaleElementReferenceException:
            f"Attempt {attempt + 1}/{max_attempts} failed: Stale element. Retrying..."
            if refresh:
                logger.info("Refreshing page")
                driver.refresh()
                sleep(1)
        except (NoSuchElementException, TimeoutException) as e:
            if attempt == max_attempts - 1:
                raise e
            else:
                logger.warning(f"Click element failed: trying again after 1s.")
                sleep(1)


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


def wait_for_url_stability(
    driver: WebDriver, timeout: int = 10, check_interval: int = 1
) -> str:
    """Wait for the URL to stabilize (stop redirecting).

    Args:
        driver: The WebDriver instance.
        timeout: Maximum time to wait for stability.
        check_interval: Time between URL checks.

    Returns:
        The final stable URL.
    """
    end_time = time.time() + timeout
    previous_url = driver.current_url

    while time.time() < end_time:
        time.sleep(check_interval)
        current_url = driver.current_url

        if current_url == previous_url:
            # URL hasn't changed, wait one more interval to confirm
            time.sleep(check_interval)
            if driver.current_url == current_url:
                return current_url

        previous_url = current_url

    # Timeout reached, return current URL
    return driver.current_url


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
        except Exception:
            if attempt == 2:
                logger.exception(f"Failed to go to client after 3 attempts")
                return
            else:
                logger.exception(f"Failed to go to client, trying again")
                driver.refresh()
                sleep(1)
    return


def check_if_opened_portal(driver: WebDriver) -> bool:
    """Check if the TA portal has been opened by the client."""
    try:
        find_element(
            driver, By.XPATH, "//div[contains(normalize-space(text()), 'Username:')]", 3
        )
        return True
    except (NoSuchElementException, TimeoutException):
        return False


def check_if_docs_signed(driver: WebDriver) -> bool:
    """Check if the TA docs have been signed by the client."""
    try:
        find_element(
            driver,
            By.XPATH,
            "//div[contains(normalize-space(text()), 'has completed registration')]",
            3,
        )
        return True
    except (NoSuchElementException, TimeoutException):
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
