import os
import time
from time import sleep

from loguru import logger
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


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
    condition=EC.presence_of_element_located,
) -> bool:
    """Check if a web element exists using an explicit wait."""
    try:
        find_element(driver, by, locator, timeout, condition)
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
    scroll: bool = False,
) -> None:
    """Click on a web element located by the specified method within the given attempts."""
    for attempt in range(max_attempts):
        try:
            element = find_element(
                driver, by, locator, timeout, condition=EC.element_to_be_clickable
            )
            if scroll:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", element
                )
                sleep(0.5)
            element.click()
            return
        except StaleElementReferenceException:
            logger.warning(
                f"Attempt {attempt + 1}/{max_attempts} failed: Stale element. Retrying..."
            )
            if refresh:
                logger.info("Refreshing page")
                driver.refresh()
                sleep(1)
        except (
            NoSuchElementException,
            TimeoutException,
            ElementClickInterceptedException,
        ) as e:
            if attempt == max_attempts - 1:
                raise e
            else:
                logger.warning(
                    f"Click element failed ({type(e).__name__}): trying again after 1s."
                )
                sleep(1)


def wait_for_page_load(driver: WebDriver, timeout: int = 15) -> bool:
    """Waits for the page to reach 'complete' readyState."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda driver: (
                driver.execute_script("return document.readyState") == "complete"
            )
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


def save_screenshot_to_path(driver: WebDriver, filepath: str) -> None:
    """Save a screenshot of the current page to the specified path."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        driver.save_screenshot(filepath)
        logger.info(f"Screenshot saved to {filepath}")
    except Exception as e:
        logger.error(f"Failed to save screenshot: {e}")


def set_local_storage_item(driver: WebDriver, key: str, value: str) -> None:
    """Set an item in localStorage using JavaScript."""
    driver.execute_script(
        "window.localStorage.setItem(arguments[0], arguments[1]);", key, value
    )
    logger.debug(f"Set localStorage item: {key}")
