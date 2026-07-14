import contextlib
import os
import signal
import time
from collections.abc import Iterator
from pathlib import Path
from time import sleep

from loguru import logger
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    JavascriptException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.remote.remote_connection import RemoteConnection
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.exceptions import MaxRetryError
from urllib3.exceptions import TimeoutError as Urllib3TimeoutError


def initialize_selenium() -> WebDriver:
    """Initialize a Selenium WebDriver with the given options.

    Returns:
        tuple[WebDriver, ActionChains]: A tuple containing the initialized WebDriver
        and ActionChains instances.
    """
    logger.info("Initializing Selenium")
    chrome_options: Options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    if os.getenv("HEADLESS") == "true":
        chrome_options.add_argument("--headless")
    # /dev/shm partition can be too small in VMs, causing Chrome to crash, make a temp dir instead
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(Path.cwd() / "put" / "downloads"),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    # Starts chromedriver as its own process group leader (rather than
    # inheriting ours) so restart_selenium can kill the whole group -
    # chromedriver plus the Chrome browser and renderer processes it spawns
    # - in one shot instead of just chromedriver, which would otherwise
    # orphan Chrome to keep running (and accumulating across restarts).
    service = ChromeService(popen_kw={"start_new_session": True})
    driver = webdriver.Chrome(options=chrome_options, service=service)
    driver.implicitly_wait(5)
    # Selenium's HTTP connection to chromedriver has no socket timeout by
    # default. page_load_timeout below only covers commands chromedriver
    # itself recognizes as a navigation; some hangs (e.g. a click that
    # triggers an in-place AJAX update that never settles) don't, and
    # chromedriver just never sends a response. Without this, that leaves
    # our code blocked forever with no exception to catch and no chance to
    # retry.
    assert isinstance(driver.command_executor, RemoteConnection)
    assert driver.command_executor.client_config is not None
    driver.command_executor.client_config.timeout = 25
    # Selenium's default page load timeout is 300s. Some sites (e.g. QGlobal)
    # occasionally never fire the page load complete event, which would hang
    # any navigation (driver.get, or a click that triggers a full page load)
    # for the full 300s instead of failing fast so we can retry.
    driver.set_page_load_timeout(20)
    return driver


@contextlib.contextmanager
def command_timeout(driver: WebDriver, seconds: float) -> Iterator[None]:
    """Temporarily lower the socket timeout for commands sent to chromedriver.

    Useful for sites where a hang is known to surface within a few seconds
    if it's going to happen at all, so callers don't have to wait out the
    full default timeout (see initialize_selenium) before giving up.
    """
    assert isinstance(driver.command_executor, RemoteConnection)
    assert driver.command_executor.client_config is not None
    client_config = driver.command_executor.client_config
    previous = client_config.timeout
    client_config.timeout = seconds
    try:
        yield
    finally:
        client_config.timeout = previous


def restart_selenium(driver: WebDriver) -> None:
    """Recover from a wedged chromedriver session by force-killing it and
    replacing it in place with a fresh one.

    A session can get wedged badly enough that chromedriver never responds
    to *any* further command on it, not even window.stop() or driver.quit()
    - both are themselves commands sent over the same stuck channel. The
    only reliable way out is to kill the chromedriver process directly,
    bypassing the WebDriver protocol entirely, then start over. Killing
    just the chromedriver process leaves the Chrome browser it spawned
    running as an orphan (chromedriver doesn't get a chance to clean it up
    on a SIGKILL), so this kills the whole process group instead - see the
    start_new_session set up in initialize_selenium.

    `driver` is mutated in place (its __dict__ is swapped for a freshly
    initialized driver's) rather than returned, so every existing reference
    to it - held by callers throughout the codebase - keeps working against
    the new session without needing to be reassigned.
    """
    logger.warning("Restarting Selenium after a wedged session")
    service = getattr(driver, "service", None)
    process = getattr(service, "process", None)
    if process is not None:
        with contextlib.suppress(Exception):
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        with contextlib.suppress(Exception):
            # Reap it so it doesn't linger as a zombie.
            process.wait(timeout=5)
    new_driver = initialize_selenium()
    driver.__dict__.clear()  # type: ignore[attr-defined]
    driver.__dict__.update(new_driver.__dict__)  # type: ignore[attr-defined]


def find_element(
    driver: WebDriver,
    by: str,
    locator: str,
    timeout: int = 5,
    condition=ec.presence_of_element_located,
) -> WebElement:
    """Find a web element using an explicit wait."""
    try:
        # is_displayed()/is_enabled() checks (used by conditions like
        # element_to_be_clickable) run a Selenium-injected JS atom that can
        # transiently error with "this.each is not a function" while a
        # page's own scripts (e.g. the OneTrust cookie banner) are still
        # initializing. Ignoring it here just means WebDriverWait polls
        # again instead of aborting the whole wait on that one bad poll.
        return WebDriverWait(
            driver,
            timeout,
            ignored_exceptions=(NoSuchElementException, JavascriptException),
        ).until(condition((by, locator)))
    except TimeoutException as e:
        logger.warning(
            f"Timeout ({timeout}s) waiting for element with {by}='{locator}'."
        )
        raise e
    except (MaxRetryError, Urllib3TimeoutError) as e:
        # The command to chromedriver itself never got a response (see the
        # command timeout set in initialize_selenium), so WebDriverWait
        # never got the chance to time out on its own. This surfaces as
        # MaxRetryError only when urllib3 actually retries the request;
        # most WebDriver commands are POST, which urllib3's default retry
        # policy excludes, so a bare socket read timeout comes through as
        # Urllib3TimeoutError (e.g. ReadTimeoutError) instead. Treat both
        # the same as a normal timeout so existing callers don't need to
        # know about it.
        logger.warning(
            f"Chromedriver command timed out waiting for element with {by}='{locator}'."
        )
        raise TimeoutException(str(e)) from e


def find_element_exists(
    driver: WebDriver,
    by: str,
    locator: str,
    timeout: int = 5,
    condition=ec.presence_of_element_located,
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
                driver, by, locator, timeout, condition=ec.element_to_be_clickable
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
            MaxRetryError,
            Urllib3TimeoutError,
        ) as e:
            if isinstance(e, (MaxRetryError, Urllib3TimeoutError)):
                # element.click() itself can hang chromedriver (e.g. the
                # click triggers an in-place update that never settles)
                # without ever raising TimeoutException - it's the command
                # timeout set in initialize_selenium that's the only thing
                # that eventually gives up here. click() is sent as a POST,
                # which urllib3's default retry policy excludes, so the
                # timeout usually comes through as a bare Urllib3TimeoutError
                # rather than MaxRetryError (which only wraps a timeout when
                # urllib3 actually retries). Normalize both to
                # TimeoutException so callers only need to handle one type.
                e = TimeoutException(str(e))
            if isinstance(e, TimeoutException):
                # Click may have triggered a navigation that never finished
                # loading (page load timeout). Cancel it so the page isn't
                # left in a half-loaded state for the next attempt.
                driver.execute_script("window.stop();")
            if attempt == max_attempts - 1:
                raise e
            logger.warning(
                f"Click element failed ({type(e).__name__}): trying again after 1s."
            )
            sleep(1)


def get_with_retry(driver: WebDriver, url: str, retries: int = 3) -> None:
    """Navigate to a URL, retrying if the page never finishes loading.

    Relies on the driver's page load timeout (set in initialize_selenium) to
    fail fast instead of hanging on a page that never fires the load
    complete event.
    """
    for attempt in range(retries):
        try:
            driver.get(url)
            return
        except TimeoutException:
            logger.warning(f"Timed out loading {url}, attempt {attempt + 1}/{retries}.")
            driver.execute_script("window.stop();")
            if attempt == retries - 1:
                raise


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


def save_screenshot_to_path(driver: WebDriver, filepath: Path) -> None:
    """Save a screenshot of the current page to the specified path."""
    try:
        Path.mkdir(filepath.parent, exist_ok=True)
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
