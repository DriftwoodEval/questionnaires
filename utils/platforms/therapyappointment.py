import re
from datetime import datetime
from time import sleep

from loguru import logger
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

from utils.custom_types import Services
from utils.selenium import (
    click_element,
    find_element,
)


def login_ta(
    driver: WebDriver,
    services: Services,
    admin: bool = False,
) -> None:
    """Log in to TherapyAppointment."""
    actions = ActionChains(driver)
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
    services: Services,
    first_time: bool = False,
    admin: bool = False,
) -> None:
    """Check if logged in to TherapyAppointment and log in if not."""
    ta_url = "https://portal.therapyappointment.com"
    if first_time:
        logger.debug("First time login to TherapyAppointment, logging in now.")
        driver.get(ta_url)
        login_ta(driver, services, admin)
        return
    try:
        logger.debug("Checking if logged in to TherapyAppointment")
        driver.get(ta_url)
        find_element(driver, By.XPATH, "//*[contains(text(), 'Clients')]", timeout=2)
        logger.debug("Already logged in to TherapyAppointment")
    except (NoSuchElementException, TimeoutException):
        logger.debug("Not logged in to TherapyAppointment, logging in now.")
        login_ta(driver, services, admin)


def go_to_client(driver: WebDriver, services: Services, client_id: str) -> str | None:
    """Navigates to the given client in TA and returns the client's URL."""
    # Callers pass the bare numeric client ID (or, from qsend, an already
    # formatted "Human Friendly ID"). Normalize to TA's own display format
    # ("C" + zero-padded 9 digits) so it can be matched exactly against the
    # Account Number cell text below.
    client_id = f"C{re.sub(r'\\D', '', client_id).zfill(9)}"

    def _search_clients(driver: WebDriver, client_id: str) -> None:
        actions = ActionChains(driver)
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
        click_element(driver, By.CSS_SELECTOR, "button[aria-label='Search']")

    def _go_to_client_loop(
        driver: WebDriver, services: Services, client_id: str
    ) -> str:
        check_and_login_ta(driver, services)
        sleep(1)
        logger.debug("Navigating to Clients section")
        click_element(driver, By.XPATH, "//*[contains(text(), 'Clients')]")

        for attempt in range(3):
            try:
                _search_clients(driver, client_id)
                break
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Failed to search after 3 attempts: {e}")
                    raise e
                logger.warning(f"Failed to search: {e}, trying again")
                driver.refresh()

        sleep(1)

        logger.debug("Selecting client profile")

        # The client list table is present (populated with all clients) even
        # before a search runs, and Vuetify may keep already-visible rows
        # mounted while search results are still loading. Scoping to the row
        # whose Account Number cell matches client_id - rather than any row
        # with the generic "Press Enter to view the profile of" link - avoids
        # both clicking a stale/wrong row and racing the search's AJAX filter.
        click_element(
            driver,
            By.XPATH,
            f"//tr[.//td[normalize-space(text())='{client_id}']]"
            "//a[contains(@aria-description, 'Press Enter to view the profile of')]",
        )

        current_url = driver.current_url
        logger.success(f"Navigated to client profile: {current_url}")
        return current_url

    for attempt in range(3):
        try:
            return _go_to_client_loop(driver, services, client_id)
        except Exception as e:
            if attempt == 2:
                logger.error(f"Failed to go to client after 3 attempts: {e}")
                return None
            logger.error(f"Failed to go to client, trying again: {e}")
    return None


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
        return "Username:" in element_text
    except TimeoutException:
        return False


def check_if_docs_signed(driver: WebDriver) -> bool:
    """Check if the TA docs have been signed by the client."""
    logger.info("Checking if docs have been signed...")
    try:
        xpath = "//div[contains(normalize-space(.), 'has completed registration') or contains(normalize-space(.), 'has not completed registration')]"
        element = find_element(driver, By.XPATH, xpath, 3)
        if "has not completed registration" in element.text:
            return False
    except TimeoutException:
        return False

    try:
        click_element(driver, By.LINK_TEXT, "Docs & Forms")
        find_element(driver, By.XPATH, "//td[@aria-label='Status']", 10)
    except TimeoutException:
        return False

    status_cells = driver.find_elements(By.XPATH, "//td[@aria-label='Status']")
    if not status_cells:
        return False

    unsigned = [
        cell.text for cell in status_cells if not cell.text.startswith("Completed on")
    ]
    if unsigned:
        logger.info(f"Docs not fully signed. Unsigned statuses: {unsigned}")
        return False
    return True


_COMPLETED_STATUS_RE = re.compile(
    r"Completed on (\d{1,2}/\d{1,2}/\d{2,4}) at (\d{1,2}:\d{2}\s*[AP]M)"
)


def find_form_link_for_session(
    driver: WebDriver, link_text: str, session_started_at: datetime | None
) -> WebElement:
    """Find the Docs & Forms link matching `link_text`, from the current session.

    Clients can go through more than one session over time, and TA keeps every
    prior copy of a form (e.g. "Receiving Consent to Release of Information")
    in the same Docs & Forms list. Matching on link text alone can grab a
    stale form from a previous session instead of the one filled out for this
    one, so pick the form whose Status cell shows it was completed after
    session_started_at (the "Assigned" date is just when it was sent, not
    when the client actually filled it out).
    """
    rows = driver.find_elements(By.XPATH, f"//a[text()='{link_text}']/ancestor::tr[1]")
    if not rows:
        raise NoSuchElementException(f"No form found with link text: {link_text}")

    if session_started_at is None:
        return rows[0].find_element(By.LINK_TEXT, link_text)

    completed_rows = []
    for row in rows:
        try:
            status_cell = row.find_element(By.XPATH, ".//td[@aria-label='Status']")
        except NoSuchElementException:
            continue
        match = _COMPLETED_STATUS_RE.search(status_cell.text)
        if not match:
            continue
        date_str, time_str = match.groups()
        year_fmt = "%y" if len(date_str.rsplit("/", maxsplit=1)[-1]) == 2 else "%Y"
        try:
            completed_at = datetime.strptime(
                f"{date_str} {time_str}", f"%m/%d/{year_fmt} %I:%M %p"
            )
        except ValueError:
            continue
        if completed_at >= session_started_at:
            completed_rows.append((completed_at, row))

    if not completed_rows:
        raise NoSuchElementException(
            f"No completed '{link_text}' form found after session start ({session_started_at})"
        )

    completed_rows.sort(key=lambda pair: pair[0])
    latest_row = completed_rows[-1][1]
    return latest_row.find_element(By.LINK_TEXT, link_text)


def resend_portal_invite(driver: WebDriver, services: Services, client_id: str) -> None:
    """Resend the TA portal invite to the client."""
    go_to_client(driver, services, client_id)
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
