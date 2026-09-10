import contextlib
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

from utils.constants import BUSINESS_TIMEZONE
from utils.custom_types import Services
from utils.selenium import (
    click_element,
    find_element,
)
from utils.timezone import business_to_utc


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
    r"Completed\s+on\s+(\d{1,2}/\d{1,2}/\d{2,4})\s+at\s+(\d{1,2}:\d{2}\s*[AP]M)"
)
# Assigned cell reads e.g. "02/25/2026 11:31 AM\n for Shannon Tapp, LPES".
_ASSIGNED_DATE_RE = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})\s+(\d{1,2}:\d{2}\s*[AP]M)")


def _business_ts_to_utc(date_str: str, time_str: str) -> datetime | None:
    """Parse a TA-rendered business-local "M/D/Y H:MM AM" into a naive UTC datetime.

    TherapyAppointment renders these timestamps in business-local time;
    session_started_at (from the DB) is a true UTC instant, so normalize to UTC.
    """
    year_fmt = "%y" if len(date_str.rsplit("/", maxsplit=1)[-1]) == 2 else "%Y"
    try:
        naive_business = datetime.strptime(
            f"{date_str} {time_str}", f"%m/%d/{year_fmt} %I:%M %p"
        )
    except ValueError:
        return None
    return business_to_utc(naive_business, BUSINESS_TIMEZONE).replace(tzinfo=None)


def find_form_link_for_session(
    driver: WebDriver, link_text: str, session_started_at: datetime | None
) -> WebElement:
    """Find the newest completed Docs & Forms link matching `link_text`.

    Clients can go through more than one session over time, and TA keeps every
    prior copy of a form (e.g. "Receiving Consent to Release of Information") in
    the same Docs & Forms list. We always want the newest copy, ordered by its
    Assigned date. If that newest copy has not been completed yet - even when an
    older copy has - the client still owes us a signature, so raise and let the
    caller treat it as "docs not signed" rather than acting on a stale form.
    A not-yet-started copy has no link, so match on the form-name cell too.
    """
    rows = driver.find_elements(
        By.XPATH,
        "//tr["
        f"td/a[normalize-space(text())='{link_text}'] or "
        f"td[@aria-label='Assigned online form name' and normalize-space(text())='{link_text}']"
        "]",
    )
    if not rows:
        raise NoSuchElementException(f"No form found with link text: {link_text}")

    # (assigned_at, completed_at | None, row) for each readable copy.
    dated_rows: list[tuple[datetime, datetime | None, WebElement]] = []
    for row in rows:
        try:
            assigned_cell = row.find_element(
                By.XPATH, ".//td[@aria-label='Assigned online form date']"
            )
        except NoSuchElementException:
            continue
        assigned_match = _ASSIGNED_DATE_RE.search(assigned_cell.text)
        if not assigned_match:
            logger.warning(
                f"Unparseable Assigned date for '{link_text}': {assigned_cell.text!r}"
            )
            continue
        assigned_at = _business_ts_to_utc(*assigned_match.groups())
        if assigned_at is None:
            continue

        status_text = ""
        with contextlib.suppress(NoSuchElementException):
            status_text = row.find_element(By.XPATH, ".//td[@aria-label='Status']").text
        status_match = _COMPLETED_STATUS_RE.search(status_text)
        completed_at = (
            _business_ts_to_utc(*status_match.groups()) if status_match else None
        )
        dated_rows.append((assigned_at, completed_at, row))

    if not dated_rows:
        raise NoSuchElementException(
            f"No '{link_text}' form with a readable Assigned date found"
        )

    newest_assigned_at = max(assigned_at for assigned_at, _, _ in dated_rows)
    newest_batch = [
        (completed_at, row)
        for assigned_at, completed_at, row in dated_rows
        if assigned_at == newest_assigned_at
    ]
    if any(completed_at is None for completed_at, _ in newest_batch):
        raise NoSuchElementException(
            f"Newest '{link_text}' form (assigned {newest_assigned_at} UTC) is not completed"
        )

    newest_completed_at, chosen_row = max(newest_batch, key=lambda pair: pair[0])
    if session_started_at is not None and newest_completed_at < session_started_at:
        raise NoSuchElementException(
            f"Newest '{link_text}' form completed {newest_completed_at} UTC, "
            f"before session start ({session_started_at})"
        )
    return chosen_row.find_element(By.LINK_TEXT, link_text)


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
    messages_tab_attempts = 3
    for attempt in range(messages_tab_attempts):
        try:
            click_element(
                driver,
                By.XPATH,
                "//a[@role='tab' and contains(normalize-space(.), 'Messages')]",
            )
            break
        except TimeoutException:
            if attempt == messages_tab_attempts - 1:
                raise
            # click_element already retries against the same DOM without
            # reloading, so if that's still timing out the page itself
            # likely never finished loading. Re-navigating gives it a fresh
            # chance instead of repeatedly polling a stuck page.
            logger.warning(
                f"Timed out finding Messages tab, reloading and retrying ({attempt + 1}/{messages_tab_attempts})."
            )
            driver.get(client_url)

    logger.debug("Initiating new message")
    # The page renders this link twice - once in a "visible-sm visible-xs"
    # mobile-only column and once in a "hidden-sm hidden-xs" desktop-only
    # column - both with the same href. At our 1920x1080 window size only
    # the desktop copy is actually displayed, but an unscoped locator
    # matches the mobile copy first (it comes first in the DOM), which
    # never becomes clickable and just times out.
    click_element(
        driver,
        By.XPATH,
        "//div[contains(@class, 'hidden-sm') and contains(@class, 'hidden-xs')]"
        "//a[contains(@href, 'createMessageThread')]",
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
