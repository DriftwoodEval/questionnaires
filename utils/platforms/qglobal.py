from collections.abc import Callable
from datetime import datetime
from functools import wraps
from time import sleep
from typing import TypeVar

import pandas as pd
from loguru import logger
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import Select

from utils.custom_types import Config, Services
from utils.selenium import (
    click_element,
    find_element,
    get_with_retry,
)


def rearrange_dob(dob: str) -> str:
    """Rearrange a date of birth string from "YYYY/MM/DD" to "MM/DD/YYYY" format."""
    return datetime.strptime(dob, "%Y/%m/%d").strftime("%m/%d/%Y")


F = TypeVar("F", bound=Callable)


def with_qglobal_recovery(recover: Callable[..., None] | None = None) -> Callable[[F], F]:
    """Retry a QGlobal flow if a page load ever hangs.

    QGlobal occasionally never fires the page load complete event, which
    Selenium surfaces as a TimeoutException once the (short) page load
    timeout is hit. Recovering mid-flow isn't reliable since QGlobal's
    server-side state may be partial, so instead we stop the hung load, put
    the browser back to a known-good spot via `recover` (called with the
    same args as the wrapped function; defaults to just re-checking login),
    and retry the whole operation once.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(driver: WebDriver, *args, **kwargs):
            attempts = 2
            for attempt in range(attempts):
                try:
                    return func(driver, *args, **kwargs)
                except TimeoutException:
                    if attempt == attempts - 1:
                        raise
                    logger.warning(
                        f"QGlobal page load hung during {func.__name__}, "
                        "recovering and retrying."
                    )
                    driver.execute_script("window.stop();")
                    if recover is not None:
                        recover(driver, *args, **kwargs)
                    else:
                        check_and_login_qglobal(driver)
            raise AssertionError("unreachable")

        return wrapper  # type: ignore[return-value]

    return decorator


def login_qglobal(driver: WebDriver) -> None:
    """Wait for manual login to QGlobal."""
    logger.info("Please log in to QGlobal manually in the browser.")
    try:
        find_element(driver, By.XPATH, "//a[text()='Search']", timeout=300)
        logger.success("Detected login to QGlobal.")
    except (NoSuchElementException, TimeoutException):
        logger.error("Timed out waiting for manual login to QGlobal.")
        raise


def check_and_login_qglobal(
    driver: WebDriver,
    services: Services | None = None,  # noqa: ARG001
    first_time: bool = False,
) -> None:
    """Check if logged in to QGlobal and log in if not."""
    qglobal_url = "https://qglobal.pearsonassessments.com"
    login_url = "http://qglobal.pearsonassessments.com/qg/welcome.seam"
    if first_time:
        logger.debug("First time login to QGlobal, opening URL.")
        get_with_retry(driver, login_url)
        login_qglobal(driver)
        return
    try:
        logger.debug("Checking if logged in to QGlobal")
        get_with_retry(driver, qglobal_url)
        find_element(driver, By.XPATH, "//a[text()='Search']", timeout=5)
        logger.debug("Already logged in to QGlobal")
    except (NoSuchElementException, TimeoutException):
        logger.debug(
            "Not logged in to QGlobal or Search link not visible, waiting for manual login."
        )
        get_with_retry(driver, login_url)
        login_qglobal(driver)


def search_qglobal(driver: WebDriver, client: pd.Series) -> None:
    """Search for a client in QGlobal.

    Searches for a client using their human-friendly ID, which is the ID
    shown on TherapyAppointment, "C" + 9-digit number.
    """

    def _search_helper(driver: WebDriver, client_id: str) -> None:
        for attempt in range(3):
            logger.info(
                f"Attempting to search QGlobal for {client_id} (attempt {attempt + 1})"
            )
            try:
                sleep(1)
                find_element(driver, By.ID, "editExamineeForm:examineeId").send_keys(
                    client_id
                )
                return
            except Exception as e:
                if attempt == 2:
                    logger.error(
                        f"Failed to search QGlobal for {client_id} after 3 attempts: {e}"
                    )
                    raise e
                logger.warning(
                    f"Failed to search QGlobal for {client_id}, attempting to retry: {e}"
                )
                driver.get("https://qglobal.pearsonassessments.com")
                click_element(driver, By.XPATH, "//a[text()='Search']")

    logger.info(f"Searching QGlobal for {client['Human Friendly ID']}")
    click_element(driver, By.XPATH, "//a[text()='Search']")

    _search_helper(driver, client["Human Friendly ID"])

    logger.debug("Submitting search form")
    click_element(driver, By.ID, "editExamineeForm:search")


def search_select_qglobal(
    driver: WebDriver,
    client: pd.Series,
):
    check_and_login_qglobal(driver)
    search_qglobal(driver, client)
    sleep(3)

    try:
        logger.debug("Selecting client")
        click_element(
            driver,
            By.XPATH,
            f"//td[contains(text(), '{client['Human Friendly ID']}') and @aria-describedby='list_examineeid']",
        )
    except (NoSuchElementException, TimeoutException):
        logger.warning("Failed to select client, searching again")
        driver.refresh()
        search_qglobal(driver, client)
        sleep(3)
        try:
            logger.debug("Selecting client")
            click_element(
                driver,
                By.XPATH,
                f"//td[contains(text(), '{client['Human Friendly ID']}') and @aria-describedby='list_examineeid']",
            )
        except (NoSuchElementException, TimeoutException):
            logger.error(
                f"Failed to automatically select client {client['Human Friendly ID']}. "
                "Please navigate to the client's page manually in the browser."
            )
            input("Press Enter once you have navigated to the client's page...")


def _recover_to_search(driver: WebDriver, client: pd.Series) -> None:
    """Get back to the search results page for a client, for recovery after a hang."""
    check_and_login_qglobal(driver)
    search_qglobal(driver, client)


@with_qglobal_recovery()
def check_for_qglobal_account(driver: WebDriver, client: pd.Series) -> bool:
    """Check if a client has an account on QGlobal.

    Returns:
        bool: True if the client has an account on QGlobal, False otherwise.
    """
    check_and_login_qglobal(driver)
    search_qglobal(driver, client)

    logger.info("Checking for QGlobal account")
    try:
        find_element(
            driver,
            By.XPATH,
            f"//td[contains(text(), '{client['Human Friendly ID']}') and @aria-describedby='list_examineeid']",
        )
        return True
    except TimeoutException:
        return False


@with_qglobal_recovery(recover=_recover_to_search)
def add_client_to_qglobal(driver: WebDriver, client: pd.Series) -> bool:
    """Add a client to QGlobal if they don't already have an account.

    Returns:
        bool: True if the client was successfully added to QGlobal, False otherwise.
    """
    logger.info(
        f"Attempting to add {client['TA First Name']} {client['TA Last Name']} to QGlobal"
    )
    firstname = client["TA First Name"]
    lastname = client["TA Last Name"]
    client_id = client["Human Friendly ID"]
    dob = client["Date of Birth"]
    gender = client["Gender"]

    logger.debug("Clicking new examinee button")
    click_element(driver, By.ID, "searchForm:newExamineeButton", refresh=True)

    first = find_element(driver, By.ID, "firstName")
    last = find_element(driver, By.ID, "lastName")
    examinee_id = find_element(driver, By.ID, "examineeId")
    birth = find_element(driver, By.ID, "calendarInputDate")

    logger.debug("Entering first name")
    first.send_keys(firstname)

    logger.debug("Entering last name")
    last.send_keys(lastname)

    logger.debug("Entering examinee id")
    examinee_id.send_keys(client_id)

    logger.debug("Selecting gender")
    gender_element = find_element(driver, By.ID, "genderMenu")
    gender_select = Select(gender_element)
    sleep(1)
    if gender == "Male":
        gender_select.select_by_visible_text("Male")
    elif gender == "Female":
        gender_select.select_by_visible_text("Female")
    else:
        gender_select.select_by_visible_text("Unspecified")

    logger.debug("Entering birthdate")
    formatted_dob = rearrange_dob(dob)
    for character in formatted_dob:
        birth.send_keys(character)
        sleep(0.3)

    logger.debug("Saving new examinee")
    click_element(driver, By.ID, "save")

    logger.debug("Waiting for search page to load")
    find_element(driver, By.ID, "searchForm:newExamineeButton")
    return True


def get_qglobal_link(driver: WebDriver) -> str | None:
    """Clicks through the QGlobal UI to get the link for an assessment.

    Returns:
        str | None: The link for the assessment, or None if it can't be found.
    """
    logger.debug("Clicking continue to email")
    click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    sleep(3)

    logger.debug("Clicking create e-mail")
    click_element(driver, By.XPATH, "//button[contains(.,'Create e-mail')]")

    sleep(2)

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    sleep(2)

    click_element(driver, By.XPATH, "//button[contains(.,'Preview')]")

    sleep(2)

    link_element = find_element(driver, By.CSS_SELECTOR, "div.email-message a")
    return link_element.get_attribute("href")


_BASC_XPATHS = {
    "Preschool": "//tr[.//span[contains(., 'BASC-4 PRS-Preschool')]]//input[@type='radio']",
    "Child": "//tr[.//span[contains(., 'BASC-4 PRS-Child')]]//input[@type='radio']",
    "Adolescent": "//tr[.//span[contains(., 'BASC-4 PRS-Adolescent')]]//input[@type='radio']",
}


@with_qglobal_recovery()
def _gen_basc(
    driver: WebDriver,
    config: Config,
    client: pd.Series,
    variant: str,
) -> str:
    logger.info(
        f"Generating BASC {variant} for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_select_qglobal(driver, client)
    click_element(driver, By.ID, "examAssessTabFormId:add_assessment")
    click_element(driver, By.XPATH, _BASC_XPATHS[variant])
    click_element(driver, By.ID, "examAssessTabFormId:assignAssessmentBtn")
    click_element(
        driver, By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    )
    find_element(driver, By.ID, "respondentFirstName").send_keys(config.initials[0])
    find_element(driver, By.ID, "respondentLastName").send_keys(config.initials[-1])

    link = get_qglobal_link(driver)
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link}")
    return link


def gen_basc_preschool(
    driver: WebDriver,
    config: Config,
    client: pd.Series,
) -> str:
    """Generates a BASC Preschool assessment for the given client and returns the link."""
    return _gen_basc(driver, config, client, "Preschool")


def gen_basc_child(
    driver: WebDriver,
    config: Config,
    client: pd.Series,
) -> str:
    """Generates a BASC Child assessment for the given client and returns the link."""
    return _gen_basc(driver, config, client, "Child")


def gen_basc_adolescent(
    driver: WebDriver,
    config: Config,
    client: pd.Series,
) -> str:
    """Generates a BASC Adolescent assessment for the given client and returns the link."""
    return _gen_basc(driver, config, client, "Adolescent")


@with_qglobal_recovery()
def gen_vineland(
    driver: WebDriver,
    config: Config,
    client: pd.Series,
) -> str:
    """Generates a Vineland assessment for the given client and returns the link."""
    logger.info(
        f"Generating Vineland for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_select_qglobal(driver, client)

    logger.debug("Clicking add assessment")
    click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logger.debug("Selecting Vineland assessment")
    click_element(driver, By.ID, "2728_radio")

    logger.debug("Assigning assessment")
    click_element(driver, By.ID, "examAssessTabFormId:assignAssessmentBtn")

    logger.debug("Selecting send via email")
    click_element(
        driver, By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    )

    logger.debug("Entering respondent first name")
    find_element(driver, By.ID, "respondentFirstName").send_keys(config.initials[0])

    logger.debug("Entering respondent last name")
    find_element(driver, By.ID, "respondentLastName").send_keys(config.initials[-1])

    logger.debug("Selecting email options")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    sleep(2)
    click_element(
        driver,
        By.XPATH,
        "//label[contains(normalize-space(text()), 'Include')]",
    )
    click_element(
        driver,
        By.XPATH,
        "(//label[contains(normalize-space(text()), 'Include')])[2]",
    )
    sleep(2)

    link = get_qglobal_link(driver)

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link}")
    return link
