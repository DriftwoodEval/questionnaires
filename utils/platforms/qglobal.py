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
from urllib3.exceptions import MaxRetryError
from urllib3.exceptions import TimeoutError as Urllib3TimeoutError

from utils.custom_types import Config, Services
from utils.selenium import (
    click_element,
    command_timeout,
    find_element,
    find_element_exists,
    get_with_retry,
    restart_selenium,
)

# QGlobal hangs (see with_qglobal_recovery) always surface within a few
# seconds if they're going to happen at all, so commands can use a much
# shorter socket timeout than the driver-wide default and still fail fast
# without misclassifying normal, working requests as hangs.
QGLOBAL_COMMAND_TIMEOUT = 7


def rearrange_dob(dob: str) -> str:
    """Rearrange a date of birth string from "YYYY/MM/DD" to "MM/DD/YYYY" format."""
    return datetime.strptime(dob, "%Y/%m/%d").strftime("%m/%d/%Y")


F = TypeVar("F", bound=Callable)


def with_qglobal_recovery(
    recover: Callable[..., None] | None = None,
) -> Callable[[F], F]:
    """Retry a QGlobal flow if a page load ever hangs.

    QGlobal occasionally never fires the page load complete event, which
    Selenium surfaces as a TimeoutException once the (short) page load
    timeout is hit - or, if a raw driver command (not one of the
    find_element/click_element helpers) hangs instead, as MaxRetryError or
    Urllib3TimeoutError once the socket-level command_timeout is hit. Which
    of the two depends on the HTTP method: urllib3's default retry policy
    excludes POST (nearly every WebDriver command), so a timeout on those
    surfaces as a bare Urllib3TimeoutError rather than MaxRetryError, which
    only wraps a timeout when urllib3 actually retried. A hang bad enough to
    trip any of these can leave chromedriver's session wedged such that no
    further command on it - including window.stop() or a fresh navigation -
    is guaranteed to ever return, so recovering mid-session isn't reliable.
    Instead we kill the browser outright (see restart_selenium), put the
    fresh one back in a known-good spot via `recover` (called with the same
    args as the wrapped function; defaults to just logging back in), and
    retry the whole operation once.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(driver: WebDriver, *args, **kwargs):
            attempts = 2
            for attempt in range(attempts):
                try:
                    with command_timeout(driver, QGLOBAL_COMMAND_TIMEOUT):
                        return func(driver, *args, **kwargs)
                except (TimeoutException, MaxRetryError, Urllib3TimeoutError) as e:
                    if attempt == attempts - 1:
                        logger.error(
                            f"QGlobal page load hung during {func.__name__} "
                            f"({type(e).__name__}: {e}) again after restarting "
                            "the browser, giving up."
                        )
                        # Leave the browser in a clean, logged-out state
                        # rather than wedged, so whatever runs next (the
                        # next test, the next client in qsend.py) doesn't
                        # inherit a session stuck on the same hang.
                        restart_selenium(driver)
                        # Normalize to TimeoutException so callers only
                        # need to handle one type, same as find_element and
                        # click_element already do.
                        if isinstance(e, (MaxRetryError, Urllib3TimeoutError)):
                            raise TimeoutException(str(e)) from e
                        raise
                    logger.warning(
                        f"QGlobal page load hung during {func.__name__} "
                        f"({type(e).__name__}: {e}), restarting the browser "
                        "and retrying."
                    )
                    restart_selenium(driver)
                    if recover is not None:
                        recover(driver, *args, **kwargs)
                    else:
                        # Every wrapped function takes services as its
                        # first arg after driver.
                        check_and_login_qglobal(driver, args[0], first_time=True)
            raise AssertionError("unreachable")

        return wrapper  # type: ignore[return-value]

    return decorator


def _accept_cookies_if_present(driver: WebDriver) -> None:
    if find_element_exists(driver, By.ID, "onetrust-accept-btn-handler", timeout=3):
        click_element(driver, By.ID, "onetrust-accept-btn-handler")


def login_qglobal(driver: WebDriver, services: Services) -> None:
    """Log in to QGlobal."""
    logger.debug("Logging in to QGlobal")
    _accept_cookies_if_present(driver)
    click_element(driver, By.ID, "welcomeLoginForm:signIn")

    # The sign-in click redirects to Pearson's SSO login page, which shows
    # its own cookie banner.
    _accept_cookies_if_present(driver)

    logger.debug("Entering username")
    find_element(driver, By.NAME, "callback_0").send_keys(services.qglobal.username)
    click_element(driver, By.ID, "idToken2_0")

    logger.debug("Entering password")
    find_element(driver, By.NAME, "callback_2").send_keys(services.qglobal.password)
    click_element(driver, By.ID, "idToken4_0")

    try:
        find_element(driver, By.XPATH, "//a[text()='Search']", timeout=60)
        logger.success("Logged in to QGlobal.")
    except (NoSuchElementException, TimeoutException):
        logger.error("Timed out waiting for QGlobal login to complete.")
        raise


def check_and_login_qglobal(
    driver: WebDriver,
    services: Services,
    first_time: bool = False,
) -> None:
    """Check if logged in to QGlobal and log in if not."""
    qglobal_url = "https://qglobal.pearsonassessments.com"
    login_url = "http://qglobal.pearsonassessments.com/qg/welcome.seam"
    if first_time:
        logger.debug("First time login to QGlobal, opening URL.")
        get_with_retry(driver, login_url)
        login_qglobal(driver, services)
        return
    try:
        logger.debug("Checking if logged in to QGlobal")
        get_with_retry(driver, qglobal_url)
        find_element(driver, By.XPATH, "//a[text()='Search']", timeout=5)
        logger.debug("Already logged in to QGlobal")
    except (NoSuchElementException, TimeoutException):
        logger.debug("Not logged in to QGlobal or Search link not visible, logging in.")
        get_with_retry(driver, login_url)
        login_qglobal(driver, services)


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


def search_by_name_qglobal(driver: WebDriver, services: Services, name: str) -> None:
    """Search QGlobal examinees by (partial) username instead of a specific ID.

    Used for test-client cleanup: matching every ZZZTEST client from a run
    with one name search is much faster than a separate search per client.
    """
    check_and_login_qglobal(driver, services)
    logger.info(f"Searching QGlobal for examinees matching {name!r}")
    click_element(driver, By.XPATH, "//a[text()='Search']")
    sleep(1)
    find_element(driver, By.ID, "editExamineeForm:userName").send_keys(name)
    click_element(driver, By.ID, "editExamineeForm:search")


def search_select_qglobal(
    driver: WebDriver,
    services: Services,
    client: pd.Series,
):
    check_and_login_qglobal(driver, services)
    search_qglobal(driver, client)
    sleep(3)

    try:
        logger.debug("Selecting client")
        click_element(
            driver,
            By.XPATH,
            f"//td[contains(text(), '{client['Human Friendly ID']}') and @aria-describedby='list_examineeid']",
        )
        # QGlobal sometimes never fires the page load complete event for
        # the client detail page, leaving the browser stuck "loading"
        # indefinitely. Waiting for a landmark element that's always
        # present once the page has actually loaded gives us a bounded way
        # to detect that hang instead of waiting on it forever.
        find_element(driver, By.ID, "examAssessTabFormId:add_assessment", timeout=5)
    except (NoSuchElementException, TimeoutException) as e:
        logger.error(
            f"Failed to select client {client['Human Friendly ID']} "
            f"({type(e).__name__}: {e})."
        )
        # A hang here means chromedriver's whole session is wedged, not
        # just this one page - confirmed by window.stop()/refresh() and
        # even a screenshot attempt timing out identically when tried
        # in-session. No further commands on this session are going to
        # fare any better, so don't bother retrying locally; propagate
        # straight up to with_qglobal_recovery, which kills and replaces
        # the whole browser process instead of sending it more commands.
        raise


def select_client_for_assessment_qglobal(
    driver: WebDriver,
    services: Services,
    client: pd.Series,
    just_created: bool = False,
) -> None:
    """Select a client from search results and open the assign-assessment modal.

    Skips navigating to the client's full detail page entirely - that
    navigation is what hangs indefinitely on QGlobal's "Processing, please
    wait" screen (see with_qglobal_recovery), and unlike that hang, there's
    no recovering from it mid-session. Checking the client's row checkbox in
    the search results grid and clicking "Assign New Assessment" opens the
    same assessment modal via AJAX, without ever leaving the search page.

    If `just_created` is set (add_client_to_qglobal created this client
    earlier in the same run), QGlobal lands back on a search results page
    that already lists the new client, so search_qglobal - itself a
    navigation, and one more thing that could hang - can be skipped. Falls
    back to a normal search if the client isn't actually there, in case
    that assumption about QGlobal's post-creation state doesn't hold.
    """
    client_id = client["Human Friendly ID"]
    checkbox_xpath = (
        f"//tr[@role='row'][.//td[@aria-describedby='list_examineeid']"
        f"[contains(text(), '{client_id}')]]//input[@type='checkbox']"
    )

    if just_created and find_element_exists(
        driver, By.XPATH, checkbox_xpath, timeout=3
    ):
        logger.debug("Client already visible after creation, skipping search")
    else:
        check_and_login_qglobal(driver, services)
        search_qglobal(driver, client)
        sleep(3)

    logger.debug("Selecting client checkbox")
    click_element(driver, By.XPATH, checkbox_xpath)

    logger.debug("Clicking Assign New Assessment")
    click_element(driver, By.ID, "searchForm:newExAssessmentBtn")

    find_element(driver, By.ID, "searchForm:assignAssessmentBtn", timeout=5)


def _recover_to_search(
    driver: WebDriver, services: Services, client: pd.Series
) -> None:
    """Get back to the search results page for a client, for recovery after a hang.

    Called after with_qglobal_recovery has already restarted the browser,
    so it's always logged out at this point.
    """
    check_and_login_qglobal(driver, services, first_time=True)
    search_qglobal(driver, client)


@with_qglobal_recovery()
def check_for_qglobal_account(
    driver: WebDriver, services: Services, client: pd.Series
) -> bool:
    """Check if a client has an account on QGlobal.

    Returns:
        bool: True if the client has an account on QGlobal, False otherwise.
    """
    check_and_login_qglobal(driver, services)
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
def add_client_to_qglobal(
    driver: WebDriver,
    services: Services,  # noqa: ARG001
    client: pd.Series,
) -> bool:
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


def delete_client_from_qglobal(
    driver: WebDriver, services: Services, client: pd.Series
) -> None:
    """Navigate to a client's QGlobal record and wait for a human to delete it.

    QGlobal doesn't expose a verified, stable selector for its delete-examinee
    action, so this only automates getting to the right examinee and then
    hands off to a human for the actual deletion.
    """
    check_and_login_qglobal(driver, services)
    if not check_for_qglobal_account(driver, services, client):
        logger.debug(
            f"No QGlobal account found for {client['Human Friendly ID']}, nothing to delete"
        )
        return
    search_select_qglobal(driver, services, client)
    input(
        f"QGlobal: please delete examinee {client['Human Friendly ID']} "
        f"({client['TA First Name']} {client['TA Last Name']}) and press enter..."
    )


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


_BASC_ROW_XPATHS = {
    "Preschool": "//tr[@role='row'][.//span[contains(., 'BASC-4 PRS-Preschool')]]",
    "Child": "//tr[@role='row'][.//span[contains(., 'BASC-4 PRS-Child')]]",
    "Adolescent": "//tr[@role='row'][.//span[contains(., 'BASC-4 PRS-Adolescent')]]",
}


def _select_basc_variant(driver: WebDriver, variant: str) -> None:
    """Clicks the row label for the given BASC-4 variant."""
    row_xpath = _BASC_ROW_XPATHS[variant]
    label_xpath = f"{row_xpath}//span[contains(@id, '_radio_span')]"
    click_element(driver, By.XPATH, label_xpath)


@with_qglobal_recovery()
def _gen_basc(
    driver: WebDriver,
    services: Services,
    config: Config,
    client: pd.Series,
    variant: str,
    just_created: bool = False,
) -> str:
    logger.info(
        f"Generating BASC {variant} for {client['TA First Name']} {client['TA Last Name']}"
    )
    select_client_for_assessment_qglobal(driver, services, client, just_created)
    _select_basc_variant(driver, variant)
    click_element(driver, By.ID, "searchForm:assignAssessmentBtn")
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
    services: Services,
    config: Config,
    client: pd.Series,
    just_created: bool = False,
) -> str:
    """Generates a BASC Preschool assessment for the given client and returns the link."""
    return _gen_basc(driver, services, config, client, "Preschool", just_created)


def gen_basc_child(
    driver: WebDriver,
    services: Services,
    config: Config,
    client: pd.Series,
    just_created: bool = False,
) -> str:
    """Generates a BASC Child assessment for the given client and returns the link."""
    return _gen_basc(driver, services, config, client, "Child", just_created)


def gen_basc_adolescent(
    driver: WebDriver,
    services: Services,
    config: Config,
    client: pd.Series,
    just_created: bool = False,
) -> str:
    """Generates a BASC Adolescent assessment for the given client and returns the link."""
    return _gen_basc(driver, services, config, client, "Adolescent", just_created)


@with_qglobal_recovery()
def gen_vineland(
    driver: WebDriver,
    services: Services,
    config: Config,
    client: pd.Series,
    just_created: bool = False,
) -> str:
    """Generates a Vineland assessment for the given client and returns the link."""
    logger.info(
        f"Generating Vineland for {client['TA First Name']} {client['TA Last Name']}"
    )
    select_client_for_assessment_qglobal(driver, services, client, just_created)

    logger.debug("Selecting Vineland assessment")
    click_element(driver, By.ID, "2728_radio")

    logger.debug("Assigning assessment")
    click_element(driver, By.ID, "searchForm:assignAssessmentBtn")

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
