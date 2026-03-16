from time import sleep

import pandas as pd
from loguru import logger
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import Select

from utils.custom_types import Config, Services
from utils.selenium import (
    click_element,
    find_element,
)


def rearrange_dob(dob: str) -> str:
    """Rearrange a date of birth string from "YYYY-MM-DD" to "MM/DD/YYYY" format."""
    year = dob[0:4]
    month = dob[5:7]
    day = dob[8:10]
    return f"{month}/{day}/{year}"


def login_qglobal(driver: WebDriver, actions: ActionChains, services: Services) -> None:
    """Log in to QGlobal."""
    logger.debug("Attempting to escape cookies popup")
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.ENTER)
    actions.perform()

    logger.debug("Entering username")
    username = find_element(driver, By.NAME, "login:uname")

    logger.debug("Entering password")
    password = find_element(driver, By.NAME, "login:pword")
    username.send_keys(services.qglobal.username)

    logger.debug("Submitting login form")
    password.send_keys(services.qglobal.password)
    password.send_keys(Keys.ENTER)

    try:
        logger.debug("Checking if password is about to expire window is present")
        click_element(driver, By.ID, "passwordAboutToExpireForm:cancel", max_attempts=1)
        logger.debug("Password is about to expire, cancelled popup")
    except (NoSuchElementException, TimeoutException):
        logger.debug("Password is not about to expire, moving on")


def check_and_login_qglobal(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    first_time: bool = False,
) -> None:
    """Check if logged in to QGlobal and log in if not."""
    qglobal_url = "https://qglobal.pearsonassessments.com"
    if first_time:
        logger.debug("First time login to QGlobal, logging in now.")
        driver.get(qglobal_url)
        login_qglobal(driver, actions, services)
        return
    try:
        logger.debug("Checking if logged in to QGlobal")
        driver.get(qglobal_url)
        find_element(driver, By.XPATH, "//a[text()='Search']", timeout=2)
        logger.debug("Already logged in to QGlobal")
    except (NoSuchElementException, TimeoutException):
        logger.debug("Not logged in to QGlobal, logging in now.")
        login_qglobal(driver, actions, services)


def search_qglobal(driver: WebDriver, actions: ActionChains, client: pd.Series) -> None:
    """Search for a client in QGlobal.

    Searches for a client using their human-friendly ID, which is the ID
    shown on TherapyAppointment, "C" + 9-digit number.
    """

    def _search_helper(driver: WebDriver, id: str) -> None:
        for attempt in range(3):
            logger.info(
                f"Attempting to search QGlobal for {id} (attempt {attempt + 1})"
            )
            try:
                sleep(1)
                find_element(driver, By.ID, "editExamineeForm:examineeId").send_keys(id)
                return
            except Exception as e:
                if attempt == 2:
                    logger.error(
                        f"Failed to search QGlobal for {id} after 3 attempts: {e}"
                    )
                    raise e
                logger.warning(
                    f"Failed to search QGlobal for {id}, attempting to retry: {e}"
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
    actions: ActionChains,
    config: Config,
    services: Services,
    client: pd.Series,
):
    check_and_login_qglobal(driver, actions, services)
    search_qglobal(driver, actions, client)
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
        search_qglobal(driver, actions, client)
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


def check_for_qglobal_account(
    driver: WebDriver, actions: ActionChains, services: Services, client: pd.Series
) -> bool:
    """Check if a client has an account on QGlobal.

    Returns:
        bool: True if the client has an account on QGlobal, False otherwise.
    """
    check_and_login_qglobal(driver, actions, services)
    search_qglobal(driver, actions, client)

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


def add_client_to_qglobal(
    driver: WebDriver, actions: ActionChains, client: pd.Series
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
    id = client["Human Friendly ID"]
    dob = client["Date of Birth"]
    gender = client["Gender"]

    logger.debug("Clicking new examinee button")
    click_element(driver, By.ID, "searchForm:newExamineeButton", refresh=True)

    first = find_element(driver, By.ID, "firstName")
    last = find_element(driver, By.ID, "lastName")
    examineeID = find_element(driver, By.ID, "examineeId")
    birth = find_element(driver, By.ID, "calendarInputDate")

    logger.debug("Entering first name")
    first.send_keys(firstname)

    logger.debug("Entering last name")
    last.send_keys(lastname)

    logger.debug("Entering examinee id")
    examineeID.send_keys(id)

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
    dob = rearrange_dob(dob)
    for character in dob:
        birth.send_keys(character)
        sleep(0.3)

    logger.debug("Saving new examinee")
    click_element(driver, By.ID, "save")

    logger.debug("Waiting for search page to load")
    find_element(driver, By.ID, "searchForm:newExamineeButton")
    return True


def get_qglobal_link(driver: WebDriver, actions: ActionChains) -> str | None:
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
    link = link_element.get_attribute("href")

    return link


def gen_basc_preschool(
    driver: WebDriver,
    actions: ActionChains,
    config: Config,
    services: Services,
    client: pd.Series,
) -> str:
    """Generates a BASC Preschool assessment for the given client and returns the link."""
    logger.info(
        f"Generating BASC Preschool for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_select_qglobal(driver, actions, config, services, client)

    logger.debug("Clicking add assessment")
    click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logger.debug("Selecting BASC Preschool")
    click_element(driver, By.ID, "2600_radio")

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

    link = get_qglobal_link(driver, actions)

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link}")
    return link


def gen_basc_child(
    driver: WebDriver,
    actions: ActionChains,
    config: Config,
    services: Services,
    client: pd.Series,
) -> str:
    """Generates a BASC Child assessment for the given client and returns the link."""
    logger.info(
        f"Generating BASC Child for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_select_qglobal(driver, actions, config, services, client)

    logger.debug("Clicking add assessment")
    click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logger.debug("Selecting BASC Child")
    click_element(driver, By.ID, "2598_radio")

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

    link = get_qglobal_link(driver, actions)

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link}")
    return link


def gen_basc_adolescent(
    driver: WebDriver,
    actions: ActionChains,
    config: Config,
    services: Services,
    client: pd.Series,
) -> str:
    """Generates a BASC Adolescent assessment for the given client and returns the link."""
    logger.info(
        f"Generating BASC Adolescent for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_select_qglobal(driver, actions, config, services, client)

    logger.debug("Clicking add assessment")
    click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logger.debug("Selecting BASC Adolescent")
    click_element(driver, By.ID, "2596_radio")

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

    link = get_qglobal_link(driver, actions)

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link}")
    return link


def gen_vineland(
    driver: WebDriver,
    actions: ActionChains,
    config: Config,
    services: Services,
    client: pd.Series,
) -> str:
    """Generates a Vineland assessment for the given client and returns the link."""
    logger.info(
        f"Generating Vineland for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_select_qglobal(driver, actions, config, services, client)

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

    link = get_qglobal_link(driver, actions)

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link}")
    return link
