import pandas as pd
from loguru import logger
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver

from utils.custom_types import Config, Services
from utils.selenium import (
    click_element,
    find_element,
    find_element_exists,
    set_local_storage_item,
)


def _present_with_text(locator):
    """An expected_conditions-style condition for find_element: waits for
    the element to be present *and* have non-empty text, not just present.

    Needed for the form-link span, which exists in the DOM before the link
    itself has actually rendered into it.
    """

    def _predicate(driver: WebDriver):
        try:
            element = driver.find_element(*locator)
        except NoSuchElementException:
            return False
        return element if element.text.strip() else False

    return _predicate


def login_wps(driver: WebDriver, services: Services) -> None:
    """Log in to WPS."""
    actions = ActionChains(driver)
    # Pre-seed WPS's onboarding tour state as already-completed so it doesn't pop
    # up and block the page on first login.
    set_local_storage_item(
        driver,
        "savedTours",
        '{"getStarted":{"visits":4,"status":"completed"},"noCase":{"visits":4,"status":"completed"},"singleCase":{"visits":4,"status":"inactive"},"multipleCase":{"visits":4,"status":"inactive"}}',
    )
    logger.debug("Entering username")
    find_element(driver, By.CSS_SELECTOR, '[name="username"]').send_keys(
        services.wps.username
    )

    logger.debug("Entering password")
    find_element(driver, By.CSS_SELECTOR, '[name="password"]').send_keys(
        services.wps.password
    )

    logger.debug("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def check_and_login_wps(
    driver: WebDriver,
    services: Services,
    first_time: bool = False,
) -> None:
    """Check if logged in to WPS and log in if not."""
    wps_url = "https://hub.wpspublish.com/clients"
    if first_time:
        logger.debug("First time login to WPS, logging in now.")
        driver.get(wps_url)
        login_wps(driver, services)
        # Wait for the post-login redirect to actually land before letting
        # callers navigate away - otherwise the first navigation after
        # login can race the redirect and hit a half-loaded page.
        find_element(
            driver,
            By.CSS_SELECTOR,
            '[data-testid="clients-create-client-button"]',
            timeout=15,
        )
        return
    try:
        logger.debug("Checking if logged in to WPS")
        driver.get(wps_url)
        maybe_later_xpath = "//button[h4[contains(text(), 'Maybe Later')]]"
        if find_element_exists(driver, By.XPATH, maybe_later_xpath, timeout=2):
            logger.info("Found 'Maybe Later' tour button, clicking it.")
            click_element(driver, By.XPATH, maybe_later_xpath)
        find_element(
            driver,
            By.CSS_SELECTOR,
            '[data-testid="clients-create-client-button"]',
            timeout=2,
        )
        logger.debug("Already logged in to WPS")
    except (NoSuchElementException, TimeoutException):
        logger.debug("Not logged in to WPS, logging in now.")
        login_wps(driver, services)


def find_and_select_client_wps(
    driver: WebDriver, firstname: str, lastname: str
) -> None:
    """Search for a client by name in the WPS client list and open their profile."""
    logger.debug("Navigating to client list")
    driver.get("https://hub.wpspublish.com/clients")

    maybe_later_xpath = "//button[h4[contains(text(), 'Maybe Later')]]"
    if find_element_exists(driver, By.XPATH, maybe_later_xpath, timeout=2):
        logger.info("Found 'Maybe Later' tour button, clicking it.")
        click_element(driver, By.XPATH, maybe_later_xpath)

    click_element(driver, By.CSS_SELECTOR, '[data-testid="clients-search-button"]')
    search = find_element(driver, By.CSS_SELECTOR, '[name="clients-search-input"]')

    logger.debug("Searching for client")
    search.send_keys(f"{firstname} {lastname}")

    logger.debug("Selecting client")
    click_element(
        driver,
        By.XPATH,
        f"//a[.//h4[contains(text(), '{firstname} {lastname}')]]",
        timeout=10,
    )

    skip_xpath = "//button[h4[contains(text(), 'Skip')]]"
    if find_element_exists(driver, By.XPATH, skip_xpath, timeout=2):
        logger.info("Found 'Skip' tour button, clicking it.")
        click_element(driver, By.XPATH, skip_xpath)


def delete_client_from_wps(driver: WebDriver, client: pd.Series) -> None:
    """Navigate to a client's WPS profile and wait for a human to delete them.

    WPS doesn't expose a verified, stable selector for its delete-client
    action, so this only automates getting to the right profile and then
    hands off to a human for the actual deletion.
    """
    firstname = client["TA First Name"]
    lastname = client["TA Last Name"]
    logger.info(f"Navigating to WPS profile for {firstname} {lastname} to delete")
    find_and_select_client_wps(driver, firstname, lastname)
    input(
        f"WPS: please delete client {firstname} {lastname} "
        f"({client['Human Friendly ID']}) and press enter..."
    )


def gen_dp4(driver: WebDriver, config: Config, client: pd.Series) -> str:
    """Generates a DP-4 assessment for the given client and returns the link."""
    logger.info(
        f"Generating DP-4 for {client['TA First Name']} {client['TA Last Name']}"
    )
    firstname = client["TA First Name"]
    lastname = client["TA Last Name"]
    hf_id = client["Human Friendly ID"]
    dob = client["Date of Birth"]
    gender = client["Gender"]
    driver.get("https://hub.wpspublish.com/clients/add-client")

    first = find_element(driver, By.ID, "firstName")
    last = find_element(driver, By.ID, "lastName")
    account = find_element(driver, By.ID, "clientId")
    birthday = find_element(driver, By.CSS_SELECTOR, '[name="birthDay"]')

    logger.debug("Entering first name")
    first.send_keys(firstname)
    logger.debug("Entering last name")
    last.send_keys(lastname)
    logger.debug("Entering account number")
    account.send_keys(hf_id)
    logger.debug("Entering birthday")
    year = dob[0:4]
    month = dob[5:7]
    day = dob[8:10]
    birthday.send_keys(f"{month}{day}{year}")

    logger.debug("Selecting gender")
    click_element(
        driver,
        By.CSS_SELECTOR,
        '[data-testid="clientform-pi-gender-dropdown"]',
        scroll=True,
    )
    if gender == "Male":
        click_element(
            driver,
            By.CSS_SELECTOR,
            '[data-testid="clientform-pi-gender-0-button"]',
            scroll=True,
            timeout=10,
        )
    else:
        click_element(
            driver,
            By.CSS_SELECTOR,
            '[data-testid="clientform-pi-gender-1-button"]',
            scroll=True,
            timeout=10,
        )

    logger.debug("Saving new client")
    click_element(driver, By.CSS_SELECTOR, '[data-testid="clientform-submit-button"]')

    logger.debug("Creating new administration")
    try:
        click_element(
            driver,
            By.XPATH,
            "//button[h5[contains(text(), 'New Administration')]]",
            timeout=15,
        )
    except (NoSuchElementException, TimeoutException, ElementClickInterceptedException):
        logger.error("Failed to create new administration. ")
        input("Please click New Administration and press enter...")

    logger.debug("Selecting test")
    click_element(
        driver,
        By.CSS_SELECTOR,
        '[data-testid="batterybuilder-assessments-3-expand-button"]',
    )

    logger.debug("Selecting form")
    if client["Language"] != "Spanish":
        click_element(
            driver,
            By.CSS_SELECTOR,
            '[data-testid="batterybuilder-assessments-3-forms-1-add-button"]',
        )
    else:
        click_element(
            driver,
            By.CSS_SELECTOR,
            '[data-testid="batterybuilder-assessments-3-forms-5-add-button"]',
        )

    click_element(
        driver, By.CSS_SELECTOR, '[data-testid="batterybuilder-submit-button"]'
    )

    logger.debug("Adding respondent")
    click_element(
        driver,
        By.CSS_SELECTOR,
        '[data-testid="cases-table-form-add-respondent-button"]',
    )

    logger.debug("Entering rater name")
    if client["Language"] != "Spanish":
        find_element(driver, By.CSS_SELECTOR, '[name="firstName"]').send_keys("Parent")
        find_element(driver, By.CSS_SELECTOR, '[name="lastName"]').send_keys(
            "Caregiver"
        )
    else:
        find_element(driver, By.CSS_SELECTOR, '[name="firstName"]').send_keys(
            "Madre/Padre"
        )
        find_element(driver, By.CSS_SELECTOR, '[name="lastName"]').send_keys("Cuidador")

    logger.debug("Entering email")
    find_element(driver, By.CSS_SELECTOR, '[name="email"]').send_keys(config.email)

    logger.debug("Saving form")
    click_element(
        driver, By.CSS_SELECTOR, '[data-testid="respondentModal-save-button"]'
    )

    logger.debug("Pretending to send form")
    click_element(
        driver, By.CSS_SELECTOR, '[data-testid="cases-table-form-action-button"]'
    )

    logger.debug("Grabbing form link")
    link_span = find_element(
        driver,
        By.XPATH,
        "//span[contains(text(), 'https://')]",
        timeout=10,
        condition=_present_with_text,
    )
    link = link_span.text.split()[-1]

    logger.success(f"Returning link {link}")
    return link
