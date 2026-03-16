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


def login_wps(driver: WebDriver, actions: ActionChains, services: Services) -> None:
    """Log in to WPS."""
    logger.debug("Going to login page")
    click_element(driver, By.ID, "loginID")

    logger.debug("Entering username")
    find_element(driver, By.ID, "Username").send_keys(services.wps.username)

    logger.debug("Entering password")
    find_element(driver, By.ID, "Password").send_keys(services.wps.password)

    logger.debug("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def check_and_login_wps(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    first_time: bool = False,
) -> None:
    """Check if logged in to WPS and log in if not."""
    wps_url = "https://platform.wpspublish.com"
    if first_time:
        logger.debug("First time login to WPS, logging in now.")
        driver.get(wps_url)
        login_wps(driver, actions, services)
        return
    try:
        logger.debug("Checking if logged in to WPS")
        driver.get(wps_url)
        find_element(driver, By.ID, "newCase", timeout=2)
        logger.debug("Already logged in to WPS")
    except (NoSuchElementException, TimeoutException):
        logger.debug("Not logged in to WPS, logging in now.")
        login_wps(driver, actions, services)


def gen_dp4(
    driver: WebDriver, actions: ActionChains, config: Config, client: pd.Series
) -> str:
    """Generates a DP-4 assessment for the given client and returns the link."""
    logger.info(
        f"Generating DP-4 for {client['TA First Name']} {client['TA Last Name']}"
    )
    firstname = client["TA First Name"]
    lastname = client["TA Last Name"]
    id = client["Human Friendly ID"]
    dob = client["Date of Birth"]
    gender = client["Gender"]
    click_element(driver, By.ID, "newCase")

    first = find_element(driver, By.XPATH, "//td[@id='FirstName']/input")
    last = find_element(driver, By.XPATH, "//td[@id='LastName']/input")
    account = find_element(driver, By.XPATH, "//td[@id='CaseAltId']/input")

    logger.debug("Entering first name")
    first.send_keys(firstname)
    logger.debug("Entering last name")
    last.send_keys(lastname)
    logger.debug("Entering account number")
    account.send_keys(id)

    logger.debug("Selecting gender")
    gender_element = find_element(driver, By.ID, "genderOpt")
    gender_select = Select(gender_element)
    sleep(1)
    if gender == "Male":
        gender_select.select_by_visible_text("Male")
    else:
        gender_select.select_by_visible_text("Female")

    year = dob[:4]
    month = dob[5:7]
    if int(dob[8:]) < 10:
        day = dob[9:]
    else:
        day = dob[8:]

    if month == "01":
        month = "January"
    elif month == "02":
        month = "February"
    elif month == "03":
        month = "March"
    elif month == "04":
        month = "April"
    elif month == "05":
        month = "May"
    elif month == "06":
        month = "June"
    elif month == "07":
        month = "July"
    elif month == "08":
        month = "August"
    elif month == "09":
        month = "September"
    elif month == "10":
        month = "October"
    elif month == "11":
        month = "November"
    elif month == "12":
        month = "December"

    logger.debug("Selecting birthdate")
    birthdate_month_element = find_element(driver, By.ID, "dobMonth")
    birthdate_month_select = Select(birthdate_month_element)
    sleep(1)
    birthdate_month_select.select_by_visible_text(month)

    birthdate_day_element = find_element(driver, By.ID, "dobDay")
    birthdate_day_select = Select(birthdate_day_element)
    sleep(1)
    birthdate_day_select.select_by_visible_text(day)

    birthdate_year_element = find_element(driver, By.ID, "dobYear")
    birthdate_year_select = Select(birthdate_year_element)
    sleep(1)
    birthdate_year_select.select_by_visible_text(year)

    logger.debug("Saving new client")
    click_element(driver, By.ID, "clientSave")

    logger.debug("Confirming new client")
    click_element(driver, By.XPATH, "//input[@id='successClientCreate']")

    logger.debug("Navigating to client list")
    driver.get("https://platform.wpspublish.com")
    search = find_element(driver, By.XPATH, "//input[@type='search']")

    logger.debug("Searching for client")
    search.send_keys(firstname, " ", lastname)

    logger.debug("Selecting client")
    click_element(driver, By.XPATH, "//table[@id='case']/tbody/tr/td/div")

    logger.debug("Creating new administration")
    click_element(driver, By.XPATH, "//input[@id='newAdministration']")

    logger.debug("Selecting test")
    click_element(
        driver,
        By.XPATH,
        "//img[contains(@src,'https://oes-cdn01.wpspublish.com/content/img/DP-4.png')]",
    )

    logger.debug("Adding form")
    click_element(driver, By.ID, "addForm")
    form_element = find_element(driver, By.ID, "TestFormId")
    form = Select(form_element)
    sleep(1)

    logger.debug("Selecting form")
    if client["Language"] != "Spanish":
        form.select_by_visible_text("Parent/Caregiver Checklist")
    else:
        form.select_by_visible_text("Spanish Parent/Caregiver Checklist")

    logger.debug("Setting delivery method")
    click_element(driver, By.ID, "DeliveryMethod")

    logger.debug("Entering rater name")
    if client["Language"] != "Spanish":
        find_element(driver, By.ID, "RaterName").send_keys("Parent/Caregiver")
    else:
        find_element(driver, By.ID, "RaterName").send_keys("Madre/Padre/Cuidador")

    logger.debug("Entering email")
    find_element(driver, By.ID, "RemoteAdminEmail_ToEmail").send_keys(config.email)

    logger.debug("Selecting copy me")
    click_element(driver, By.ID, "RemoteAdminEmail_CopyMe")

    logger.debug("Pretending to send form")
    click_element(driver, By.XPATH, "//input[@value='Send Form']")

    logger.debug("Selecting form link")
    if client["Language"] != "Spanish":
        click_element(
            driver, By.XPATH, "//td[contains(.,'Parent/Caregiver Checklist')]"
        )
    else:
        click_element(
            driver, By.XPATH, "//td[contains(.,'Spanish Parent/Caregiver Checklist')]"
        )

    logger.debug("Selecting delivery method")
    click_element(driver, By.ID, "DeliveryMethod")
    sleep(3)

    logger.debug("Getting form link")
    body = find_element(driver, By.ID, "RemoteAdminEmail_Content").get_attribute(
        "value"
    )
    if body is None:
        raise ValueError("Email body is None")
    body = body.split()
    body = body[3]
    link = body[6:-1]

    logger.success(f"Returning link {link}")
    return link
