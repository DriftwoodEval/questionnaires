from time import sleep

import pandas as pd
from loguru import logger
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import Select

from utils.custom_types import Services
from utils.selenium import (
    click_element,
    find_element,
)


def login_mhs(driver: WebDriver, actions: ActionChains, services: Services) -> None:
    """Log in to MHS."""
    logger.debug("Entering username")
    username = find_element(driver, By.NAME, "txtUsername")

    logger.debug("Entering password")
    password = find_element(driver, By.NAME, "txtPassword")
    username.send_keys(services.mhs.username)
    password.send_keys(services.mhs.password)

    logger.debug("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def check_and_login_mhs(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    first_time: bool = False,
) -> None:
    """Check if logged in to MHS and log in if not."""
    mhs_url = "https://assess.mhs.com"
    if first_time:
        logger.debug("First time login to MHS, logging in now.")
        driver.get(mhs_url)
        login_mhs(driver, actions, services)
        return
    try:
        logger.debug("Checking if logged in to MHS")
        driver.get(mhs_url)
        find_element(
            driver,
            By.XPATH,
            "//span[normalize-space(text())='My Assessments']",
            timeout=2,
        )
        logger.debug("Already logged in to MHS")
    except (NoSuchElementException, TimeoutException):
        logger.debug("Not logged in to MHS, logging in now.")
        login_mhs(driver, actions, services)


def add_client_to_mhs(
    driver: WebDriver,
    actions: ActionChains,
    client: pd.Series,
    questionnaire: str,
    accounts_created: dict[str, bool],
) -> bool:
    """Add a client to MHS, or goes to the existing client.

    Returns:
        bool: True if successful, False otherwise.
    """

    def _add_to_existing(
        driver: WebDriver, actions: ActionChains, client: pd.Series, questionnaire: str
    ) -> bool:
        logger.debug("Client already exists, adding to existing")
        click_element(
            driver,
            By.XPATH,
            "//span[contains(normalize-space(text()), 'My Assessments')]",
        )
        logger.debug(f"Selecting {questionnaire}")
        click_element(
            driver,
            By.XPATH,
            f"//span[contains(normalize-space(text()), '{questionnaire}')]",
        )
        click_element(
            driver,
            By.XPATH,
            "//div[contains(normalize-space(text()), 'Email Invitation')]",
        )
        if questionnaire == "ASRS":
            search = find_element(
                driver,
                By.ID,
                "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_SelectClient_clientSearchBox_Input",
            )
        else:
            search = find_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_SelectClient_clientSearchBox_Input']",
            )

        logger.debug("Searching for client")
        search.send_keys(client["Human Friendly ID"])
        actions.send_keys(Keys.ENTER)
        actions.perform()
        sleep(1)
        if questionnaire == "ASRS":
            logger.debug("Selecting client")
            click_element(
                driver,
                By.XPATH,
                "//tr[@id='ctrlControls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_SelectClient_gdClients_ctl00__0']/td[2]",
            )

            logger.debug("Submitting")
            click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext']",
            )
        else:
            logger.debug("Selecting client")
            click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_SelectClient_gdClients_ctl00_ctl04_ClientSelectSelectCheckBox']",
            )

            logger.debug("Submitting")
            click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_btnNext']",
            )

        logger.debug("Selecting purpose")
        purpose_element = find_element(
            driver, By.CSS_SELECTOR, "select[placeholder='Select an option']"
        )
        purpose = Select(purpose_element)
        sleep(1)
        purpose.select_by_visible_text("Psychoeducational Evaluation")

        if questionnaire == "ASRS":
            logger.debug("Submitting")
            click_element(
                driver,
                By.ID,
                "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext",
            )
        else:
            logger.debug("Submitting")
            click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext']",
            )

        logger.debug("Making sure age matches")
        try:
            age_error = find_element(
                driver,
                By.ID,
                "agerr",
            )
            age_error_style = age_error.get_attribute("style")
            error = age_error_style != "display: none;"
        except (
            NoSuchElementException,
            StaleElementReferenceException,
            TimeoutException,
        ):
            error = False
        if error:
            logger.warning("Age does not match previous client, updating age")
            age_field = find_element(
                driver,
                By.ID,
                "txtAge",
            )
            age_field.send_keys(Keys.CONTROL + "a")
            age_field.send_keys(Keys.BACKSPACE)
            age_field.send_keys(client["Age"])
            if questionnaire == "ASRS":
                logger.debug("Submitting")
                click_element(
                    driver,
                    By.ID,
                    "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext",
                )
            else:
                logger.debug("Submitting")
                click_element(
                    driver,
                    By.ID,
                    "ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext",
                )
            click_element(
                driver,
                By.ID,
                "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_ClientProfile_SaveSuccessWindow_C_btnConfirmOK",
            )
        else:
            logger.debug("Age matches")
            return True
        return True

    if accounts_created.get("mhs"):
        return _add_to_existing(driver, actions, client, questionnaire)

    logger.info(
        f"Attempting to add {client['TA First Name']} {client['TA Last Name']} to MHS"
    )
    firstname = client["TA First Name"]
    lastname = client["TA Last Name"]
    id = client["Human Friendly ID"]
    dob = client["Date of Birth"]
    gender = client["Gender"]
    click_element(driver, By.XPATH, "//div[@class='pull-right']//input[@type='submit']")

    firstname_label = find_element(driver, By.XPATH, "//label[text()='FIRST NAME']")

    logger.debug("Entering first name")
    firstname_field = firstname_label.find_element(
        By.XPATH, "./following-sibling::input"
    )
    firstname_field.send_keys(firstname)

    lastname_label = find_element(driver, By.XPATH, "//label[text()='LAST NAME']")

    logger.debug("Entering last name")
    lastname_field = lastname_label.find_element(By.XPATH, "./following-sibling::input")
    lastname_field.send_keys(lastname)

    id_label = find_element(driver, By.XPATH, "//label[text()='ID']")

    logger.debug("Entering ID")
    id_field = id_label.find_element(By.XPATH, "./following-sibling::input")
    id_field.send_keys(id)

    logger.debug("Entering birthdate")
    date_of_birth_field = find_element(
        driver, By.CSS_SELECTOR, "input[placeholder='YYYY/Mmm/DD']"
    )
    date_of_birth_field.send_keys(dob)

    if questionnaire == "Conners EC" or questionnaire == "ASRS":
        logger.debug("Selecting gender")
        male_label = find_element(driver, By.XPATH, "//label[text()='Male']")
        female_label = find_element(driver, By.XPATH, "//label[text()='Female']")
        if gender == "Male":
            male_label.click()
        else:
            female_label.click()
    else:
        logger.debug("Selecting gender")
        gender_element = find_element(
            driver,
            By.ID,
            "ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_ClientProfile_ddl_Gender",
        )
        gender_select = Select(gender_element)
        sleep(1)
        if gender == "Male":
            gender_select.select_by_visible_text("Male")
        elif gender == "Female":
            gender_select.select_by_visible_text("Female")
        else:
            gender_select.select_by_visible_text("Other")

    logger.debug("Selecting purpose")
    purpose_element = find_element(
        driver, By.CSS_SELECTOR, "select[placeholder='Select an option']"
    )
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Psychoeducational Evaluation")

    logger.debug("Saving")
    click_element(driver, By.CSS_SELECTOR, ".pull-right > input[type='submit']")
    try:
        logger.debug("Checking for existing client")
        find_element(
            driver,
            By.XPATH,
            "//span[contains(text(), 'A client with the same ID already exists')]",
        )
    except TimeoutException:
        logger.success("Added to MHS")
        return True
    return _add_to_existing(driver, actions, client, questionnaire)


def gen_conners_ec(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates a Conners EC assessment for the given client and returns the link."""
    check_and_login_mhs(driver, actions, services)
    logger.info(
        f"Generating Conners EC for {client['TA First Name']} {client['TA Last Name']}"
    )
    click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting Conners EC")
    click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'Conners EC')]"
    )

    logger.debug("Selecting Email Invitation")
    click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "Conners EC", accounts_created
    )

    logger.debug("Selecting assessment description")
    purpose_element = find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)

    logger.debug("Selecting Conners EC")
    purpose.select_by_visible_text("Conners EC")

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")
    rater_type_select = Select(rater_type_element)
    sleep(1)

    logger.debug("Selecting Parent")
    rater_type_select.select_by_visible_text("Parent")

    logger.debug("Selecting language")
    language_element = find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    sleep(1)
    if client["Language"] != "Spanish":
        language_select.select_by_visible_text("English")
    else:
        language_select.select_by_visible_text("Spanish")

    logger.debug("Entering rater name")
    if client["Language"] != "Spanish":
        find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")
    else:
        find_element(driver, By.ID, "txtRaterName").send_keys("Madre/Padre/Cuidador")

    logger.debug("Selecting next")
    click_element(driver, By.ID, "_btnnext")

    logger.debug("Selecting generate link")
    try:
        click_element(driver, By.ID, "btnGenerateLinks")
    except (NoSuchElementException, TimeoutException):
        logger.error(
            "Failed to automatically click 'Generate Links'. "
            "Please click it manually in the browser."
        )
        input(
            "Press Enter once you have clicked 'Generate Links' and the link is visible..."
        )
    sleep(3)

    logger.debug("Getting link")
    link = find_element(driver, By.ID, "txtLink").get_attribute("value")
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_conners_4(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    client: pd.Series,
    accounts_created: dict,
) -> tuple[str, dict[str, bool]]:
    """Generates a Conners 4 assessment for the given client and returns the link."""
    check_and_login_mhs(driver, actions, services)
    logger.info(
        f"Generating Conners 4 for {client['TA First Name']} {client['TA Last Name']}"
    )
    click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting Conners 4")
    click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'Conners 4')]"
    )

    logger.debug("Selecting Email Invitation")
    click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "Conners 4", accounts_created
    )

    logger.debug("Selecting assessment description")
    purpose_element = find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Conners 4")

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")
    rater_type_select = Select(rater_type_element)
    sleep(1)
    rater_type_select.select_by_visible_text("Parent")

    logger.debug("Selecting language")
    language_element = find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    sleep(1)

    if client["Language"] != "Spanish":
        language_select.select_by_visible_text("English")
    else:
        language_select.select_by_visible_text("Spanish")

    logger.debug("Entering rater name")
    if client["Language"] != "Spanish":
        find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")
    else:
        find_element(driver, By.ID, "txtRaterName").send_keys("Madre/Padre/Cuidador")

    logger.debug("Selecting next")
    click_element(driver, By.ID, "_btnnext")

    logger.debug("Selecting generate link")
    try:
        click_element(driver, By.ID, "btnGenerateLinks")
    except (NoSuchElementException, TimeoutException):
        logger.error(
            "Failed to automatically click 'Generate Links'. "
            "Please click it manually in the browser."
        )
        input(
            "Press Enter once you have clicked 'Generate Links' and the link is visible..."
        )
    sleep(3)
    link = find_element(driver, By.ID, "txtLink").get_attribute("value")
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_conners_4_self(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates a Conners 4 Self assessment for the given client and returns the link."""
    check_and_login_mhs(driver, actions, services)
    logger.info(
        f"Generating Conners 4 for {client['TA First Name']} {client['TA Last Name']}"
    )
    click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting Conners 4")
    click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'Conners 4')]"
    )

    logger.debug("Selecting Email Invitation")
    click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "Conners 4", accounts_created
    )

    logger.debug("Selecting assessment description")
    purpose_element = find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)

    logger.debug("Selecting Conners 4")
    purpose.select_by_visible_text("Conners 4")

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")
    rater_type_select = Select(rater_type_element)
    sleep(1)

    logger.debug("Selecting Self-Report")
    rater_type_select.select_by_visible_text("Self-Report")

    logger.debug("Selecting language")
    language_element = find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    sleep(1)

    if client["Language"] != "Spanish":
        language_select.select_by_visible_text("English")
    else:
        language_select.select_by_visible_text("Spanish")

    logger.debug("Entering rater name")
    if client["Language"] != "Spanish":
        find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")
    else:
        find_element(driver, By.ID, "txtRaterName").send_keys("Madre/Padre/Cuidador")

    logger.debug("Selecting next")
    click_element(driver, By.ID, "_btnnext")

    logger.debug("Selecting generate link")
    try:
        click_element(driver, By.ID, "btnGenerateLinks")
    except (NoSuchElementException, TimeoutException):
        logger.error(
            "Failed to automatically click 'Generate Links'. "
            "Please click it manually in the browser."
        )
        input(
            "Press Enter once you have clicked 'Generate Links' and the link is visible..."
        )
    sleep(3)
    link = find_element(driver, By.ID, "txtLink").get_attribute("value")
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_asrs_2_5(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates an ASRS 2-5 assessment for the given client and returns the link."""
    check_and_login_mhs(driver, actions, services)
    logger.info(
        f"Generating ASRS (2-5 Years) for {client['TA First Name']} {client['TA Last Name']}"
    )
    click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting ASRS")
    click_element(driver, By.XPATH, "//span[contains(normalize-space(text()), 'ASRS')]")

    logger.debug("Selecting Email Invitation")
    click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "ASRS", accounts_created
    )

    logger.debug("Selecting assessment description")
    purpose_element = find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("ASRS (2-5 Years)")

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")
    rater_select = Select(rater_type_element)
    sleep(1)
    rater_select.select_by_visible_text("Parent")

    logger.debug("Selecting language")
    language_element = find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    sleep(1)
    if client["Language"] != "Spanish":
        language_select.select_by_visible_text("English")
    else:
        language_select.select_by_visible_text("Spanish")

    logger.debug("Entering rater name")
    if client["Language"] != "Spanish":
        find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")
    else:
        find_element(driver, By.ID, "txtRaterName").send_keys("Madre/Padre/Cuidador")

    logger.debug("Selecting next")
    click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext",
    )

    logger.debug("Generating link")
    try:
        click_element(
            driver,
            By.ID,
            "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_btnGenerateLinks",
        )
    except (NoSuchElementException, TimeoutException):
        logger.error(
            "Failed to automatically click 'Generate Links'. "
            "Please click it manually in the browser."
        )
        input(
            "Press Enter once you have clicked 'Generate Links' and the link is visible..."
        )
    sleep(3)
    link = find_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_rptraters_txtLink_0",
    ).get_attribute("value")
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_asrs_6_18(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates an ASRS 6-18 assessment for the given client and returns the link."""
    check_and_login_mhs(driver, actions, services)
    logger.info(
        f"Generating ASRS (6-18 Years) for {client['TA First Name']} {client['TA Last Name']}"
    )
    click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting ASRS")
    click_element(driver, By.XPATH, "//span[contains(normalize-space(text()), 'ASRS')]")

    logger.debug("Selecting Email Invitation")
    click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "ASRS", accounts_created
    )
    sleep(1)

    logger.debug("Selecting assessment description")
    purpose_element = find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    purpose.select_by_visible_text("ASRS (6-18 Years)")
    sleep(1)

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")
    rater_select = Select(rater_type_element)
    rater_select.select_by_visible_text("Parent")
    sleep(1)

    logger.debug("Selecting language")
    language_element = find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    if client["Language"] != "Spanish":
        language_select.select_by_visible_text("English")
    else:
        language_select.select_by_visible_text("Spanish")

    logger.debug("Entering rater name")
    if client["Language"] != "Spanish":
        find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")
    else:
        find_element(driver, By.ID, "txtRaterName").send_keys("Madre/Padre/Cuidador")

    logger.debug("Selecting next")
    click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext",
    )

    logger.debug("Generating link")
    try:
        click_element(
            driver,
            By.ID,
            "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_btnGenerateLinks",
        )
    except (NoSuchElementException, TimeoutException):
        logger.error(
            "Failed to automatically click 'Generate Links'. "
            "Please click it manually in the browser."
        )
        input(
            "Press Enter once you have clicked 'Generate Links' and the link is visible..."
        )
    sleep(3)
    link = find_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_rptraters_txtLink_0",
    ).get_attribute("value")
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_caars_2(
    driver: WebDriver,
    actions: ActionChains,
    services: Services,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates a CAARS 2 assessment for the given client and returns the link."""
    check_and_login_mhs(driver, actions, services)
    logger.info(
        f"Generating CAARS 2 for {client['TA First Name']} {client['TA Last Name']}"
    )
    click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting CAARS 2")
    click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'CAARS 2')]"
    )

    logger.debug("Selecting Email Invitation")
    click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "CAARS 2", accounts_created
    )

    logger.debug("Selecting assessment description")
    purpose_element = find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("CAARS 2")

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")
    rater_type_select = Select(rater_type_element)
    sleep(1)
    rater_type_select.select_by_visible_text("Self-Report")

    logger.debug("Selecting language")
    language_element = find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    sleep(1)
    if client["Language"] != "Spanish":
        language_select.select_by_visible_text("English")
    else:
        language_select.select_by_visible_text("Spanish")

    logger.debug("Selecting next")
    click_element(driver, By.ID, "_btnnext")

    logger.debug("Generating link")
    try:
        click_element(driver, By.ID, "btnGenerateLinks")
    except (NoSuchElementException, TimeoutException):
        logger.error(
            "Failed to automatically click 'Generate Links'. "
            "Please click it manually in the browser."
        )
        input(
            "Press Enter once you have clicked 'Generate Links' and the link is visible..."
        )
    sleep(5)
    link = find_element(
        driver,
        By.NAME,
        "ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx$CreateLink$txtLink",
    ).get_attribute("value")

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created
