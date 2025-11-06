import re
from datetime import date, datetime
from time import sleep, strftime, strptime
from typing import Union

import pandas as pd
from dateutil.relativedelta import relativedelta
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

from utils.database import (
    get_previous_clients,
    insert_basic_client,
    put_questionnaire_in_db,
    update_failure_in_db,
    update_questionnaire_in_db,
)
from utils.google import get_punch_list, update_punch_by_column
from utils.misc import add_failure, load_config
from utils.selenium import (
    check_if_docs_signed,
    check_if_opened_portal,
    click_element,
    find_element,
    go_to_client,
    initialize_selenium,
    login_ta,
)
from utils.types import ClientFromDB, Config, FailedClientFromDB, Services


def get_clients_to_send(config: Config) -> pd.DataFrame | None:
    """Gets a list of clients from the punch list who need to have their questionnaire(s) sent to them.

    The list is filtered to only include clients who have a "TRUE" value in the "DA Qs Needed" column, but not in the "DA Qs Sent" column, or who have a "TRUE" value in the "EVAL Qs Needed" column, but not in the "EVAL Qs Sent" column.

    The "daeval" column is added to the DataFrame to distinguish between clients who need to receive the DA and EVAL questionnaires, just the DA questionnaires, or just the EVAL questionnaires.

    Returns:
        pandas.DataFrame | None: A DataFrame containing the punch list data, or None if the punch list is empty.
    """
    punch_list = get_punch_list(config)

    if punch_list is None:
        logger.critical("Punch list is empty")
        return None

    # Filter the punch list to only include clients who need to receive the DA and/or EVAL questionnaires
    punch_list = punch_list[
        (punch_list["DA Qs Needed"] == "TRUE") & (punch_list["DA Qs Sent"] != "TRUE")
        | (punch_list["EVAL Qs Needed"] == "TRUE")
        & (punch_list["EVAL Qs Sent"] != "TRUE")
    ]

    # Add the "daeval" column to the DataFrame
    punch_list["daeval"] = punch_list.apply(
        # Use a lambda function to determine the value of the "daeval" column
        lambda client: (
            "DAEVAL"
            if (
                client["DA Qs Needed"] == "TRUE"
                and client["DA Qs Sent"] != "TRUE"
                and client["EVAL Qs Needed"] == "TRUE"
                and client["EVAL Qs Sent"] != "TRUE"
            )
            else "EVAL"
            if (client["EVAL Qs Needed"] == "TRUE" and client["EVAL Qs Sent"] != "TRUE")
            else "DA"
        ),
        axis=1,
    )

    return punch_list


def rearrange_dob(dob: str) -> str:
    """Rearrange a date of birth string from "YYYY-MM-DD" to "MM/DD/YYYY" format."""
    year = dob[0:4]
    month = dob[5:7]
    day = dob[8:10]
    return f"{month}/{day}/{year}"


def login_wps(driver: WebDriver, actions: ActionChains, services: Services) -> None:
    """Log in to WPS."""
    logger.info("Logging in to WPS")
    driver.get("https://platform.wpspublish.com")

    logger.debug("Going to login page")
    click_element(driver, By.ID, "loginID")

    logger.debug("Entering username")
    find_element(driver, By.ID, "Username").send_keys(services["wps"]["username"])

    logger.debug("Entering password")
    find_element(driver, By.ID, "Password").send_keys(services["wps"]["password"])

    logger.debug("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def login_qglobal(driver: WebDriver, actions: ActionChains, services: Services) -> None:
    """Log in to QGlobal."""
    logger.info("Logging in to QGlobal")
    driver.get("https://qglobal.pearsonassessments.com/")

    logger.debug("Waiting for page to load")
    sleep(3)

    logger.debug("Attempting to escape cookies popup")
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.ENTER)
    actions.perform()

    logger.debug("Entering username")
    username = find_element(driver, By.NAME, "login:uname")

    logger.debug("Entering password")
    password = find_element(driver, By.NAME, "login:pword")
    username.send_keys(services["qglobal"]["username"])

    logger.debug("Submitting login form")
    password.send_keys(services["qglobal"]["password"])
    password.send_keys(Keys.ENTER)


def login_mhs(driver: WebDriver, actions: ActionChains, services: Services) -> None:
    """Log in to MHS."""
    logger.info("Logging in to MHS")
    driver.get("https://assess.mhs.com/Account/Login.aspx")

    logger.debug("Entering username")
    username = find_element(driver, By.NAME, "txtUsername")

    logger.debug("Entering password")
    password = find_element(driver, By.NAME, "txtPassword")
    username.send_keys(services["mhs"]["username"])
    password.send_keys(services["mhs"]["password"])

    logger.debug("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def search_qglobal(driver: WebDriver, actions: ActionChains, client: pd.Series) -> None:
    """Search for a client in QGlobal.

    Searches for a client using their human-friendly ID, which is the ID
    shown on TherapyAppointment, "C" + 9-digit number.

    Args:
        driver (WebDriver): The Selenium WebDriver instance used for
            browser automation.
        actions (ActionChains): The ActionChains instance used for
            simulating user actions.
        client (pd.Series): A Pandas Series containing the client's data.

    Returns:
        None
    """

    def _search_helper(driver: WebDriver, id: str) -> None:
        logger.info(f"Attempting to search QGlobal for {id}")
        try:
            sleep(1)
            find_element(driver, By.ID, "editExamineeForm:examineeId").send_keys(id)
        except:  # noqa: E722
            logger.warning("Failed to search, attempting to retry")
            driver.get("https://qglobal.pearsonassessments.com")
            click_element(driver, By.NAME, "searchForm:j_id347")
            _search_helper(driver, id)

    logger.info(f"Searching QGlobal for {client['Human Friendly ID']}")
    click_element(driver, By.NAME, "searchForm:j_id347")

    _search_helper(driver, client["Human Friendly ID"])

    logger.debug("Waiting for page to load")
    sleep(
        15
    )  # This needs to be this long or we enter the client id in the search twice?

    logger.debug("Submitting search form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def check_for_qglobal_account(
    driver: WebDriver, actions: ActionChains, client: pd.Series
) -> bool:
    """Check if a client has an account on QGlobal.

    Args:
        driver (WebDriver): The Selenium WebDriver instance used for
            browser automation.
        actions (ActionChains): The ActionChains instance used for
            simulating user actions.
        client (pd.Series): A Pandas Series containing the client's data.

    Returns:
        bool: True if the client has an account on QGlobal, False otherwise.
    """
    driver.get("https://qglobal.pearsonassessments.com/qg/searchExaminee.seam")
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

    Args:
        driver (WebDriver): The Selenium WebDriver instance used for
            browser automation.
        actions (ActionChains): The ActionChains instance used for
            simulating user actions.
        client (pd.Series): A Pandas Series containing the client's data.

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
        print("edge case")

    logger.debug("Entering birthdate")
    dob = rearrange_dob(dob)
    birth.send_keys(dob)

    logger.debug("Saving new examinee")
    click_element(driver, By.ID, "save")
    return True


def add_client_to_mhs(
    driver: WebDriver,
    actions: ActionChains,
    client: pd.Series,
    questionnaire: str,
    accounts_created: dict[str, bool],
) -> bool:
    """Add a client to MHS, or goes to the existing client.

    Args:
        driver (WebDriver): The Selenium WebDriver instance used for
            browser automation.
        actions (ActionChains): The ActionChains instance used for
            simulating user actions.
        client (pd.Series): A Pandas Series containing the client's data.
        questionnaire (str): The type of questionnaire to be added to MHS.
        accounts_created (dict[str, bool]): A dictionary containing the
            status of accounts created for the client.

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


def get_questionnaires(age: int, check: str, daeval: str) -> list[str] | str:
    """Get the list of questionnaires to send to a client based on age, appointment type, and prospective diagnosis.

    Returns a list of questionnaire names as strings or a string indicating the client is too young.
    """

    def _get_da_questionnaires(age: int, check: str) -> list[str] | str:
        if check == "ASD+ADHD":
            asd_da = _get_da_questionnaires(age, "ASD")
            if asd_da == "Too young":
                return "Too young"
            adhd_da = _get_da_questionnaires(age, "ADHD")
            return list(asd_da) + list(adhd_da)
        if check == "ASD":
            if age < 2:  # 1.5
                return "Too young"
            elif age < 6:
                return ["ASRS (2-5 Years)"]
            elif age < 19:
                return ["ASRS (6-18 Years)"]
            elif age < 22:
                return ["SRS Self"]
            else:
                return ["SRS Self"]
        elif check == "ADHD":
            if age < 4:
                return "Too young"
            elif age < 6:
                return ["Conners EC"]
            elif age < 12:
                return ["Conners 4"]
            elif age < 18:
                return ["Conners 4", "Conners 4 Self"]
            else:
                return ["CAARS 2"]

        return "Unknown"

    def _get_eval_questionnaires(age: int, check: str) -> list[str] | str:
        if check == "ASD+ADHD":
            asd_adhd_da = _get_eval_questionnaires(age, "ASD+ADHD")
            if asd_adhd_da == "Too young":
                return "Too young"

            asd_eval = _get_eval_questionnaires(age, "ASD")
            adhd_eval = _get_eval_questionnaires(age, "ADHD")
            combined_eval = list(asd_eval) + list(adhd_eval)
            combined_eval = [q for q in combined_eval if q not in asd_adhd_da]
            return combined_eval

        elif check == "ASD":
            if age < 2:  # 1.5
                return "Too young"
            elif age < 6:
                qs = ["Conners EC", "DP4", "BASC Preschool", "Vineland"]
                return qs
            elif age < 12:
                qs = ["Conners 4", "BASC Child", "Vineland"]
                return qs
            elif age < 18:
                qs = ["Conners 4 Self", "Conners 4", "BASC Adolescent", "Vineland"]
                return qs
            elif age < 19:
                qs = ["ABAS 3", "BASC Adolescent", "PAI", "CAARS 2", "Vineland"]
            elif age < 22:
                return ["ABAS 3", "BASC Adolescent", "SRS-2", "CAARS 2", "PAI"]
            else:
                return ["ABAS 3", "SRS-2", "CAARS 2", "PAI"]

        return "Unknown"

    def _get_daeval_questionnaires(age: int) -> list[str] | str:
        if age < 2:  # 1.5
            return "Too young"
        elif age < 6:
            return [
                "Conners EC",
                "ASRS (2-5 Years)",
                "DP4",
                "BASC Preschool",
                "Vineland",
            ]
        elif age < 7:
            return ["Conners 4", "ASRS (6-18 Years)", "DP4", "BASC Child", "Vineland"]
        elif age < 12:
            return [
                "Conners 4",
                "ASRS (6-18 Years)",
                "BASC Child",
                "Vineland",
            ]
        elif age < 18:
            return [
                "Conners 4 Self",
                "Conners 4",
                "ASRS (6-18 Years)",
                "BASC Adolescent",
                "Vineland",
            ]
        elif age < 19:
            return [
                "ASRS (6-18 Years)",
                "ABAS 3",
                "BASC Adolescent",
                "Vineland",
                "PAI",
                "CAARS 2",
            ]
        elif age < 22:
            return ["SRS Self", "ABAS 3", "BASC Adolescent", "SRS-2", "CAARS 2", "PAI"]
        else:
            return ["SRS Self", "ABAS 3", "SRS-2", "CAARS 2", "PAI"]

    if check == "ADHD+LD":
        check = "ADHD"
    if check == "ASD+LD":
        check = "ASD"

    if daeval == "EVAL":
        return _get_eval_questionnaires(age, check)

    elif daeval == "DA":
        return _get_da_questionnaires(age, check)

    elif daeval == "DAEVAL":
        return _get_daeval_questionnaires(age)

    return "Unknown"


def assign_questionnaire(
    driver: WebDriver,
    actions: ActionChains,
    config: Config,
    client: pd.Series,
    questionnaire: str,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generate a questionnaire and assign it to a client.

    Args:
       driver (WebDriver): The Selenium WebDriver instance used for
           browser automation.
       actions (ActionChains): The ActionChains instance used for
           simulating user actions.
       config (Config): The configuration object.
       client (pd.Series): A Pandas Series containing the client's data.
       questionnaire (str): The type of questionnaire to be added to MHS.
       accounts_created (dict[str, bool]): A dictionary containing the
           status of accounts created for the client.

    Returns:
        tuple[str, dict[str, bool]]: A tuple containing the assigned
            questionnaire and the updated accounts_created dictionary.
    """
    logger.info(
        f"Assigning questionnaire '{questionnaire}' to client {client['TA First Name']} {client['TA Last Name']}"
    )
    mhs_url = "https://assess.mhs.com/MainPortal.aspx"
    qglobal_url = "https://qglobal.pearsonassessments.com/qg/searchExaminee.seam"
    wps_url = "https://platform.wpspublish.com/administration/details/4116148"

    if questionnaire == "Conners EC":
        logger.debug(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_conners_ec(driver, actions, client, accounts_created)
    elif questionnaire == "Conners 4":
        logger.debug(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_conners_4(driver, actions, client, accounts_created)
    elif questionnaire == "Conners 4 Self":
        logger.debug(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_conners_4_self(driver, actions, client, accounts_created)
    elif questionnaire == "BASC Preschool":
        logger.debug(f"Navigating to QGlobal for {questionnaire}")
        driver.get(qglobal_url)
        if not accounts_created.get("qglobal"):
            if check_for_qglobal_account(driver, actions, client):
                accounts_created["qglobal"] = True
            else:
                accounts_created["qglobal"] = add_client_to_qglobal(
                    driver, actions, client
                )
        return gen_basc_preschool(driver, actions, config, client), accounts_created
    elif questionnaire == "BASC Child":
        logger.debug(f"Navigating to QGlobal for {questionnaire}")
        driver.get(qglobal_url)
        if not accounts_created.get("qglobal"):
            if check_for_qglobal_account(driver, actions, client):
                accounts_created["qglobal"] = True
            else:
                accounts_created["qglobal"] = add_client_to_qglobal(
                    driver, actions, client
                )
        return gen_basc_child(driver, actions, config, client), accounts_created
    elif questionnaire == "BASC Adolescent":
        logger.debug(f"Navigating to QGlobal for {questionnaire}")
        driver.get(qglobal_url)
        if not accounts_created.get("qglobal"):
            if check_for_qglobal_account(driver, actions, client):
                accounts_created["qglobal"] = True
            else:
                accounts_created["qglobal"] = add_client_to_qglobal(
                    driver, actions, client
                )
        return gen_basc_adolescent(driver, actions, config, client), accounts_created
    elif questionnaire == "ASRS (2-5 Years)":
        logger.debug(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_asrs_2_5(driver, actions, client, accounts_created)
    elif questionnaire == "ASRS (6-18 Years)":
        logger.debug(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_asrs_6_18(driver, actions, client, accounts_created)
    elif questionnaire == "Vineland":
        logger.debug(f"Navigating to QGlobal for {questionnaire}")
        driver.get(qglobal_url)
        if not accounts_created.get("qglobal"):
            if check_for_qglobal_account(driver, actions, client):
                accounts_created["qglobal"] = True
            else:
                accounts_created["qglobal"] = add_client_to_qglobal(
                    driver, actions, client
                )
        return gen_vineland(driver, actions, config, client), accounts_created
    elif questionnaire == "CAARS 2":
        logger.debug(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_caars_2(driver, actions, client, accounts_created)
    elif questionnaire == "DP4":
        logger.debug(f"Navigating to WPS for {questionnaire}")
        driver.get(wps_url)
        return gen_dp4(driver, actions, config, client), accounts_created
    else:
        logger.critical("Unexpected questionnaire type encountered")
        raise ValueError("Unsupported questionnaire type")


def gen_dp4(
    driver: WebDriver, actions: ActionChains, config: Config, client: pd.Series
) -> str:
    """Generates a DP4 assessment for the given client and returns the link."""
    logger.info(
        f"Generating DP4 for {client['TA First Name']} {client['TA Last Name']}"
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
    form.select_by_visible_text("Parent/Caregiver Checklist")

    logger.debug("Setting delivery method")
    click_element(driver, By.ID, "DeliveryMethod")

    logger.debug("Entering rater name")
    find_element(driver, By.ID, "RaterName").send_keys("Parent/Caregiver")

    logger.debug("Entering email")
    find_element(driver, By.ID, "RemoteAdminEmail_ToEmail").send_keys(config.email)

    logger.debug("Selecting copy me")
    click_element(driver, By.ID, "RemoteAdminEmail_CopyMe")

    logger.debug("Pretending to send form")
    click_element(driver, By.XPATH, "//input[@value='Send Form']")

    logger.debug("Selecting form link")
    click_element(driver, By.XPATH, "//td[contains(.,'Parent/Caregiver Checklist')]")

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


def gen_conners_ec(
    driver: WebDriver,
    actions: ActionChains,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates a Conners EC assessment for the given client and returns the link."""
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

    logger.debug("Selecting English")
    language_select.select_by_visible_text("English")

    logger.debug("Entering rater name")
    find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logger.debug("Selecting next")
    click_element(driver, By.ID, "_btnnext")

    logger.debug("Selecting generate link")
    click_element(driver, By.ID, "btnGenerateLinks")
    sleep(3)

    logger.debug("Getting link")
    link = find_element(driver, By.ID, "txtLink").get_attribute("value")
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_conners_4(
    driver: WebDriver, actions: ActionChains, client: pd.Series, accounts_created: dict
) -> tuple[str, dict[str, bool]]:
    """Generates a Conners 4 assessment for the given client and returns the link."""
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
    language_select.select_by_visible_text("English")

    logger.debug("Entering rater name")
    find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logger.debug("Selecting next")
    click_element(driver, By.ID, "_btnnext")

    logger.debug("Selecting generate link")
    click_element(driver, By.ID, "btnGenerateLinks")
    sleep(3)
    link = find_element(driver, By.ID, "txtLink").get_attribute("value")
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_conners_4_self(
    driver: WebDriver,
    actions: ActionChains,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates a Conners 4 Self assessment for the given client and returns the link."""
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

    logger.debug("Selecting English")
    language_select.select_by_visible_text("English")

    logger.debug("Entering rater name")
    find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logger.debug("Selecting next")
    click_element(driver, By.ID, "_btnnext")

    logger.debug("Selecting generate link")
    click_element(driver, By.ID, "btnGenerateLinks")
    sleep(3)
    link = find_element(driver, By.ID, "txtLink").get_attribute("value")
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_asrs_2_5(
    driver: WebDriver,
    actions: ActionChains,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates an ASRS 2-5 assessment for the given client and returns the link."""
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
    language_select.select_by_visible_text("English")

    logger.debug("Entering rater name")
    find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logger.debug("Selecting next")
    click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext",
    )

    logger.debug("Generating link")
    click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_btnGenerateLinks",
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
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates an ASRS 6-18 assessment for the given client and returns the link."""
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
    language_select.select_by_visible_text("English")

    logger.debug("Entering rater name")
    find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logger.debug("Selecting next")
    click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext",
    )

    logger.debug("Generating link")
    click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_btnGenerateLinks",
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
    driver: WebDriver, actions: ActionChains, config: Config, client: pd.Series
) -> str:
    """Generates a BASC Preschool assessment for the given client and returns the link."""
    logger.info(
        f"Generating BASC Preschool for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logger.debug("Selecting client")
    click_element(driver, By.XPATH, "//tr[2]/td[5]")

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
    driver: WebDriver, actions: ActionChains, config: Config, client: pd.Series
) -> str:
    """Generates a BASC Child assessment for the given client and returns the link."""
    logger.info(
        f"Generating BASC Child for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logger.debug("Selecting client")
    click_element(driver, By.XPATH, "//tr[2]/td[5]")

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
    driver: WebDriver, actions: ActionChains, config: Config, client: pd.Series
) -> str:
    """Generates a BASC Adolescent assessment for the given client and returns the link."""
    logger.info(
        f"Generating BASC Adolescent for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logger.debug("Selecting client")
    click_element(driver, By.XPATH, "//tr[2]/td[5]")

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
    driver: WebDriver, actions: ActionChains, config: Config, client: pd.Series
) -> str:
    """Generates a Vineland assessment for the given client and returns the link."""
    logger.info(
        f"Generating Vineland for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logger.debug("Selecting client")
    click_element(driver, By.XPATH, "//tr[2]/td[5]")

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

    logger.debug("Continuing to email step")
    click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    logger.debug("Selecting email options")
    click_element(driver, By.XPATH, "//div/div[2]/label")
    click_element(
        driver,
        By.XPATH,
        "//div[2]/qg2-multi-column-layout/div/section[2]/div/qg2-form-radio-button/div/div/section[2]/div/div[2]/label",
    )

    link = get_qglobal_link(driver, actions)

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link}")
    return link


def gen_caars_2(
    driver: WebDriver,
    actions: ActionChains,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates a CAARS 2 assessment for the given client and returns the link."""
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
    language_select.select_by_visible_text("English")

    logger.debug("Selecting next")
    click_element(driver, By.ID, "_btnnext")

    logger.debug("Generating link")
    click_element(driver, By.ID, "btnGenerateLinks")
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


def extract_client_data(driver: WebDriver) -> dict[str, str | int]:
    """Extracts client data from TherapyAppointment client profile page.

    Args:
        driver (WebDriver): The Selenium WebDriver instance used for
            browser automation.

    Returns:
        dict[str, str | int]: A dictionary containing the following client data:
            - firstname (str)
            - lastname (str)
            - account_number (str)
            - birthdate (str): formatted as "%Y/%m/%d"
            - gender (str): one of "Male", "Female", or "Other"
            - age (int): the client's age in years
            - phone_number (str): the client's phone number
    """
    logger.debug("Attempting to extract client data")
    name = find_element(driver, By.CLASS_NAME, "text-h4").text
    firstname = name.split(" ")[0]
    lastname = name.split(" ")[-1]
    # If client has a suffix, remove it
    if lastname.lower() in [
        "jr",
        "sr",
        "ii",
        "iii",
        "iv",
        "v",
        "vi",
        "vii",
        "viii",
        "ix",
        "x",
    ]:
        lastname = name.split(" ")[-2]
    account_number_element = find_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Account #')]"
    ).text
    account_number = account_number_element.split(" ")[-1]
    birthdate_element = find_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'DOB ')]"
    ).text
    birthdate_str = birthdate_element.split(" ")[-1]
    birthdate = strftime("%Y/%m/%d", strptime(birthdate_str, "%m/%d/%Y"))
    phone_number_element = find_element(
        driver, By.CSS_SELECTOR, "a[aria-description=' current default phone'"
    )
    sleep(0.5)
    phone_number = phone_number_element.text
    phone_number = re.sub(r"\D", "", phone_number)
    gender_title_element = find_element(
        driver,
        By.XPATH,
        "//div[contains(normalize-space(text()), 'Gender') and contains(@class, 'v-list-item__title')]",
    )
    gender_element = gender_title_element.find_element(
        By.XPATH, "following-sibling::div"
    )
    sleep(0.5)
    gender = gender_element.text.split(" ")[0]

    age = relativedelta(datetime.now(), datetime.strptime(birthdate, "%Y/%m/%d")).years
    logger.success("Returned client data")
    return {
        "firstname": firstname,
        "lastname": lastname,
        "account_number": account_number,
        "birthdate": birthdate,
        "gender": gender,
        "age": age,
        "phone_number": phone_number,
    }


def format_ta_message(questionnaires: list[dict]) -> str:
    """Formats the message to be sent in TA."""
    logger.debug("Formatting TA message")
    message = ""
    for id, questionnaire in enumerate(questionnaires, start=1):
        notes = ""
        if "Self" in questionnaire["type"]:
            notes = " - For client being tested"
        message += f"{id}) {questionnaire['link']}{notes}\n"
    logger.success("Formatted TA message")
    return message


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


def write_file(filepath: str, data: str) -> None:
    """Writes data to a file, avoiding duplicates.

    Args:
        filepath (str): The path to the file to be written to.
        data (str): The data to be written to the file.

    If the data already exists in the file, does nothing. If the file does not exist, creates a new file with the data.
    """
    data = data.strip("\n")
    try:
        logger.debug(f"Opening file {filepath} for reading")
        with open(filepath, "r") as file:
            existing_content = file.read().strip("\n")
            if data == existing_content or data in existing_content.split(", "):
                logger.warning("Data already exists in file, skipping write")
                return
            new_content = (
                data if not existing_content else f"{existing_content}, {data}"
            )
        logger.debug(f"Opening file {filepath} for writing")
        with open(filepath, "w") as file:
            file.write(new_content)
            logger.success(f"Wrote new content to {filepath}")
    except FileNotFoundError:
        logger.warning(f"File {filepath} not found, creating new file")
        with open(filepath, "w") as file:
            file.write(data)
            logger.success("Wrote data to new file")


def check_client_failed(
    prev_failed_clients: dict[int, FailedClientFromDB], client_info: pd.Series
) -> tuple[bool, Union[str, None]]:
    """Checks if the client has failed before.

    Args:
        prev_failed_clients (dict): A dictionary where the keys are client IDs and the values are dictionaries containing
            client information.
        client_info (pd.Series): A Pandas Series containing the client to be checked's data.

    Returns:
        bool: True if the client has failed before, False if not.

    A client is considered to have failed before if their ID is in the prev_failed_clients dictionary and they are looking for the same appointment type as before.
    """
    logger.debug("Checking if client failed previously")
    if prev_failed_clients == {}:
        return (False, None)

    client_id = client_info["Client ID"]
    if client_id and isinstance(prev_failed_clients, dict):
        client_id = int(client_id)
        if client_id not in prev_failed_clients:
            return (False, None)

        prev_failed_client = prev_failed_clients[client_id]

        if prev_failed_client.failure["reminded"] >= 100:
            return (False, None)

        prev_daeval = prev_failed_client.failure.get("daEval", None)
        daeval = client_info["daeval"]

        error = prev_failed_client.failure.get("reason", None)
        error = str(error).lower()
        if daeval == "DA":
            return (True, error)
        elif daeval == "EVAL" and prev_daeval == "DA":
            return (False, error)
        elif daeval == "EVAL" and prev_daeval != "DA":
            return (True, error)
        elif daeval == "DAEVAL":
            return (True, error)

    return (False, None)


def check_client_previous(
    prev_clients: dict[int, ClientFromDB], client_info: pd.Series
):
    """Check if a client has any questionnaires from a previous run.

    Returns:
        list | None: A list of questionnaires for the client, if the client was found in the previous clients dictionary and had questionnaires. Otherwise, None.
    """
    if not prev_clients:
        return None

    client_id = int(client_info["Client ID"])

    if client_id in prev_clients:
        questionnaires = prev_clients[client_id].questionnaires
        return questionnaires


def main():
    """Main function for qsend.py.

    Loads the configuration and services objects, sets up the Selenium WebDriver,
    gets the clients to send questionnaires to, and loops through each client to
    send the necessary questionnaires.

    Also handles if a client has already failed to send before, and if a client
    has already been sent questionnaires before, to avoid sending duplicate
    questionnaires.

    If a client has no Client ID or is missing required information, will log an
    error and skip the client.

    If an error occurs while sending questionnaires, will log the error and skip
    the client.

    """
    services, config = load_config()
    driver, actions = initialize_selenium()

    clients = get_clients_to_send(config)
    prev_clients, prev_failed_clients = get_previous_clients(config, failed=True)

    if clients is None or clients.empty:
        logger.critical("No clients marked to send, exiting")
        return

    for login in [login_ta, login_wps, login_qglobal, login_mhs]:
        while True:
            try:
                login(driver, actions, services)
                sleep(1)
                break
            except Exception:
                logger.exception(f"Login failed, trying again")
                sleep(1)

    today = date.today()

    for _, client in clients.iterrows():
        logger.info(f"Starting loop for {client['Client Name']}")

        if prev_failed_clients != {}:
            previously_failed, error = check_client_failed(prev_failed_clients, client)
            if previously_failed and error is not None:
                client["Previous Error"] = error
                if error not in [
                    "too young",
                    "portal not opened",
                    "docs not signed",
                    "not in db",
                    "no dob",
                    "unable to find client",
                ]:
                    logger.error(
                        f"Client {client['Client Name']} has already failed to send"
                    )
                    add_failure(
                        config=config,
                        client_id=client["Client ID"],
                        error=error.lower(),
                        failed_date=today,
                        full_name=client["Client Name"],
                        asd_adhd=client["For"],
                        daeval=client["daeval"],
                    )

                    continue

        if client["Language"] != "" and client["Language"] != "English":
            logger.error(f"Client {client['Client Name']} doesn't speak English")
            add_failure(
                config=config,
                client_id=client["Client ID"],
                error=client["Language"].lower(),
                failed_date=today,
                full_name=client["Client Name"],
                asd_adhd=client["For"],
                daeval=client["daeval"],
            )
            continue

        try:
            client_url = go_to_client(driver, actions, client["Client ID"])

            if not client_url:
                logger.error("Client URL not found")
                add_failure(
                    config=config,
                    client_id=client["Client ID"],
                    error="unable to find client",
                    failed_date=today,
                    full_name=client["Client Name"],
                    asd_adhd=client["For"],
                    daeval=client["daeval"],
                )
                continue
            if not check_if_opened_portal(driver):
                add_failure(
                    config=config,
                    client_id=client["Client ID"],
                    error="portal not opened",
                    failed_date=today,
                    full_name=client["Client Name"],
                    asd_adhd=client["For"],
                    daeval=client["daeval"],
                )
                continue
            else:
                if client.get("Previous Error") == "portal not opened":
                    update_failure_in_db(
                        config=config,
                        client_id=client["Client ID"],
                        reason=client["Previous Error"],
                        da_eval=client["daeval"],
                        resolved=True,
                    )

            if not check_if_docs_signed(driver):
                add_failure(
                    config=config,
                    client_id=client["Client ID"],
                    error="docs not signed",
                    failed_date=today,
                    full_name=client["Client Name"],
                    asd_adhd=client["For"],
                    daeval=client["daeval"],
                )
                continue
            else:
                if client.get("Previous Error") == "docs not signed":
                    update_failure_in_db(
                        config=config,
                        client_id=client["Client ID"],
                        reason=client["Previous Error"],
                        da_eval=client["daeval"],
                        resolved=True,
                    )

            client_from_db = prev_clients.get(int(client["Client ID"]))
            if not client_from_db:
                logger.error(f"Client {client['Client Name']} not found in DB")
                add_failure(
                    config=config,
                    client_id=client["Client ID"],
                    error="not in db",
                    failed_date=today,
                    full_name=client["Client Name"],
                    asd_adhd=client["For"],
                    daeval=client["daeval"],
                )
                continue
            client["Date of Birth"] = client_from_db.dob.strftime("%Y/%m/%d")
            client["Age"] = relativedelta(datetime.now(), client_from_db.dob).years
            client["Gender"] = client_from_db.gender
            client["Phone Number"] = client_from_db.phoneNumber
            if (
                client_from_db.preferredName != None
                and client_from_db.preferredName != ""
            ):
                client["TA First Name"] = client_from_db.preferredName
            else:
                client["TA First Name"] = client_from_db.firstName
            client["TA Last Name"] = client_from_db.lastName

        except (NoSuchElementException, TimeoutException):
            logger.exception(f"Element not found")
            add_failure(
                config=config,
                client_id=client["Client ID"],
                error="unable to find client",
                failed_date=today,
                full_name=client["Client Name"],
                asd_adhd=client["For"],
                daeval=client["daeval"],
            )
            continue

        try:
            accounts_created = {}

            questionnaires_needed = get_questionnaires(
                client["Age"],
                client["For"],
                client["daeval"],
            )

            if str(questionnaires_needed) == "Too young":
                logger.error(f"Client {client['Client Name']} is too young")
                add_failure(
                    config=config,
                    client_id=client["Client ID"],
                    error="too young",
                    failed_date=today,
                    full_name=client["Client Name"],
                    asd_adhd=client["For"],
                    daeval=client["daeval"],
                )
                continue

            if str(questionnaires_needed) == "Unknown":
                logger.error(
                    f"Client {client['Client Name']} has unknown questionnaire needs"
                )
                add_failure(
                    config=config,
                    client_id=client["Client ID"],
                    error="unknown questionnaire needs",
                    failed_date=today,
                    full_name=client["Client Name"],
                    asd_adhd=client["For"],
                    daeval=client["daeval"],
                )
                continue

            if prev_clients != {}:
                previous_questionnaires = check_client_previous(prev_clients, client)

                if previous_questionnaires:
                    previous_questionnaire_info = {
                        q["questionnaireType"]: q["status"]
                        for q in previous_questionnaires
                    }

                    questionnaires_to_remove = [
                        q_type
                        for q_type, status in previous_questionnaire_info.items()
                        if q_type in questionnaires_needed
                        and status in ["COMPLETED", "EXTERNAL"]
                    ]

                    questionnaires_needed = list(
                        set(questionnaires_needed) - set(questionnaires_to_remove)
                    )

                    remaining_overlaps = []
                    for q_type in questionnaires_needed:
                        if q_type in previous_questionnaire_info:
                            status = previous_questionnaire_info[q_type]
                            if status not in ["COMPLETED", "EXTERNAL"]:
                                remaining_overlaps.append(f"{q_type} - {status}")

                    if remaining_overlaps:
                        logger.error(
                            f"Client {client['Client Name']} needs questionnaires that were previously sent and are not complete: {', '.join(remaining_overlaps)}"
                        )
                    add_failure(
                        config=config,
                        client_id=client["Client ID"],
                        error=f"Overlapping questionnaires: {', '.join(remaining_overlaps)}",
                        failed_date=today,
                        full_name=client["Client Name"],
                        asd_adhd=client["For"],
                        daeval=client["daeval"],
                        questionnaires_needed=questionnaires_needed,
                    )
                    continue

            logger.info(
                f"Client {client['Client Name']} needs questionnaires for a {client['For']} {client['daeval']}: {questionnaires_needed}"
            )

            questionnaires = []
            send = True
            for questionnaire in questionnaires_needed:
                try:
                    link, accounts_created = assign_questionnaire(
                        driver,
                        actions,
                        config,
                        client,
                        questionnaire,
                        accounts_created,
                    )

                    if link is None or link == "":
                        logger.error(f"No link grabbed for {questionnaire}")
                        add_failure(
                            config=config,
                            client_id=client["Client ID"],
                            error=f"No link grabbed for {questionnaire}",
                            failed_date=today,
                            full_name=client["Client Name"],
                            asd_adhd=client["For"],
                            daeval=client["daeval"],
                            questionnaires_needed=questionnaires_needed
                            if type(questionnaires_needed) is list
                            else [],
                            questionnaire_links_generated=questionnaires,
                        )
                        send = False
                        continue

                    questionnaires.append({"link": link, "type": questionnaire})
                    put_questionnaire_in_db(
                        config,
                        client["Client ID"],
                        link,
                        questionnaire,
                        datetime.today().strftime("%Y-%m-%d"),
                        "JUST_ADDED",
                    )

                except Exception as e:  # noqa: E722
                    logger.exception(f"Error assigning {questionnaire}")

                    # TODO 🫠: see if generated questionnaires make it in
                    add_failure(
                        config=config,
                        client_id=client["Client ID"],
                        error=f"Error assigning {questionnaire}",
                        failed_date=today,
                        full_name=client["Client Name"],
                        asd_adhd=client["For"],
                        daeval=client["daeval"],
                        questionnaires_needed=questionnaires_needed
                        if type(questionnaires_needed) is list
                        else [],
                        questionnaire_links_generated=questionnaires,
                    )
                    send = False
                    continue

            if send:
                insert_basic_client(
                    config,
                    client["Client ID"],
                    client["Date of Birth"],
                    client["TA First Name"],
                    client["TA Last Name"],
                    client["For"],
                    client["Gender"],
                    client["Phone Number"],
                )
                update_punch_by_column(
                    config, client["Client ID"], client["daeval"], "sent"
                )
                for questionnaire in questionnaires:
                    update_questionnaire_in_db(
                        config,
                        client["Client ID"],
                        questionnaire["type"],
                        datetime.today().strftime("%Y-%m-%d"),
                        "PENDING",
                    )

                message = format_ta_message(questionnaires)
                send_message_ta(driver, client_url, message)
        except Exception as e:
            logger.exception(f"Error for client {client['Client Name']}")
            add_failure(
                config=config,
                client_id=client["Client ID"],
                error=str(e),
                failed_date=today,
                full_name=client["Client Name"],
                asd_adhd=client["For"],
                daeval=client["daeval"],
            )


if __name__ == "__main__":
    logger.add("logs/qsend.log", rotation="500 MB")
    main()
