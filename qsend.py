import re
from datetime import datetime
from time import sleep, strftime, strptime

import pandas as pd
from dateutil.relativedelta import relativedelta
from loguru import logger
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import Select

import shared_utils as utils


def get_clients_to_send(config: utils.Config):
    punch_list = utils.get_punch_list(config)

    if punch_list is None:
        logger.critical("Punch list is empty")
        return None

    punch_list = punch_list[
        (punch_list["DA Qs Needed"] == "TRUE") & (punch_list["DA Qs Sent"] != "TRUE")
        | (punch_list["EVAL Qs Needed"] == "TRUE")
        & (punch_list["EVAL Qs Sent"] != "TRUE")
    ]

    punch_list["daeval"] = punch_list.apply(
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


def rearrangedob(dob: str) -> str:
    year = dob[0:4]
    month = dob[5:7]
    day = dob[8:10]
    return f"{month}/{day}/{year}"


def login_ta(
    driver: WebDriver,
    actions: ActionChains,
    services: utils.Services,
    admin: bool = False,
) -> None:
    logger.info("Logging in to TherapyAppointment")

    logger.debug("Going to login page")
    driver.get("https://portal.therapyappointment.com")

    logger.debug("Entering username")
    username_field = utils.find_element(driver, By.NAME, "user_username")
    username_field.send_keys(
        services["therapyappointment"]["admin_username" if admin else "username"]
    )

    logger.debug("Entering password")
    password_field = utils.find_element(driver, By.NAME, "user_password")
    password_field.send_keys(
        services["therapyappointment"]["admin_password" if admin else "password"]
    )

    logger.debug("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def login_wps(
    driver: WebDriver, actions: ActionChains, services: utils.Services
) -> None:
    logger.info("Logging in to WPS")
    driver.get("https://platform.wpspublish.com")

    logger.debug("Going to login page")
    utils.click_element(driver, By.ID, "loginID")

    logger.debug("Entering username")
    utils.find_element(driver, By.ID, "Username").send_keys(services["wps"]["username"])

    logger.debug("Entering password")
    utils.find_element(driver, By.ID, "Password").send_keys(services["wps"]["password"])

    logger.debug("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def login_qglobal(
    driver: WebDriver, actions: ActionChains, services: utils.Services
) -> None:
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
    username = utils.find_element(driver, By.NAME, "login:uname")

    logger.debug("Entering password")
    password = utils.find_element(driver, By.NAME, "login:pword")
    username.send_keys(services["qglobal"]["username"])

    logger.debug("Submitting login form")
    password.send_keys(services["qglobal"]["password"])
    password.send_keys(Keys.ENTER)


def login_mhs(
    driver: WebDriver, actions: ActionChains, services: utils.Services
) -> None:
    logger.info("Logging in to MHS")
    driver.get("https://assess.mhs.com/Account/Login.aspx")

    logger.debug("Entering username")
    username = utils.find_element(driver, By.NAME, "txtUsername")

    logger.debug("Entering password")
    password = utils.find_element(driver, By.NAME, "txtPassword")
    username.send_keys(services["mhs"]["username"])
    password.send_keys(services["mhs"]["password"])

    logger.debug("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def search_qglobal(driver: WebDriver, actions: ActionChains, client: pd.Series) -> None:
    def _search_helper(driver: WebDriver, id: str) -> None:
        logger.info(f"Attempting to search QGlobal for {id}")
        try:
            sleep(1)
            utils.find_element(driver, By.ID, "editExamineeForm:examineeId").send_keys(
                id
            )
        except:  # noqa: E722
            logger.warning("Failed to search, attempting to retry")
            driver.get("https://qglobal.pearsonassessments.com")
            utils.click_element(driver, By.NAME, "searchForm:j_id347")
            _search_helper(driver, id)

    logger.info(f"Searching QGlobal for {client['Human Friendly ID']}")
    utils.click_element(driver, By.NAME, "searchForm:j_id347")

    _search_helper(driver, client["Human Friendly ID"])

    logger.debug("Waiting for page to load")
    sleep(1)

    logger.debug("Submitting search form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def check_for_qglobal_account(
    driver: WebDriver, actions: ActionChains, client: pd.Series
) -> bool:
    driver.get("https://qglobal.pearsonassessments.com/qg/searchExaminee.seam")
    search_qglobal(driver, actions, client)

    logger.info("Checking for QGlobal account")
    try:
        utils.find_element(
            driver,
            By.XPATH,
            f"//td[contains(text(), '{client['Human Friendly ID']}') and @aria-describedby='list_examineeid']",
        )
        return True
    except NoSuchElementException:
        return False


def add_client_to_qglobal(
    driver: WebDriver, actions: ActionChains, client: pd.Series
) -> bool:
    logger.info(
        f"Attempting to add {client['TA First Name']} {client['TA Last Name']} to QGlobal"
    )
    firstname = client["TA First Name"]
    lastname = client["TA Last Name"]
    id = client["Human Friendly ID"]
    dob = client["Date of Birth"]
    gender = client["Gender"]

    logger.debug("Clicking new examinee button")
    utils.click_element(driver, By.ID, "searchForm:newExamineeButton", refresh=True)

    first = utils.find_element(driver, By.ID, "firstName")
    last = utils.find_element(driver, By.ID, "lastName")
    examineeID = utils.find_element(driver, By.ID, "examineeId")
    birth = utils.find_element(driver, By.ID, "calendarInputDate")

    logger.debug("Entering first name")
    first.send_keys(firstname)

    logger.debug("Entering last name")
    last.send_keys(lastname)

    logger.debug("Entering examinee id")
    examineeID.send_keys(id)

    logger.debug("Selecting gender")
    gender_element = utils.find_element(driver, By.ID, "genderMenu")
    gender_select = Select(gender_element)
    sleep(1)
    if gender == "Male":
        gender_select.select_by_visible_text("Male")
    elif gender == "Female":
        gender_select.select_by_visible_text("Female")
    else:
        print("edge case")

    logger.debug("Entering birthdate")
    dob = rearrangedob(dob)
    birth.send_keys(dob)

    logger.debug("Saving new examinee")
    utils.click_element(driver, By.ID, "save")
    return True


def add_client_to_mhs(
    driver: WebDriver,
    actions: ActionChains,
    client: pd.Series,
    questionnaire: str,
    accounts_created: dict[str, bool],
) -> bool:
    def _add_to_existing(
        driver: WebDriver, actions: ActionChains, client: pd.Series, questionnaire: str
    ) -> bool:
        logger.debug("Client already exists, adding to existing")
        utils.click_element(
            driver,
            By.XPATH,
            "//span[contains(normalize-space(text()), 'My Assessments')]",
        )
        logger.debug(f"Selecting {questionnaire}")
        utils.click_element(
            driver,
            By.XPATH,
            f"//span[contains(normalize-space(text()), '{questionnaire}')]",
        )
        utils.click_element(
            driver,
            By.XPATH,
            "//div[contains(normalize-space(text()), 'Email Invitation')]",
        )
        if questionnaire == "ASRS":
            search = utils.find_element(
                driver,
                By.ID,
                "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_SelectClient_clientSearchBox_Input",
            )
        else:
            search = utils.find_element(
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
            utils.click_element(
                driver,
                By.XPATH,
                "//tr[@id='ctrlControls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_SelectClient_gdClients_ctl00__0']/td[2]",
            )

            logger.debug("Submitting")
            utils.click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext']",
            )
        else:
            logger.debug("Selecting client")
            utils.click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_SelectClient_gdClients_ctl00_ctl04_ClientSelectSelectCheckBox']",
            )

            logger.debug("Submitting")
            utils.click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_btnNext']",
            )

        logger.debug("Selecting purpose")
        purpose_element = utils.find_element(
            driver, By.CSS_SELECTOR, "select[placeholder='Select an option']"
        )
        purpose = Select(purpose_element)
        sleep(1)
        purpose.select_by_visible_text("Psychoeducational Evaluation")

        if questionnaire == "ASRS":
            logger.debug("Submitting")
            utils.click_element(
                driver,
                By.ID,
                "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext",
            )
        else:
            logger.debug("Submitting")
            utils.click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext']",
            )

        logger.debug("Making sure age matches")
        try:
            age_error = utils.find_element(
                driver,
                By.ID,
                "agerr",
            )
            age_error_style = age_error.get_attribute("style")
            error = age_error_style != "display: none;"
        except (NoSuchElementException, StaleElementReferenceException):
            error = False
        if error:
            logger.warning("Age does not match previous client, updating age")
            age_field = utils.find_element(
                driver,
                By.ID,
                "txtAge",
            )
            age_field.send_keys(Keys.CONTROL + "a")
            age_field.send_keys(Keys.BACKSPACE)
            age_field.send_keys(client["Age"])
            if questionnaire == "ASRS":
                logger.debug("Submitting")
                utils.click_element(
                    driver,
                    By.ID,
                    "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext",
                )
            else:
                logger.debug("Submitting")
                utils.click_element(
                    driver,
                    By.ID,
                    "ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext",
                )
            utils.click_element(
                driver,
                By.ID,
                "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_ClientProfile_SaveSuccessWindow_C_btnConfirmOK",
            )
        else:
            logger.debug("Age matches")
            return True
        return True

    if "mhs" in accounts_created and accounts_created["mhs"]:
        return _add_to_existing(driver, actions, client, questionnaire)

    logger.info(
        f"Attempting to add {client['TA First Name']} {client['TA Last Name']} to MHS"
    )
    firstname = client["TA First Name"]
    lastname = client["TA Last Name"]
    id = client["Human Friendly ID"]
    dob = client["Date of Birth"]
    gender = client["Gender"]
    utils.click_element(
        driver, By.XPATH, "//div[@class='pull-right']//input[@type='submit']"
    )

    firstname_label = utils.find_element(
        driver, By.XPATH, "//label[text()='FIRST NAME']"
    )

    logger.debug("Entering first name")
    firstname_field = firstname_label.find_element(
        By.XPATH, "./following-sibling::input"
    )
    firstname_field.send_keys(firstname)

    lastname_label = utils.find_element(driver, By.XPATH, "//label[text()='LAST NAME']")

    logger.debug("Entering last name")
    lastname_field = lastname_label.find_element(By.XPATH, "./following-sibling::input")
    lastname_field.send_keys(lastname)

    id_label = utils.find_element(driver, By.XPATH, "//label[text()='ID']")

    logger.debug("Entering ID")
    id_field = id_label.find_element(By.XPATH, "./following-sibling::input")
    id_field.send_keys(id)

    logger.debug("Entering birthdate")
    date_of_birth_field = utils.find_element(
        driver, By.CSS_SELECTOR, "input[placeholder='YYYY/Mmm/DD']"
    )
    date_of_birth_field.send_keys(dob)

    if questionnaire == "Conners EC" or questionnaire == "ASRS":
        logger.debug("Selecting gender")
        male_label = utils.find_element(driver, By.XPATH, "//label[text()='Male']")
        female_label = utils.find_element(driver, By.XPATH, "//label[text()='Female']")
        if gender == "Male":
            male_label.click()
        else:
            female_label.click()
    else:
        logger.debug("Selecting gender")
        gender_element = utils.find_element(
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
    purpose_element = utils.find_element(
        driver, By.CSS_SELECTOR, "select[placeholder='Select an option']"
    )
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Psychoeducational Evaluation")

    logger.debug("Saving")
    utils.click_element(driver, By.CSS_SELECTOR, ".pull-right > input[type='submit']")
    try:
        logger.debug("Checking for existing client")
        utils.find_element(
            driver,
            By.XPATH,
            "//span[contains(text(), 'A client with the same ID already exists')]",
        )
    except NoSuchElementException:
        logger.success("Added to MHS")
        return True
    return _add_to_existing(driver, actions, client, questionnaire)


def get_questionnaires(
    age: int, check: str, daeval: str, qglobal_exists: bool
) -> list[str] | str:
    if daeval == "EVAL":
        if check == "ASD":
            if age < 2:  # 1.5
                return "Too young"
            elif age < 6:
                qs = ["DP4", "BASC Preschool", "Conners EC"]
                if qglobal_exists:
                    qs.append("ASRS (2-5 Years)")
                else:
                    qs.append("Vineland")
                return qs
            elif age < 12:
                qs = ["BASC Child", "Conners 4"]
                if qglobal_exists:
                    qs.append("ASRS (6-18 Years)")
                else:
                    qs.append("Vineland")
                return qs
            elif age < 18:
                qs = [
                    "BASC Adolescent",
                    "Conners 4 Self",
                    "Conners 4",
                ]
                if qglobal_exists:
                    qs.append("ASRS (6-18 Years)")
                else:
                    qs.append("Vineland")
                return qs
            elif age < 19:
                qs = ["ABAS 3", "BASC Adolescent", "PAI", "CAARS 2"]
                if qglobal_exists:
                    qs.append("ASRS (6-18 Years)")
                else:
                    qs.append("Vineland")
            elif age < 22:
                return ["ABAS 3", "BASC Adolescent", "SRS-2", "CAARS 2", "PAI"]
            else:
                return ["ABAS 3", "SRS-2", "CAARS 2", "PAI"]
    elif daeval == "DA":
        if check == "ASD":
            if age < 2:  # 1.5
                return "Too young"
            elif age < 6:
                return ["ASRS (2-5 Years)"]
            elif age < 7:
                return ["ASRS (6-18 Years)"]
            elif age < 8:
                return ["ASRS (6-18 Years)"]
            elif age < 12:
                return ["ASRS (6-18 Years)"]
            elif age < 18:
                return ["ASRS (6-18 Years)"]
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
    elif daeval == "DAEVAL":
        if age < 2:  # 1.5
            return "Too young"
        elif age < 6:
            return [
                "ASRS (2-5 Years)",
                "DP4",
                "BASC Preschool",
                "Vineland",
                "Conners EC",
            ]
        elif age < 7:
            return ["ASRS (6-18 Years)", "BASC Child", "Vineland", "Conners 4"]
        elif age < 12:
            return [
                "ASRS (6-18 Years)",
                "BASC Child",
                "Vineland",
                "Conners 4",
            ]
        elif age < 18:
            return [
                "ASRS (6-18 Years)",
                "BASC Adolescent",
                "Vineland",
                "Conners 4 Self",
                "Conners 4",
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
    return "Unknown"


def assign_questionnaire(
    driver: WebDriver,
    actions: ActionChains,
    config: utils.Config,
    client: pd.Series,
    questionnaire: str,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
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
        if not accounts_created["qglobal"]:
            accounts_created["qglobal"] = add_client_to_qglobal(driver, actions, client)
        else:
            logger.debug("Client already added to QGlobal")
        return gen_basc_preschool(driver, actions, config, client), accounts_created
    elif questionnaire == "BASC Child":
        logger.debug(f"Navigating to QGlobal for {questionnaire}")
        driver.get(qglobal_url)
        if not accounts_created["qglobal"]:
            accounts_created["qglobal"] = add_client_to_qglobal(driver, actions, client)
        else:
            logger.debug("Client already added to QGlobal")
        return gen_basc_child(driver, actions, config, client), accounts_created
    elif questionnaire == "BASC Adolescent":
        logger.debug(f"Navigating to QGlobal for {questionnaire}")
        driver.get(qglobal_url)
        if not accounts_created["qglobal"]:
            accounts_created["qglobal"] = add_client_to_qglobal(driver, actions, client)
        else:
            logger.debug("Client already added to QGlobal")
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
    driver: WebDriver, actions: ActionChains, config: utils.Config, client: pd.Series
) -> str:
    logger.info(
        f"Generating DP4 for {client['TA First Name']} {client['TA Last Name']}"
    )
    firstname = client["TA First Name"]
    lastname = client["TA Last Name"]
    id = client["Human Friendly ID"]
    dob = client["Date of Birth"]
    gender = client["Gender"]
    utils.click_element(driver, By.ID, "newCase")

    first = utils.find_element(driver, By.XPATH, "//td[@id='FirstName']/input")
    last = utils.find_element(driver, By.XPATH, "//td[@id='LastName']/input")
    account = utils.find_element(driver, By.XPATH, "//td[@id='CaseAltId']/input")

    logger.debug("Entering first name")
    first.send_keys(firstname)
    logger.debug("Entering last name")
    last.send_keys(lastname)
    logger.debug("Entering account number")
    account.send_keys(id)

    logger.debug("Selecting gender")
    gender_element = utils.find_element(driver, By.ID, "genderOpt")
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
    birthdate_month_element = utils.find_element(driver, By.ID, "dobMonth")
    birthdate_month_select = Select(birthdate_month_element)
    sleep(1)
    birthdate_month_select.select_by_visible_text(month)

    birthdate_day_element = utils.find_element(driver, By.ID, "dobDay")
    birthdate_day_select = Select(birthdate_day_element)
    sleep(1)
    birthdate_day_select.select_by_visible_text(day)

    birthdate_year_element = utils.find_element(driver, By.ID, "dobYear")
    birthdate_year_select = Select(birthdate_year_element)
    sleep(1)
    birthdate_year_select.select_by_visible_text(year)

    logger.debug("Saving new client")
    utils.click_element(driver, By.ID, "clientSave")

    logger.debug("Confirming new client")
    utils.click_element(driver, By.XPATH, "//input[@id='successClientCreate']")

    logger.debug("Navigating to client list")
    driver.get("https://platform.wpspublish.com")
    search = utils.find_element(driver, By.XPATH, "//input[@type='search']")

    logger.debug("Searching for client")
    search.send_keys(firstname, " ", lastname)

    logger.debug("Selecting client")
    utils.click_element(driver, By.XPATH, "//table[@id='case']/tbody/tr/td/div")

    logger.debug("Creating new administration")
    utils.click_element(driver, By.XPATH, "//input[@id='newAdministration']")

    logger.debug("Selecting test")
    utils.click_element(
        driver,
        By.XPATH,
        "//img[contains(@src,'https://oes-cdn01.wpspublish.com/content/img/DP-4.png')]",
    )

    logger.debug("Adding form")
    utils.click_element(driver, By.ID, "addForm")
    form_element = utils.find_element(driver, By.ID, "TestFormId")
    form = Select(form_element)
    sleep(1)

    logger.debug("Selecting form")
    form.select_by_visible_text("Parent/Caregiver Checklist")

    logger.debug("Setting delivery method")
    utils.click_element(driver, By.ID, "DeliveryMethod")

    logger.debug("Entering rater name")
    utils.find_element(driver, By.ID, "RaterName").send_keys("Parent/Caregiver")

    logger.debug("Entering email")
    utils.find_element(driver, By.ID, "RemoteAdminEmail_ToEmail").send_keys(
        config.email
    )

    logger.debug("Selecting copy me")
    utils.click_element(driver, By.ID, "RemoteAdminEmail_CopyMe")

    logger.debug("Pretending to send form")
    utils.click_element(driver, By.XPATH, "//input[@value='Send Form']")

    logger.debug("Selecting form link")
    utils.click_element(
        driver, By.XPATH, "//td[contains(.,'Parent/Caregiver Checklist')]"
    )

    logger.debug("Selecting delivery method")
    utils.click_element(driver, By.ID, "DeliveryMethod")
    sleep(3)

    logger.debug("Getting form link")
    body = utils.find_element(driver, By.ID, "RemoteAdminEmail_Content").get_attribute(
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
    logger.info(
        f"Generating Conners EC for {client['TA First Name']} {client['TA Last Name']}"
    )
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting Conners EC")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'Conners EC')]"
    )

    logger.debug("Selecting Email Invitation")
    utils.click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "Conners EC", accounts_created
    )

    logger.debug("Selecting assessment description")
    purpose_element = utils.find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)

    logger.debug("Selecting Conners EC")
    purpose.select_by_visible_text("Conners EC")

    logger.debug("Selecting rater type")
    rater_type_element = utils.find_element(driver, By.ID, "ddl_RaterType")
    rater_type_select = Select(rater_type_element)
    sleep(1)

    logger.debug("Selecting Parent")
    rater_type_select.select_by_visible_text("Parent")

    logger.debug("Selecting language")
    language_element = utils.find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    sleep(1)

    logger.debug("Selecting English")
    language_select.select_by_visible_text("English")

    logger.debug("Entering rater name")
    utils.find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logger.debug("Selecting next")
    utils.click_element(driver, By.ID, "_btnnext")

    logger.debug("Selecting generate link")
    utils.click_element(driver, By.ID, "btnGenerateLinks")
    sleep(3)

    logger.debug("Getting link")
    link = utils.find_element(driver, By.ID, "txtLink").get_attribute("value")
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_conners_4(
    driver: WebDriver, actions: ActionChains, client: pd.Series, accounts_created: dict
) -> tuple[str, dict[str, bool]]:
    logger.info(
        f"Generating Conners 4 for {client['TA First Name']} {client['TA Last Name']}"
    )
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting Conners 4")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'Conners 4')]"
    )

    logger.debug("Selecting Email Invitation")
    utils.click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "Conners 4", accounts_created
    )

    logger.debug("Selecting assessment description")
    purpose_element = utils.find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Conners 4")

    logger.debug("Selecting rater type")
    rater_type_element = utils.find_element(driver, By.ID, "ddl_RaterType")
    rater_type_select = Select(rater_type_element)
    sleep(1)
    rater_type_select.select_by_visible_text("Parent")

    logger.debug("Selecting language")
    language_element = utils.find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    sleep(1)
    language_select.select_by_visible_text("English")

    logger.debug("Entering rater name")
    utils.find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logger.debug("Selecting next")
    utils.click_element(driver, By.ID, "_btnnext")

    logger.debug("Selecting generate link")
    utils.click_element(driver, By.ID, "btnGenerateLinks")
    sleep(3)
    link = utils.find_element(driver, By.ID, "txtLink").get_attribute("value")
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
    logger.info(
        f"Generating Conners 4 for {client['TA First Name']} {client['TA Last Name']}"
    )
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting Conners 4")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'Conners 4')]"
    )

    logger.debug("Selecting Email Invitation")
    utils.click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "Conners 4", accounts_created
    )

    logger.debug("Selecting assessment description")
    purpose_element = utils.find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)

    logger.debug("Selecting Conners 4")
    purpose.select_by_visible_text("Conners 4")

    logger.debug("Selecting rater type")
    rater_type_element = utils.find_element(driver, By.ID, "ddl_RaterType")
    rater_type_select = Select(rater_type_element)
    sleep(1)

    logger.debug("Selecting Self-Report")
    rater_type_select.select_by_visible_text("Self-Report")

    logger.debug("Selecting language")
    language_element = utils.find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    sleep(1)

    logger.debug("Selecting English")
    language_select.select_by_visible_text("English")

    logger.debug("Entering rater name")
    utils.find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logger.debug("Selecting next")
    utils.click_element(driver, By.ID, "_btnnext")

    logger.debug("Selecting generate link")
    utils.click_element(driver, By.ID, "btnGenerateLinks")
    sleep(3)
    link = utils.find_element(driver, By.ID, "txtLink").get_attribute("value")
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
    logger.info(
        f"Generating ASRS (2-5 Years) for {client['TA First Name']} {client['TA Last Name']}"
    )
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting ASRS")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'ASRS')]"
    )

    logger.debug("Selecting Email Invitation")
    utils.click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "ASRS", accounts_created
    )

    logger.debug("Selecting assessment description")
    purpose_element = utils.find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("ASRS (2-5 Years)")

    logger.debug("Selecting rater type")
    rater_type_element = utils.find_element(driver, By.ID, "ddl_RaterType")
    rater_select = Select(rater_type_element)
    sleep(1)
    rater_select.select_by_visible_text("Parent")

    logger.debug("Selecting language")
    language_element = utils.find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    sleep(1)
    language_select.select_by_visible_text("English")

    logger.debug("Entering rater name")
    utils.find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logger.debug("Selecting next")
    utils.click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext",
    )

    logger.debug("Generating link")
    utils.click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_btnGenerateLinks",
    )
    sleep(3)
    link = utils.find_element(
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
    logger.info(
        f"Generating ASRS (6-18 Years) for {client['TA First Name']} {client['TA Last Name']}"
    )
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting ASRS")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'ASRS')]"
    )

    logger.debug("Selecting Email Invitation")
    utils.click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "ASRS", accounts_created
    )
    sleep(1)

    logger.debug("Selecting assessment description")
    purpose_element = utils.find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    purpose.select_by_visible_text("ASRS (6-18 Years)")
    sleep(1)

    logger.debug("Selecting rater type")
    rater_type_element = utils.find_element(driver, By.ID, "ddl_RaterType")
    rater_select = Select(rater_type_element)
    rater_select.select_by_visible_text("Parent")
    sleep(1)

    logger.debug("Selecting language")
    language_element = utils.find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    language_select.select_by_visible_text("English")

    logger.debug("Entering rater name")
    utils.find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logger.debug("Selecting next")
    utils.click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext",
    )

    logger.debug("Generating link")
    utils.click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_btnGenerateLinks",
    )
    sleep(3)
    link = utils.find_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_rptraters_txtLink_0",
    ).get_attribute("value")
    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_basc_preschool(
    driver: WebDriver, actions: ActionChains, config: utils.Config, client: pd.Series
) -> str:
    logger.info(
        f"Generating BASC Preschool for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logger.debug("Selecting client")
    utils.click_element(driver, By.XPATH, "//tr[2]/td[5]")

    logger.debug("Clicking add assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logger.debug("Selecting BASC Preschool")
    utils.click_element(driver, By.ID, "2600_radio")

    logger.debug("Assigning assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:assignAssessmentBtn")

    logger.debug("Selecting send via email")
    utils.click_element(
        driver, By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    )

    logger.debug("Entering respondent first name")
    utils.find_element(driver, By.ID, "respondentFirstName").send_keys(
        config.initials[0]
    )

    logger.debug("Entering respondent last name")
    utils.find_element(driver, By.ID, "respondentLastName").send_keys(
        config.initials[-1]
    )

    logger.debug("Clicking continue to email")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    sleep(5)

    logger.debug("Clicking create e-mail")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Create e-mail')]")
    driver.switch_to.frame(
        utils.find_element(driver, By.XPATH, "//iframe[@title='Editor, editor1']")
    )
    link = utils.find_element(driver, By.CSS_SELECTOR, "a").get_attribute("href")

    driver.switch_to.default_content()

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link}")
    return link


def gen_basc_child(
    driver: WebDriver, actions: ActionChains, config: utils.Config, client: pd.Series
) -> str:
    logger.info(
        f"Generating BASC Child for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logger.debug("Selecting client")
    utils.click_element(driver, By.XPATH, "//tr[2]/td[5]")

    logger.debug("Clicking add assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logger.debug("Selecting BASC Child")
    utils.click_element(driver, By.ID, "2598_radio")

    logger.debug("Assigning assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:assignAssessmentBtn")

    logger.debug("Selecting send via email")
    utils.click_element(
        driver, By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    )

    logger.debug("Entering respondent first name")
    utils.find_element(driver, By.ID, "respondentFirstName").send_keys(
        config.initials[0]
    )

    logger.debug("Entering respondent last name")
    utils.find_element(driver, By.ID, "respondentLastName").send_keys(
        config.initials[-1]
    )

    logger.debug("Clicking continue to email")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    sleep(5)

    logger.debug("Clicking create e-mail")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Create e-mail')]")
    driver.switch_to.frame(
        utils.find_element(driver, By.XPATH, "//iframe[@title='Editor, editor1']")
    )
    link = utils.find_element(driver, By.CSS_SELECTOR, "a").get_attribute("href")

    driver.switch_to.default_content()

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link}")
    return link


def gen_basc_adolescent(
    driver: WebDriver, actions: ActionChains, config: utils.Config, client: pd.Series
) -> str:
    logger.info(
        f"Generating BASC Adolescent for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logger.debug("Selecting client")
    utils.click_element(driver, By.XPATH, "//tr[2]/td[5]")

    logger.debug("Clicking add assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logger.debug("Selecting BASC Adolescent")
    utils.click_element(driver, By.ID, "2596_radio")

    logger.debug("Assigning assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:assignAssessmentBtn")

    logger.debug("Selecting send via email")
    utils.click_element(
        driver, By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    )

    logger.debug("Entering respondent first name")
    utils.find_element(driver, By.ID, "respondentFirstName").send_keys(
        config.initials[0]
    )

    logger.debug("Entering respondent last name")
    utils.find_element(driver, By.ID, "respondentLastName").send_keys(
        config.initials[-1]
    )

    logger.debug("Clicking continue to email")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    sleep(5)

    logger.debug("Clicking create e-mail")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Create e-mail')]")
    driver.switch_to.frame(
        utils.find_element(driver, By.XPATH, "//iframe[@title='Editor, editor1']")
    )
    link = utils.find_element(driver, By.CSS_SELECTOR, "a").get_attribute("href")

    driver.switch_to.default_content()

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link}")
    return link


def gen_vineland(
    driver: WebDriver, actions: ActionChains, config: utils.Config, client: pd.Series
) -> str:
    logger.info(
        f"Generating Vineland for {client['TA First Name']} {client['TA Last Name']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logger.debug("Selecting client")
    utils.click_element(driver, By.XPATH, "//tr[2]/td[5]")

    logger.debug("Clicking add assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logger.debug("Selecting Vineland assessment")
    utils.click_element(driver, By.ID, "2728_radio")

    logger.debug("Assigning assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:assignAssessmentBtn")

    logger.debug("Selecting send via email")
    utils.click_element(
        driver, By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    )

    logger.debug("Entering respondent first name")
    utils.find_element(driver, By.ID, "respondentFirstName").send_keys(
        config.initials[0]
    )

    logger.debug("Entering respondent last name")
    utils.find_element(driver, By.ID, "respondentLastName").send_keys(
        config.initials[-1]
    )

    logger.debug("Continuing to email step")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    logger.debug("Selecting email options")
    utils.click_element(driver, By.XPATH, "//div/div[2]/label")
    utils.click_element(
        driver,
        By.XPATH,
        "//div[2]/qg2-multi-column-layout/div/section[2]/div/qg2-form-radio-button/div/div/section[2]/div/div[2]/label",
    )

    logger.debug("Clicking continue to email")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    sleep(5)

    logger.debug("Clicking create e-mail")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Create e-mail')]")
    driver.switch_to.frame(
        utils.find_element(driver, By.XPATH, "//iframe[@title='Editor, editor1']")
    )
    link = utils.find_element(driver, By.CSS_SELECTOR, "a").get_attribute("href")

    driver.switch_to.default_content()

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
    logger.info(
        f"Generating CAARS 2 for {client['TA First Name']} {client['TA Last Name']}"
    )
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logger.debug("Selecting CAARS 2")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'CAARS 2')]"
    )

    logger.debug("Selecting Email Invitation")
    utils.click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    accounts_created["mhs"] = add_client_to_mhs(
        driver, actions, client, "CAARS 2", accounts_created
    )

    logger.debug("Selecting assessment description")
    purpose_element = utils.find_element(driver, By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("CAARS 2")

    logger.debug("Selecting rater type")
    rater_type_element = utils.find_element(driver, By.ID, "ddl_RaterType")
    rater_type_select = Select(rater_type_element)
    sleep(1)
    rater_type_select.select_by_visible_text("Self-Report")

    logger.debug("Selecting language")
    language_element = utils.find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    sleep(1)
    language_select.select_by_visible_text("English")

    logger.debug("Selecting next")
    utils.click_element(driver, By.ID, "_btnnext")

    logger.debug("Generating link")
    utils.click_element(driver, By.ID, "btnGenerateLinks")
    sleep(5)
    link = utils.find_element(
        driver,
        By.NAME,
        "ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx$CreateLink$txtLink",
    ).get_attribute("value")

    if link is None:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def go_to_client(
    driver: WebDriver, actions: ActionChains, client_id: str
) -> str | None:
    def _search_clients(
        driver: WebDriver, actions: ActionChains, client_id: str
    ) -> None:
        logger.info(f"Searching for {client_id} on TA")
        sleep(2)

        logger.debug("Trying to escape random popups")
        actions.send_keys(Keys.ESCAPE)
        actions.perform()

        logger.debug("Entering client ID")
        client_id_label = utils.find_element(
            driver, By.XPATH, "//label[text()='Account Number']"
        )
        client_id_field = client_id_label.find_element(
            By.XPATH, "./following-sibling::input"
        )
        client_id_field.send_keys(client_id)

        logger.debug("Clicking search")
        utils.click_element(driver, By.CSS_SELECTOR, "button[aria-label='Search'")

    def _go_to_client_loop(
        driver: WebDriver, actions: ActionChains, client_id: str
    ) -> str:
        driver.get("https://portal.therapyappointment.com")
        sleep(1)
        logger.debug("Navigating to Clients section")
        utils.click_element(driver, By.XPATH, "//*[contains(text(), 'Clients')]")

        for attempt in range(3):
            try:
                _search_clients(driver, actions, client_id)
                break
            except Exception as e:
                if attempt == 2:
                    logger.exception(f"Failed to search after 3 attempts: {e}")
                    raise e
                else:
                    logger.warning(f"Failed to search: {e}, trying again")
                    driver.refresh()

        sleep(1)

        logger.debug("Selecting client profile")

        utils.click_element(
            driver,
            By.CSS_SELECTOR,
            "a[aria-description*='Press Enter to view the profile of",
            max_attempts=1,
        )

        current_url = driver.current_url
        logger.success(f"Navigated to client profile: {current_url}")
        return current_url

    for attempt in range(3):
        try:
            return _go_to_client_loop(driver, actions, client_id)
        except Exception as e:
            if attempt == 2:
                logger.exception(f"Failed to go to client after 3 attempts: {e}")
                return
            else:
                logger.warning(f"Failed to go to client: {e}, trying again")
                driver.refresh()
    return


def extract_client_data(driver: WebDriver) -> dict[str, str | int]:
    logger.debug("Attempting to extract client data")
    name = utils.find_element(driver, By.CLASS_NAME, "text-h4").text
    firstname = name.split(" ")[0]
    lastname = name.split(" ")[-1]
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
    account_number_element = utils.find_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Account #')]"
    ).text
    account_number = account_number_element.split(" ")[-1]
    birthdate_element = utils.find_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'DOB ')]"
    ).text
    birthdate_str = birthdate_element.split(" ")[-1]
    birthdate = strftime("%Y/%m/%d", strptime(birthdate_str, "%m/%d/%Y"))
    phone_number_element = utils.find_element(
        driver, By.CSS_SELECTOR, "a[aria-description=' current default phone'"
    )
    sleep(0.5)
    phone_number = phone_number_element.text
    phone_number = re.sub(r"\D", "", phone_number)
    gender_title_element = utils.find_element(
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


def check_if_opened_portal(driver: WebDriver) -> bool:
    try:
        utils.find_element(driver, By.CSS_SELECTOR, "input[aria-checked='true']")
        return True
    except NoSuchElementException:
        return False


def check_if_docs_signed(driver: WebDriver) -> bool:
    try:
        utils.find_element(
            driver,
            By.XPATH,
            "//div[contains(normalize-space(text()), 'has completed registration')]",
        )
        return True
    except NoSuchElementException:
        return False


def format_ta_message(questionnaires: list[dict]) -> str:
    logger.debug("Formatting TA message")
    message = ""
    for id, questionnaire in enumerate(questionnaires, start=1):
        notes = ""
        if "Self" in questionnaire["type"]:
            notes = " - For client being tested"
        message += f"{id}) {questionnaire['link']}{notes}\n"
    logger.success("Formatted TA message")
    return message


def send_message_ta(driver: WebDriver, client_url: str, message: str) -> None:
    logger.info("Navigating to client URL")
    driver.get(client_url)

    logger.debug("Accessing Messages section")
    utils.click_element(
        driver, By.XPATH, "//a[contains(normalize-space(text()), 'Messages')]"
    )

    logger.debug("Initiating new message")
    utils.click_element(
        driver,
        By.XPATH,
        "//div[2]/section/div/a/span/span",
    )
    sleep(1)

    logger.debug("Setting message subject")
    utils.find_element(driver, By.ID, "message_thread_subject").send_keys(
        "Please complete the link(s) below. Thank you."
    )
    sleep(1)

    logger.debug("Entering message content")
    text_field = utils.find_element(driver, By.XPATH, "//section/div/div[3]")
    text_field.click()
    sleep(1)
    text_field.send_keys(message)
    sleep(1)

    text_field.click()
    utils.click_element(driver, By.CSS_SELECTOR, "button[type='submit']")
    logger.success("Submitted TA message")


def format_failed_client(
    client: pd.Series,
    error: str,
    questionnaires_needed: list[str] | str = "",
    questionnaire_links_generated: list[str] = [],
) -> dict:
    key = client["Client ID"] if client["Client ID"] else client["Client Name"]
    client_info = {
        "firstName": client["TA First Name"]
        if "TA First Name" in client
        else client["Client Name"].split(" ")[0],
        "lastName": client["TA Last Name"]
        if "TA Last Name" in client
        else client["Client Name"].split(" ")[-1],
        "fullName": client["Client Name"],
        "asdAdhd": client.For,
        "daEval": client.daeval,
        "failedDate": datetime.today().strftime("%Y/%m/%d"),
        "error": error,
    }
    if questionnaires_needed != "":
        client_info["questionnaires_needed"] = questionnaires_needed
    if questionnaire_links_generated != []:
        client_info["questionnaire_links_generated"] = questionnaire_links_generated
    return {key: client_info}


def write_file(filepath: str, data: str) -> None:
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


def check_client_failed(prev_failed_clients: dict, client_info: pd.Series) -> bool:
    logger.debug("Checking if client failed previously")
    if prev_failed_clients == {}:
        return False

    client_id = client_info["Client ID"]
    human_friendly_id = client_info["Human Friendly ID"]
    if client_id and isinstance(prev_failed_clients, dict):
        if int(client_id) in prev_failed_clients:
            client_id_to_use = int(client_id)
        elif client_id in prev_failed_clients:
            client_id_to_use = client_id
        elif human_friendly_id in prev_failed_clients:
            client_id_to_use = human_friendly_id
        else:
            return False

        previously_failed = str(client_id_to_use) != ""

        prev_daeval = prev_failed_clients.get(client_id_to_use, {}).get("daEval", None)
        if prev_daeval is None:
            prev_daeval = prev_failed_clients.get(client_id_to_use, {}).get(
                "check", None
            )
        daeval = client_info["daEval"]

        if previously_failed:
            if daeval == "DA":
                return True
            elif daeval == "EVAL" and prev_daeval == "DA":
                return False
            elif daeval == "EVAL" and prev_daeval != "DA":
                return True
            elif daeval == "DAEVAL":
                return True

    return False


def check_client_previous(prev_clients: dict, client_info: pd.Series):
    if prev_clients is None:
        return False

    client_id = client_info["Client ID"]
    human_friendly_id = client_info["Human Friendly ID"]

    if client_id and isinstance(prev_clients, dict):
        if int(client_id) in prev_clients:
            client_id_to_use = int(client_id)
        elif client_id in prev_clients:
            client_id_to_use = client_id
        elif human_friendly_id in prev_clients:
            client_id_to_use = human_friendly_id
        else:
            return False
        questionnaires = prev_clients.get(client_id_to_use, {}).get(
            "questionnaires", []
        )
        return questionnaires if questionnaires else None


def main():
    services, config = utils.load_config()
    driver, actions = utils.initialize_selenium()

    clients = get_clients_to_send(config)
    prev_clients, prev_failed_clients = utils.get_previous_clients(config, failed=True)

    if clients is None or clients.empty:
        logger.critical("No clients marked to send, exiting")
        return

    projects_api = utils.init_asana(services)
    for login in [login_ta, login_wps, login_qglobal, login_mhs]:
        while True:
            try:
                login(driver, actions, services)
                sleep(1)
                break
            except Exception as e:
                logger.warning(f"Login failed: {e}, trying again")
                sleep(1)

    for _, client in clients.iterrows():
        logger.info(f"Starting loop for {client['Client Name']}")

        if pd.isna(client["Client ID"]) or not client["Client ID"]:
            logger.error(f"Client {client['Client Name']} is missing Client ID")
            utils.add_failure(format_failed_client(client, "Missing Client ID"))
            continue

        if prev_failed_clients != {}:
            if check_client_failed(prev_failed_clients, client):
                logger.error(
                    f"Client {client['Client Name']} has already failed to send"
                )
                continue

        try:
            client_url = go_to_client(driver, actions, client["Client ID"])

            if not client_url:
                logger.error("Client URL not found")
                utils.add_failure(format_failed_client(client, "Unable to find client"))
                continue
            if not check_if_opened_portal(driver):
                utils.add_failure(format_failed_client(client, "Portal not opened"))
                continue
            if not check_if_docs_signed(driver):
                utils.add_failure(format_failed_client(client, "Docs not signed"))
                continue

            client_info = extract_client_data(driver)
            client["Date of Birth"] = client_info["birthdate"]
            client["Age"] = client_info["age"]
            client["Gender"] = client_info["gender"]
            client["Phone Number"] = client_info["phone_number"]
            client["TA First Name"] = client_info["firstname"]
            client["TA Last Name"] = client_info["lastname"]

        except NoSuchElementException as e:
            logger.exception(f"Element not found: {e}")
            utils.add_failure(format_failed_client(client, "Unable to find client"))
            break

        write_file(
            "./put/records.txt",
            f"{client['Client Name']} {client['Client ID']} {datetime.today().strftime('%Y/%m/%d')}",
        )

        try:
            accounts_created = {}
            if int(client["Age"]) < 19 and client["daeval"] != "DA":
                accounts_created["qglobal"] = check_for_qglobal_account(
                    driver, actions, client
                )
            else:
                accounts_created["qglobal"] = False

            questionnaires_needed = get_questionnaires(
                client["Age"],
                client["For"],
                client["daeval"],
                accounts_created["qglobal"],
            )

            if str(questionnaires_needed) == "Too young":
                logger.error(f"Client {client['Client Name']} is too young")
                utils.add_failure(format_failed_client(client, "Too young"))
                break

            if str(questionnaires_needed) == "Unknown":
                logger.error(
                    f"Client {client['Client Name']} has unknown questionnaire needs"
                )
                utils.add_failure(
                    format_failed_client(client, "Unknown questionnaire needs")
                )
                break

            if prev_clients != {}:
                previous_questionnaires = check_client_previous(prev_clients, client)

                if previous_questionnaires:
                    previous_questionnaire_types = [
                        q["questionnaireType"] for q in previous_questionnaires
                    ]
                    overlapping_questionnaires = list(
                        set(previous_questionnaire_types) & set(questionnaires_needed)
                    )
                    if overlapping_questionnaires:
                        logger.error(
                            f"Client {client['Client Name']} needs questionnaires that have already been sent: {', '.join(overlapping_questionnaires)}"
                        )
                        utils.add_failure(
                            format_failed_client(
                                client,
                                f"Overlapping questionnaires: {', '.join(overlapping_questionnaires)}",
                                questionnaires_needed,
                            )
                        )
                        break

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
                except Exception as e:  # noqa: E722
                    logger.exception(f"Error assigning {questionnaire}: {e}")

                    utils.add_failure(
                        format_failed_client(
                            client,
                            "Error assigning questionnaires",
                            questionnaires_needed,
                            questionnaire_links_generated=questionnaires,
                        )
                    )
                    send = False
                    break

                if link is None or link == "":
                    logger.error(f"Gap in elif statement for {questionnaire}")
                    utils.add_failure(
                        format_failed_client(
                            client,
                            "Gap in elif statement",
                            questionnaires_needed,
                            questionnaire_links_generated=questionnaires,
                        )
                    )
                    send = False
                    break

                questionnaires.append(
                    {"done": False, "link": link, "type": questionnaire}
                )

            if send:
                client = utils.search_and_add_questionnaires(
                    projects_api, services, config, client, questionnaires
                )
                utils.insert_basic_client(
                    config,
                    client["Client ID"],
                    client["Asana"],
                    client["Date of Birth"],
                    client["TA First Name"],
                    client["TA Last Name"],
                    client["For"],
                    client["Gender"],
                    client["Phone Number"],
                )
                utils.update_punch_by_daeval(
                    config, client["Client Name"], client["daeval"]
                )
                for questionnaire in questionnaires:
                    utils.put_questionnaire_in_db(
                        config,
                        client["Client ID"],
                        questionnaire["link"],
                        questionnaire["type"],
                        datetime.today().strftime("%Y-%m-%d"),
                        "PENDING",
                    )

                message = format_ta_message(questionnaires)
                send_message_ta(driver, client_url, message)
        except NoSuchElementException as e:
            logger.exception(f"Element not found: {e}")
            utils.add_failure(format_failed_client(client, "Unknown error"))


if __name__ == "__main__":
    logger.add("logs/qsend.log", rotation="500 MB")
    main()
