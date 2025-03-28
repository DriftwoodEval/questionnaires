import logging
from datetime import datetime
from time import sleep, strftime, strptime

from dateutil.relativedelta import relativedelta
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

import shared_utils as utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("qsend.log"), logging.StreamHandler()],
)

services, config = utils.load_config()


def get_clients():
    logging.info("Loading clients list file")
    with open("./put/automation.txt", "r") as f:
        automation = f.read()
        clients = automation.split(",")
        logging.info(f"Loaded {len(clients)} clients")
    return clients


def parameterize(client):
    client = client.split()
    first = client[0]
    last = client[1]
    check = client[2]
    daeval = "DA"
    if check == "ADHD":
        date = client[3]
    else:
        daeval = client[3]
        date = client[4]
    logging.info(
        f"Client: {first} {last}, {check}, {daeval}, with an appointment on {date}"
    )
    return {
        "firstname": first,
        "lastname": last,
        "check": check,
        "daeval": daeval,
        "date": date,
    }


def rearrangedob(dob):
    year = dob[0:4]
    month = dob[5:7]
    day = dob[8:10]
    return f"{month}/{day}/{year}"


def login_ta(driver, actions):
    logging.info("Logging in to TherapyAppointment")

    logging.info("Going to login page")
    driver.get("https://portal.therapyappointment.com")

    logging.info("Entering username")
    username_field = driver.find_element(By.NAME, "user_username")
    username_field.send_keys(services["therapyappointment"]["username"])

    logging.info("Entering password")
    password_field = driver.find_element(By.NAME, "user_password")
    password_field.send_keys(services["therapyappointment"]["password"])

    logging.info("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def login_wps(driver, actions):
    logging.info("Logging in to WPS")
    driver.get("https://platform.wpspublish.com")

    logging.info("Going to login page")
    utils.click_element(driver, By.ID, "loginID")

    logging.info("Entering username")
    driver.find_element(By.ID, "Username").send_keys(services["wps"]["username"])

    logging.info("Entering password")
    driver.find_element(By.ID, "Password").send_keys(services["wps"]["password"])

    logging.info("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def login_qglobal(driver, actions):
    logging.info("Logging in to QGlobal")
    driver.get("https://qglobal.pearsonassessments.com/")

    logging.info("Waiting for page to load")
    sleep(3)

    logging.info("Attempting to escape cookies popup")
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.ENTER)
    actions.perform()

    logging.info("Entering username")
    username = driver.find_element(By.NAME, value="login:uname")

    logging.info("Entering password")
    password = driver.find_element(By.NAME, value="login:pword")
    username.send_keys(services["qglobal"]["username"])

    logging.info("Submitting login form")
    password.send_keys(services["qglobal"]["password"])
    password.send_keys(Keys.ENTER)


def login_mhs(driver, actions):
    logging.info("Logging in to MHS")
    driver.get("https://assess.mhs.com/Account/Login.aspx")

    logging.info("Entering username")
    username = driver.find_element(By.NAME, value="txtUsername")

    logging.info("Entering password")
    password = driver.find_element(By.NAME, value="txtPassword")
    username.send_keys(services["mhs"]["username"])
    password.send_keys(services["mhs"]["password"])

    logging.info("Submitting login form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def search_helper(driver, id):
    logging.info(f"Attempting to search for {id}")
    try:
        sleep(1)
        driver.find_element(By.ID, "editExamineeForm:examineeId").send_keys(id)
    except:  # noqa: E722
        logging.info("Failed to search, attempting to retry")
        driver.get("https://qglobal.pearsonassessments.com")
        utils.click_element(driver, By.NAME, "searchForm:j_id347")
        search_helper(driver, id)


def search_qglobal(driver, actions, client):
    logging.info(f"Searching QGlobal for {client['account_number']}")
    id = client["account_number"]
    utils.click_element(driver, By.NAME, "searchForm:j_id347")

    search_helper(driver, id)

    logging.info("Waiting for page to load")
    sleep(1)

    logging.info("Submitting search form")
    actions.send_keys(Keys.ENTER)
    actions.perform()


def add_client_to_qglobal(driver, actions, client):
    logging.info(
        f"Attempting to add {client['firstname']} {client['lastname']} to QGlobal"
    )
    firstname = client["firstname"]
    lastname = client["lastname"]
    id = client["account_number"]
    dob = client["birthdate"]
    gender = client["gender"]

    logging.info("Clicking new examinee button")
    loop = True
    while loop:
        try:
            utils.click_element(driver, By.ID, "searchForm:newExamineeButton")
            loop = False
        except:  # noqa: E722
            logging.info("Failed to click new examinee, trying again.")
            driver.refresh()

    logging.info("Entering first name")
    first = driver.find_element(By.ID, "firstName")

    logging.info("Entering last name")
    last = driver.find_element(By.ID, "lastName")

    logging.info("Entering examinee id")
    examineeID = driver.find_element(By.ID, "examineeId")

    logging.info("Entering birthdate")
    birth = driver.find_element(By.ID, "calendarInputDate")
    first.send_keys(firstname)
    last.send_keys(lastname)
    examineeID.send_keys(id)

    logging.info("Selecting gender")
    purpose_element = driver.find_element(By.ID, "genderMenu")
    purpose = Select(purpose_element)
    sleep(1)
    if gender == "Male":
        purpose.select_by_visible_text("Male")
    elif gender == "Female":
        purpose.select_by_visible_text("Female")
    else:
        print("edge case")

    logging.info("Entering birthdate")
    dob = rearrangedob(dob)
    birth.send_keys(dob)

    logging.info("Saving new examinee")
    utils.click_element(driver, By.ID, "save")
    try:
        logging.info("Checking if client already exists")
        error = driver.find_element(By.NAME, "j_id201")
        exists = True
    except:  # noqa: E722
        logging.info("Client doesn't exist")
        exists = False
        return 0
    if error:
        sleep(3)
        # TODO: log this, I'm not sure what conditions cause this
        try:
            utils.click_element(driver, By.NAME, "j_id201")
            utils.click_element(driver, By.ID, "j_id182")
            utils.click_element(driver, By.ID, "unSavedChangeForm:YesUnSavedChanges")
        except:  # noqa: E722
            try:
                utils.click_element(driver, By.NAME, "j_id209")
            except:  # noqa: E722
                exists = False
    sleep(2)
    return exists


def add_client_to_mhs(driver, actions, client, questionnaire):
    logging.info(f"Atempting to add {client['firstname']} {client['lastname']} to MHS")
    firstname = client["firstname"]
    lastname = client["lastname"]
    id = client["account_number"]
    dob = client["birthdate"]
    gender = client["gender"]
    utils.click_element(
        driver, By.XPATH, "//div[@class='pull-right']//input[@type='submit']"
    )

    firstname_label = driver.find_element(By.XPATH, "//label[text()='FIRST NAME']")

    logging.info("Entering first name")
    firstname_field = firstname_label.find_element(
        By.XPATH, "./following-sibling::input"
    )
    firstname_field.send_keys(firstname)

    lastname_label = driver.find_element(By.XPATH, "//label[text()='LAST NAME']")

    logging.info("Entering last name")
    lastname_field = lastname_label.find_element(By.XPATH, "./following-sibling::input")
    lastname_field.send_keys(lastname)

    id_label = driver.find_element(By.XPATH, "//label[text()='ID']")

    logging.info("Entering ID")
    id_field = id_label.find_element(By.XPATH, "./following-sibling::input")
    id_field.send_keys(id)

    logging.info("Entering birthdate")
    date_of_birth_field = driver.find_element(
        By.CSS_SELECTOR, "input[placeholder='YYYY/Mmm/DD']"
    )
    date_of_birth_field.send_keys(dob)

    if questionnaire == "Conners EC" or questionnaire == "ASRS":
        logging.info("Selecting gender")
        male_label = driver.find_element(By.XPATH, "//label[text()='Male']")
        female_label = driver.find_element(By.XPATH, "//label[text()='Female']")
        if gender == "Male":
            male_label.click()
        else:
            female_label.click()
    else:
        logging.info("Selecting gender")
        gender_element = driver.find_element(
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

    logging.info("Selecting purpose")
    purpose_element = driver.find_element(
        By.CSS_SELECTOR, "select[placeholder='Select an option']"
    )
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Psychoeducational Evaluation")

    logging.info("Saving")
    utils.click_element(driver, By.CSS_SELECTOR, ".pull-right > input[type='submit']")
    try:
        error = driver.find_element(
            By.XPATH,
            "//span[contains(text(), 'A client with the same ID already exists')]",
        )
    except:  # noqa: E722
        logging.info("No error")
        return 0
    if error:
        logging.info("Client already exists, adding to existing")
        utils.click_element(
            driver,
            By.XPATH,
            "//span[contains(normalize-space(text()), 'My Assessments')]",
        )
        logging.info(f"Selecting {questionnaire}")
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
            search = driver.find_element(
                By.ID,
                "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_SelectClient_clientSearchBox_Input",
            )
        else:
            search = driver.find_element(
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_SelectClient_clientSearchBox_Input']",
            )

        logging.info("Searching for client")
        search.send_keys(id)
        actions.send_keys(Keys.ENTER)
        actions.perform()
        sleep(1)
        if questionnaire == "ASRS":
            logging.info("Selecting client")
            utils.click_element(
                driver,
                By.XPATH,
                "//tr[@id='ctrlControls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_SelectClient_gdClients_ctl00__0']/td[2]",
            )

            logging.info("Submitting")
            utils.click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext']",
            )
        else:
            logging.info("Selecting client")
            utils.click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_SelectClient_gdClients_ctl00_ctl04_ClientSelectSelectCheckBox']",
            )

            logging.info("Submitting")
            utils.click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_btnNext']",
            )

        logging.info("Selecting purpose")
        purpose_element = driver.find_element(
            By.CSS_SELECTOR, "select[placeholder='Select an option']"
        )
        purpose = Select(purpose_element)
        sleep(1)
        purpose.select_by_visible_text("Psychoeducational Evaluation")
        if questionnaire == "ASRS":
            logging.info("Submitting")
            utils.click_element(
                driver,
                By.ID,
                "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext",
            )
        else:
            logging.info("Submitting")
            utils.click_element(
                driver,
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext']",
            )


def get_questionnaires(age, check, daeval, vineland):
    if daeval == "EVAL":
        if check == "ASD":
            if age < 2:  # 1.5
                return "Too young"
            elif age < 6:
                qs = ["DP4", "BASC Preschool", "Conners EC"]
                if vineland:
                    qs.append("ASRS (2-5 Years)")
                else:
                    qs.append("Vineland")
                return qs
            elif age < 12:
                qs = ["BASC Child", "Conners 4"]
                if vineland:
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
                if vineland:
                    qs.append("ASRS (6-18 Years)")
                else:
                    qs.append("Vineland")
                return qs
            elif age < 19:
                qs = ["ABAS 3", "BASC Adolescent", "PAI", "CAARS 2"]
                if vineland:
                    qs.append("ASRS (6-18 Years)")
                else:
                    qs.append("Vineland")
            elif age < 22:
                return ["ABAS 3", "BASC Adolescent", "SRS-2", "CAARS 2", "PAI"]
            else:
                return ["ABAS 3", "SRS-2", "CAARS 2", "PAI"]
        else:
            return
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
                "Vineland",
                "DP4",
                "BASC Preschool",
                "Conners EC",
            ]
        elif age < 7:
            return ["ASRS (6-18 Years)", "Vineland", "BASC Child", "Conners 4"]
        elif age < 12:
            return [
                "ASRS (6-18 Years)",
                "Vineland",
                "BASC Child",
                "Conners 4",
            ]
        elif age < 18:
            return [
                "ASRS (6-18 Years)",
                "Vineland",
                "BASC Adolescent",
                "Conners 4 Self",
                "Conners 4",
            ]
        elif age < 19:
            return [
                "ASRS (6-18 Years)",
                "Vineland",
                "ABAS 3",
                "BASC Adolescent",
                "PAI",
                "CAARS 2",
            ]
        elif age < 22:
            return ["SRS Self", "ABAS 3", "BASC Adolescent", "SRS-2", "CAARS 2", "PAI"]
        else:
            return ["SRS Self", "ABAS 3", "SRS-2", "CAARS 2", "PAI"]


def assign_questionnaire(driver, actions, client, questionnaire):
    logging.info(
        f"Assigning questionnaire '{questionnaire}' to client {client['firstname']} {client['lastname']}"
    )
    mhs_url = "https://assess.mhs.com/MainPortal.aspx"
    qglobal_url = "https://qglobal.pearsonassessments.com/qg/searchExaminee.seam"
    wps_url = "https://platform.wpspublish.com/administration/details/4116148"

    if questionnaire == "Conners EC":
        logging.info(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_conners_ec(driver, actions, client)
    elif questionnaire == "Conners 4":
        logging.info(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_conners_4(driver, actions, client)
    elif questionnaire == "Conners 4 Self":
        logging.info(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_conners_4_self(driver, actions, client)
    elif questionnaire == "BASC Preschool":
        logging.info(f"Navigating to QGlobal and adding client for {questionnaire}")
        driver.get(qglobal_url)
        add_client_to_qglobal(driver, actions, client)
        return gen_basc_preschool(driver, actions, client)
    elif questionnaire == "BASC Child":
        logging.info(f"Navigating to QGlobal and adding client for {questionnaire}")
        driver.get(qglobal_url)
        add_client_to_qglobal(driver, actions, client)
        return gen_basc_child(driver, actions, client)
    elif questionnaire == "BASC Adolescent":
        logging.info(f"Navigating to QGlobal and adding client for {questionnaire}")
        driver.get(qglobal_url)
        add_client_to_qglobal(driver, actions, client)
        return gen_basc_adolescent(driver, actions, client)
    elif questionnaire == "ASRS (2-5 Years)":
        logging.info(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_asrs_2_5(driver, actions, client)
    elif questionnaire == "ASRS (6-18 Years)":
        logging.info(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_asrs_6_18(driver, actions, client)
    elif questionnaire == "Vineland":
        logging.info(f"Navigating to QGlobal for {questionnaire}")
        driver.get(qglobal_url)
        return gen_vineland(driver, actions, client)
    elif questionnaire == "CAARS 2":
        logging.info(f"Navigating to MHS for {questionnaire}")
        driver.get(mhs_url)
        return gen_caars_2(driver, actions, client)
    elif questionnaire == "DP4":
        logging.info(f"Navigating to WPS for {questionnaire}")
        driver.get(wps_url)
        return gen_dp4(driver, actions, client)


def gen_dp4(driver, actions, client):
    logging.info(f"Generating DP4 for {client['firstname']} {client['lastname']}")
    firstname = client["firstname"]
    lastname = client["lastname"]
    id = client["account_number"]
    dob = client["birthdate"]
    gender = client["gender"]
    utils.click_element(driver, By.ID, "newCase")

    first = driver.find_element(By.XPATH, "//td[@id='FirstName']/input")
    last = driver.find_element(By.XPATH, "//td[@id='LastName']/input")
    account = driver.find_element(By.XPATH, "//td[@id='CaseAltId']/input")

    logging.info("Entering first name")
    first.send_keys(firstname)
    logging.info("Entering last name")
    last.send_keys(lastname)
    logging.info("Entering account number")
    account.send_keys(id)

    logging.info("Selecting gender")
    purpose_element = driver.find_element(By.ID, "genderOpt")
    purpose = Select(purpose_element)
    sleep(1)
    if gender == "Male":
        purpose.select_by_visible_text("Male")
    else:
        purpose.select_by_visible_text("Female")

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

    logging.info("Selecting birthdate")
    purpose_element = driver.find_element(By.ID, "dobMonth")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text(month)
    purpose_element = driver.find_element(By.ID, "dobDay")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text(day)
    purpose_element = driver.find_element(By.ID, "dobYear")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text(year)

    logging.info("Saving new client")
    utils.click_element(driver, By.ID, "clientSave")

    logging.info("Confirming new client")
    utils.click_element(driver, By.XPATH, "//input[@id='successClientCreate']")

    logging.info("Navigating to client list")
    driver.get("https://platform.wpspublish.com")
    search = driver.find_element(By.XPATH, "//input[@type='search']")

    logging.info("Searching for client")
    search.send_keys(firstname, " ", lastname)

    logging.info("Selecting client")
    utils.click_element(driver, By.XPATH, "//table[@id='case']/tbody/tr/td/div")

    logging.info("Creating new administration")
    utils.click_element(driver, By.XPATH, "//input[@id='newAdministration']")

    logging.info("Selecting test")
    utils.click_element(
        driver,
        By.XPATH,
        "//img[contains(@src,'https://oes-cdn01.wpspublish.com/content/img/DP-4.png')]",
    )

    logging.info("Adding form")
    utils.click_element(driver, By.ID, "addForm")
    form_element = driver.find_element(By.ID, "TestFormId")
    form = Select(form_element)
    sleep(1)

    logging.info("Selecting form")
    form.select_by_visible_text("Parent/Caregiver Checklist")

    logging.info("Setting delivery method")
    utils.click_element(driver, By.ID, "DeliveryMethod")

    logging.info("Entering rater name")
    driver.find_element(By.ID, "RaterName").send_keys("Parent/Caregiver")

    logging.info("Entering email")
    driver.find_element(By.ID, "RemoteAdminEmail_ToEmail").send_keys(
        "maddy@driftwoodeval.com"
    )

    logging.info("Selecting copy me")
    utils.click_element(driver, By.ID, "RemoteAdminEmail_CopyMe")

    logging.info("Pretending to send form")
    utils.click_element(driver, By.XPATH, "//input[@value='Send Form']")

    logging.info("Selecting form link")
    utils.click_element(
        driver, By.XPATH, "//td[contains(.,'Parent/Caregiver Checklist')]"
    )

    logging.info("Selecting delivery method")
    utils.click_element(driver, By.ID, "DeliveryMethod")
    sleep(3)

    logging.info("Getting form link")
    body = driver.find_element(By.ID, "RemoteAdminEmail_Content").get_attribute("value")
    body = body.split()
    body = body[3]
    link = body[6:-1]

    logging.info(f"Returning link {link}")
    return link


def gen_conners_ec(driver, actions, client):
    logging.info(
        f"Generating Conners EC for {client['firstname']} {client['lastname']}"
    )
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logging.info("Selecting Conners EC")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'Conners EC')]"
    )

    logging.info("Selecting Email Invitation")
    utils.click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    add_client_to_mhs(driver, actions, client, "Conners EC")

    logging.info("Selecting assessment description")
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)

    logging.info("Selecting Conners EC")
    purpose.select_by_visible_text("Conners EC")

    logging.info("Selecting rater type")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    sleep(1)

    logging.info("Selecting Parent")
    purpose.select_by_visible_text("Parent")

    logging.info("Selecting language")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    sleep(1)

    logging.info("Selecting English")
    purpose.select_by_visible_text("English")

    logging.info("Entering rater name")
    driver.find_element(By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logging.info("Selecting next")
    utils.click_element(driver, By.ID, "_btnnext")

    logging.info("Selecting generate link")
    utils.click_element(driver, By.ID, "btnGenerateLinks")
    sleep(3)

    logging.info("Getting link")
    link = driver.find_element(By.ID, "txtLink").get_attribute("value")

    logging.info(f"Returning link {link}")
    return link


def gen_conners_4(driver, actions, client):
    logging.info(f"Generating Conners 4 for {client['firstname']} {client['lastname']}")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logging.info("Selecting Conners 4")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'Conners 4')]"
    )

    logging.info("Selecting Email Invitation")
    utils.click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    logging.info("Adding client to MHS")
    add_client_to_mhs(driver, actions, client, "Conners 4")

    logging.info("Selecting assessment description")
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Conners 4")

    logging.info("Selecting rater type")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Parent")

    logging.info("Selecting language")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("English")

    logging.info("Entering rater name")
    driver.find_element(By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logging.info("Selecting next")
    utils.click_element(driver, By.ID, "_btnnext")

    logging.info("Selecting generate link")
    utils.click_element(driver, By.ID, "btnGenerateLinks")
    sleep(3)
    link = driver.find_element(By.ID, "txtLink").get_attribute("value")

    logging.info(f"Returning link {link}")
    return link


def gen_conners_4_self(driver, actions, client):
    logging.info(f"Generating Conners 4 for {client['firstname']} {client['lastname']}")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logging.info("Selecting Conners 4")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'Conners 4')]"
    )

    logging.info("Selecting Email Invitation")
    utils.click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    logging.info("Adding client to MHS")
    add_client_to_mhs(driver, actions, client, "Conners 4")

    logging.info("Selecting assessment description")
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)

    logging.info("Selecting Conners 4")
    purpose.select_by_visible_text("Conners 4")

    logging.info("Selecting rater type")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    sleep(1)

    logging.info("Selecting Self-Report")
    purpose.select_by_visible_text("Self-Report")

    logging.info("Selecting language")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    sleep(1)

    logging.info("Selecting English")
    purpose.select_by_visible_text("English")

    logging.info("Entering rater name")
    driver.find_element(By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logging.info("Selecting next")
    utils.click_element(driver, By.ID, "_btnnext")

    logging.info("Selecting generate link")
    utils.click_element(driver, By.ID, "btnGenerateLinks")
    sleep(3)
    link = driver.find_element(By.ID, "txtLink").get_attribute("value")

    logging.info(f"Returning link {link}")
    return link


def gen_asrs_2_5(driver, actions, client):
    logging.info(
        f"Generating ASRS (2-5 Years) for {client['firstname']} {client['lastname']}"
    )
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    )

    logging.info("Selecting ASRS")
    utils.click_element(
        driver, By.XPATH, "//span[contains(normalize-space(text()), 'ASRS')]"
    )

    logging.info("Selecting Email Invitation")
    utils.click_element(
        driver, By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    )

    logging.info("Adding client to MHS")
    add_client_to_mhs(driver, actions, client, "ASRS")

    logging.info("Selecting assessment description")
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("ASRS (2-5 Years)")

    logging.info("Selecting rater type")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Parent")

    logging.info("Selecting language")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("English")

    logging.info("Entering rater name")
    driver.find_element(By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logging.info("Selecting next")
    utils.click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext",
    )

    logging.info("Generating link")
    utils.click_element(
        driver,
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_btnGenerateLinks",
    )
    sleep(3)
    link = driver.find_element(
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_rptraters_txtLink_0",
    ).get_attribute("value")

    logging.info(f"Returning link {link}")
    return link


def gen_asrs_6_18(driver, actions, client):
    logging.info(
        f"Generating ASRS (6-18 Years) for {client['firstname']} {client['lastname']}"
    )
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    ).click()

    logging.info("Selecting ASRS")
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'ASRS')]"
    ).click()

    logging.info("Selecting Email Invitation")
    driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    ).click()

    logging.info("Adding client to MHS")
    add_client_to_mhs(driver, actions, client, "ASRS")
    sleep(1)

    logging.info("Selecting assessment description")
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    purpose.select_by_visible_text("ASRS (6-18 Years)")
    sleep(1)

    logging.info("Selecting rater type")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    purpose.select_by_visible_text("Parent")
    sleep(1)

    logging.info("Selecting language")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    purpose.select_by_visible_text("English")

    logging.info("Entering rater name")
    driver.find_element(By.ID, "txtRaterName").send_keys("Parent/Caregiver")

    logging.info("Selecting next")
    driver.find_element(
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext",
    ).click()

    logging.info("Generating link")
    driver.find_element(
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_btnGenerateLinks",
    ).click()
    sleep(3)
    link = driver.find_element(
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_rptraters_txtLink_0",
    ).get_attribute("value")

    logging.info(f"Returning link {link}")
    return link


def gen_basc_preschool(driver, actions, client):
    logging.info(
        f"Generating BASC Preschool for {client['firstname']} {client['lastname']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logging.info("Selecting client")
    utils.click_element(driver, By.XPATH, "//tr[2]/td[5]")

    logging.info("Clicking add assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logging.info("Selecting BASC Preschool")
    utils.click_element(driver, By.ID, "2600_radio")

    logging.info("Assigning assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:assignAssessmentBtn")

    logging.info("Selecting send via email")
    driver.find_element(
        By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    ).click()

    logging.info("Entering respondent first name")
    driver.find_element(By.ID, "respondentFirstName").send_keys("M")

    logging.info("Entering respondent last name")
    driver.find_element(By.ID, "respondentLastName").send_keys("P")

    logging.info("Clicking continue to email")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    sleep(5)

    logging.info("Clicking create e-mail")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Create e-mail')]")
    driver.switch_to.frame(
        driver.find_element(By.XPATH, "//iframe[@title='Editor, editor1']")
    )
    link = driver.find_element(By.CSS_SELECTOR, "a").get_attribute("href")

    driver.switch_to.default_content()

    logging.info(f"Returning link {link}")
    return link


def gen_basc_child(driver, actions, client):
    logging.info(
        f"Generating BASC Child for {client['firstname']} {client['lastname']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logging.info("Selecting client")
    utils.click_element(driver, By.XPATH, "//tr[2]/td[5]")

    logging.info("Clicking add assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logging.info("Selecting BASC Child")
    utils.click_element(driver, By.ID, "2598_radio")

    logging.info("Assigning assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:assignAssessmentBtn")

    logging.info("Selecting send via email")
    driver.find_element(
        By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    ).click()

    logging.info("Entering respondent first name")
    driver.find_element(By.ID, "respondentFirstName").send_keys("M")

    logging.info("Entering respondent last name")
    driver.find_element(By.ID, "respondentLastName").send_keys("P")

    logging.info("Clicking continue to email")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    sleep(5)

    logging.info("Clicking create e-mail")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Create e-mail')]")
    driver.switch_to.frame(
        driver.find_element(By.XPATH, "//iframe[@title='Editor, editor1']")
    )
    link = driver.find_element(By.CSS_SELECTOR, "a").get_attribute("href")

    driver.switch_to.default_content()

    logging.info(f"Returning link {link}")
    return link


def gen_basc_adolescent(driver, actions, client):
    logging.info(
        f"Generating BASC Adolescent for {client['firstname']} {client['lastname']}"
    )
    search_qglobal(driver, actions, client)
    sleep(3)

    logging.info("Selecting client")
    utils.click_element(driver, By.XPATH, "//tr[2]/td[5]")

    logging.info("Clicking add assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logging.info("Selecting BASC Adolescent")
    utils.click_element(driver, By.ID, "2596_radio")

    logging.info("Assigning assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:assignAssessmentBtn")

    logging.info("Selecting send via email")
    driver.find_element(
        By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    ).click()

    logging.info("Entering respondent first name")
    driver.find_element(By.ID, "respondentFirstName").send_keys("M")

    logging.info("Entering respondent last name")
    driver.find_element(By.ID, "respondentLastName").send_keys("P")

    logging.info("Clicking continue to email")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    sleep(5)

    logging.info("Clicking create e-mail")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Create e-mail')]")
    driver.switch_to.frame(
        driver.find_element(By.XPATH, "//iframe[@title='Editor, editor1']")
    )
    link = driver.find_element(By.CSS_SELECTOR, "a").get_attribute("href")

    driver.switch_to.default_content()

    logging.info(f"Returning link {link}")
    return link


def gen_vineland(driver, actions, client):
    logging.info(f"Generating Vineland for {client['firstname']} {client['lastname']}")
    logging.info("Searching QGlobal")
    search_qglobal(driver, actions, client)
    sleep(3)

    logging.info("Selecting client")
    utils.click_element(driver, By.XPATH, "//tr[2]/td[5]")

    logging.info("Clicking add assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:add_assessment")

    logging.info("Selecting Vineland assessment")
    utils.click_element(driver, By.ID, "2728_radio")

    logging.info("Assigning assessment")
    utils.click_element(driver, By.ID, "examAssessTabFormId:assignAssessmentBtn")

    logging.info("Selecting send via email")
    driver.find_element(
        By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    ).click()

    logging.info("Entering respondent first name")
    driver.find_element(By.ID, "respondentFirstName").send_keys("M")

    logging.info("Entering respondent last name")
    driver.find_element(By.ID, "respondentLastName").send_keys("P")

    logging.info("Continuing to email step")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    logging.info("Selecting email options")
    utils.click_element(driver, By.XPATH, "//div/div[2]/label")
    driver.find_element(
        By.XPATH,
        "//div[2]/qg2-multi-column-layout/div/section[2]/div/qg2-form-radio-button/div/div/section[2]/div/div[2]/label",
    ).click()

    logging.info("Clicking continue to email")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Continue to E-mail')]")

    sleep(5)

    logging.info("Clicking create e-mail")
    utils.click_element(driver, By.XPATH, "//button[contains(.,'Create e-mail')]")
    driver.switch_to.frame(
        driver.find_element(By.XPATH, "//iframe[@title='Editor, editor1']")
    )
    link = driver.find_element(By.CSS_SELECTOR, "a").get_attribute("href")

    driver.switch_to.default_content()

    logging.info(f"Returning link {link}")
    return link


def gen_caars_2(driver, actions, client):
    logging.info(f"Generating CAARS 2 for {client['firstname']} {client['lastname']}")
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    ).click()

    logging.info("Selecting CAARS 2")
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'CAARS 2')]"
    ).click()

    logging.info("Selecting Email Invitation")
    driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    ).click()

    logging.info("Adding client to MHS")
    add_client_to_mhs(driver, actions, client, "CAARS 2")

    logging.info("Selecting assessment description")
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("CAARS 2")

    logging.info("Selecting rater type")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Self-Report")

    logging.info("Selecting language")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("English")

    logging.info("Selecting next")
    utils.click_element(driver, By.ID, "_btnnext")

    logging.info("Generating link")
    utils.click_element(driver, By.ID, "btnGenerateLinks")
    sleep(5)
    link = driver.find_element(
        By.NAME,
        "ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx$CreateLink$txtLink",
    ).get_attribute("value")

    logging.info(f"Returning link {link}")
    return link


def search_clients(driver, actions, firstname, lastname):
    logging.info(f"Searching for {firstname} {lastname}")
    sleep(2)

    logging.info("Trying to escape random popups")
    actions.send_keys(Keys.ESCAPE)
    actions.perform()
    logging.info("Entering first name")
    firstname_label = driver.find_element(By.XPATH, "//label[text()='First Name']")
    firstname_field = firstname_label.find_element(
        By.XPATH, "./following-sibling::input"
    )
    firstname_field.send_keys(firstname)

    logging.info("Entering last name")
    lastname_label = driver.find_element(By.XPATH, "//label[text()='Last Name']")
    lastname_field = lastname_label.find_element(By.XPATH, "./following-sibling::input")
    lastname_field.send_keys(lastname)

    logging.info("Clicking search")
    search_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Search'")
    search_button.click()


def go_to_client(driver, actions, firstname, lastname):
    driver.get("https://portal.therapyappointment.com")
    sleep(1)
    logging.info("Navigating to Clients section")
    clients_button = driver.find_element(
        By.XPATH, value="//*[contains(text(), 'Clients')]"
    )
    clients_button.click()

    for _ in range(3):
        try:
            search_clients(driver, actions, firstname, lastname)
            break
        except Exception as e:
            logging.info(f"Failed to search: {e}, trying again")
            driver.refresh()

    sleep(1)

    logging.info("Selecting client profile")

    try:
        driver.find_element(
            By.CSS_SELECTOR,
            "a[aria-description*='Press Enter to view the profile of",
        ).click()
    except Exception as e:
        logging.info(f"Failed to select client: {e}, trying again")
        driver.refresh()
        go_to_client(driver, actions, firstname, lastname)

    current_url = driver.current_url
    logging.info(f"Navigated to client profile: {current_url}")
    return current_url


def extract_client_data(driver):
    logging.info("Attempting to extract client data")
    name = driver.find_element(By.CLASS_NAME, "text-h4").text
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
    account_number_element = driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'Account #')]"
    ).text
    account_number = account_number_element.split(" ")[-1]
    birthdate_element = driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'DOB ')]"
    ).text
    birthdate_str = birthdate_element.split(" ")[-1]
    birthdate = strftime("%Y/%m/%d", strptime(birthdate_str, "%m/%d/%Y"))
    phone_number_element = driver.find_element(
        By.CSS_SELECTOR, "a[aria-description=' current default phone'"
    )
    sleep(0.5)
    phone_number = phone_number_element.text
    gender_title_element = driver.find_element(
        By.XPATH,
        "//div[contains(normalize-space(text()), 'Gender') and contains(@class, 'v-list-item__title')]",
    )
    gender_element = gender_title_element.find_element(
        By.XPATH, "following-sibling::div"
    )
    sleep(0.5)
    gender = gender_element.text.split(" ")[0]

    age = relativedelta(datetime.now(), datetime.strptime(birthdate, "%Y/%m/%d")).years
    logging.info("Returned client data")
    return {
        "firstname": firstname,
        "lastname": lastname,
        "account_number": account_number,
        "birthdate": birthdate,
        "gender": gender,
        "age": age,
        "phone_number": phone_number,
    }


def format_ta_message(questionnaires):
    logging.info("Formatting TA message")
    message = ""
    for id, questionnaire in enumerate(questionnaires, start=1):
        notes = ""
        if "Self" in questionnaire["type"]:
            notes = " - For client being tested"
        message += f"{id}) {questionnaire['link']}{notes}\n"
    logging.info("Finished formatting TA message")
    return message


def send_message_ta(driver, client_url, message):
    logging.info("Navigating to client URL")
    driver.get(client_url)

    logging.info("Accessing Messages section")
    driver.find_element(
        By.XPATH, "//a[contains(normalize-space(text()), 'Messages')]"
    ).click()

    logging.info("Initiating new message")
    driver.find_element(
        By.XPATH,
        "//div[2]/section/div/a/span/span",
    ).click()
    sleep(1)

    logging.info("Setting message subject")
    driver.find_element(By.ID, "message_thread_subject").send_keys(
        "Please complete the link(s) below"
    )
    sleep(1)

    logging.info("Entering message content")
    text_field = driver.find_element(By.XPATH, "//section/div/div[3]")
    text_field.click()
    sleep(1)
    text_field.send_keys(message)
    sleep(1)

    logging.info("Submitting the message")
    text_field.click()
    utils.click_element(driver, By.CSS_SELECTOR, "button[type='submit']")


def format_client(client):
    account_number = client["account_number"]
    return {account_number: client}


def format_failed_client(client_params):
    client_info = {
        "check": client_params["check"],
        "daeval": client_params["daeval"],
        "date": client_params["date"],
    }
    return {f"{client_params['firstname']} {client_params['lastname']}": client_info}


def write_file(filepath, data):
    data = data.strip("\n")
    try:
        logging.info(f"Opening file {filepath} for reading")
        with open(filepath, "r") as file:
            existing_content = file.read().strip("\n")
            if data == existing_content or data in existing_content.split(", "):
                logging.info("Data already exists in file, skipping write")
                return
            new_content = (
                data if not existing_content else f"{existing_content}, {data}"
            )
        logging.info(f"Opening file {filepath} for writing")
        with open(filepath, "w") as file:
            file.write(new_content)
            logging.info("Wrote new content to file")
    except FileNotFoundError:
        logging.warning(f"File {filepath} not found, creating new file")
        with open(filepath, "w") as file:
            file.write(data)
            logging.info("Wrote data to new file")


def check_client_in_yaml(prev_clients, client_info):
    if prev_clients is None:
        return False
    account_number = client_info.get("account_number")

    if account_number and isinstance(prev_clients, dict):
        if account_number in prev_clients:
            return prev_clients[account_number]["daeval"] == client_info.get("daeval")
    return False


def main():
    driver, actions = utils.initialize_selenium()
    projects_api = utils.init_asana(services)
    for login in [login_ta, login_wps, login_qglobal, login_mhs]:
        while True:
            try:
                login(driver, actions)
                sleep(1)
                break
            except Exception as e:
                logging.info(f"Login failed: {e}, trying again")
                sleep(1)
    clients = get_clients()
    prev_clients = utils.get_previous_clients()

    for client in clients:
        client_params = parameterize(client)

        logging.info(
            f"Starting loop for {client_params['firstname']} {client_params['lastname']}"
        )

        try:
            client_url = go_to_client(
                driver, actions, client_params["firstname"], client_params["lastname"]
            )
            client_info = extract_client_data(driver)
            combined_client_info = client_params | client_info
            client_already_ran = check_client_in_yaml(
                prev_clients, combined_client_info
            )
        except NoSuchElementException as e:
            logging.error(f"Element not found: {e}")
            utils.update_yaml(format_failed_client(client_params), "./put/qfailure.yml")
            break

        if client_already_ran:
            logging.warning(
                f"{client_params['firstname']} {client_params['lastname']} with {combined_client_info['daeval']} has already been run before, skipping."
            )
            continue

        write_file(
            "./put/records.txt",
            f"{client_params['firstname']} {client_params['lastname']} {client_params['date']}",
        )

        try:
            if (
                int(combined_client_info["age"]) < 19
                and client_params["daeval"] != "DA"
            ):
                driver.get(
                    "https://qglobal.pearsonassessments.com/qg/searchExaminee.seam"
                )
                vineland = add_client_to_qglobal(driver, actions, combined_client_info)
            else:
                vineland = False
            questionnaires = get_questionnaires(
                combined_client_info["age"],
                combined_client_info["check"],
                combined_client_info["daeval"],
                vineland,
            )
            client_add_q_field = {"questionnaires": []}
            combined_client_info: dict = combined_client_info | client_add_q_field
            formatted_client = format_client(combined_client_info)

            if str(questionnaires) == "Too young":
                logging.warning(
                    f"{formatted_client[client_info['account_number']]['firstname']} "
                    f"{formatted_client[client_info['account_number']]['lastname']} is too young at age "
                    f"{formatted_client[client_info['account_number']]['age']}"
                )
                formatted_client[client_info["account_number"]][
                    "questionnaires"
                ].append({"error": "Too young"})
                utils.update_yaml(formatted_client, "./put/qfailure.yml")
                break

            logging.info(
                f"Questionnaires needed for {formatted_client[client_info['account_number']]['firstname']} "
                f"{formatted_client[client_info['account_number']]['lastname']} for "
                f"a {formatted_client[client_info['account_number']]['check']} "
                f"{formatted_client[client_info['account_number']]['daeval']}: "
                f"{questionnaires}"
            )
            send = True
            for questionnaire in questionnaires:
                try:
                    link = assign_questionnaire(
                        driver, actions, combined_client_info, questionnaire
                    )
                except Exception as e:  # noqa: E722
                    logging.error(f"Error assigning {questionnaire}: {e}")
                    utils.update_yaml(formatted_client, "./put/qfailure.yml")
                    send = False
                    break

                if link is None or link == "":
                    utils.update_yaml(formatted_client, "./put/qfailure.yml")
                    send = False
                    break

                formatted_client[client_info["account_number"]][
                    "questionnaires"
                ].append({"done": False, "link": link, "type": questionnaire})

            if send:
                del formatted_client[client_info["account_number"]]["account_number"]
                formatted_client[client_info["account_number"]] = (
                    utils.search_and_add_questionnaires(
                        projects_api,
                        services,
                        config,
                        formatted_client[client_info["account_number"]],
                    )
                )
                utils.update_yaml(
                    formatted_client,
                    "./put/clients.yml",
                )
                message = format_ta_message(
                    formatted_client[client_info["account_number"]]["questionnaires"]
                )
                send_message_ta(driver, client_url, message)
        except NoSuchElementException as e:
            logging.error(f"Element not found: {e}")
            utils.update_yaml(format_failed_client(client_params), "./put/qfailure.yml")


main()

# TODO: do not try to add client accounts multiple times
