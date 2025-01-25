from datetime import datetime
from time import sleep, strftime, strptime

import pyperclip
import yaml
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

with open("./info.yml", "r") as file:
    info = yaml.safe_load(file)["services"]


def rearrangedob(dob):
    year = dob[0:4]
    month = dob[5:7]
    day = dob[8:10]
    return f"{month}/{day}/{year}"


def login_qglobal(driver, actions):
    driver.get("https://qglobal.pearsonassessments.com/")
    sleep(3)
    try:
        actions.send_keys(Keys.TAB)
        actions.send_keys(Keys.TAB)
        actions.send_keys(Keys.ENTER)
        actions.perform()
        username = driver.find_element(By.NAME, value="login:uname")
        password = driver.find_element(By.NAME, value="login:pword")
        username.send_keys(info["qglobal"]["username"])
        password.send_keys(info["qglobal"]["password"])
        password.send_keys(Keys.ENTER)
    except:  # noqa: E722
        pass


def search_fix(driver, id):
    try:
        sleep(1)
        driver.find_element(By.ID, "editExamineeForm:examineeId").send_keys(id)
    except:  # noqa: E722
        driver.get("https://qglobal.pearsonassessments.com")
        search = driver.find_element(By.NAME, value="searchForm:j_id347")
        search.click()
        search_fix(driver, id)


def search_qglobal(client, driver, actions):
    id = client["account_number"]
    search = driver.find_element(By.NAME, value="searchForm:j_id347")
    search.click()
    search_fix(driver, id)
    sleep(1)
    actions.send_keys(Keys.ENTER)
    actions.perform()


def login_mhs(driver, actions):
    driver.get("https://assess.mhs.com/Account/Login.aspx")
    username = driver.find_element(By.NAME, value="txtUsername")
    password = driver.find_element(By.NAME, value="txtPassword")
    username.send_keys(info["mhs"]["username"])
    password.send_keys(info["mhs"]["password"])
    actions.send_keys(Keys.ENTER)
    actions.perform()


def login_ta(driver, actions):
    driver.get("https://portal.therapyappointment.com")
    driver.maximize_window()
    actions.send_keys(info["therapyappointment"]["username"])
    actions.send_keys(Keys.TAB)
    actions.send_keys(info["therapyappointment"]["password"])
    actions.send_keys(Keys.ENTER)
    actions.perform()


def login_wps(driver, actions):
    driver.get("https://platform.wpspublish.com")
    driver.find_element(By.ID, "loginID").click()
    driver.find_element(By.ID, "Username").send_keys(info["wps"]["username"])
    driver.find_element(By.ID, "Password").send_keys(info["wps"]["password"])
    actions.send_keys(Keys.ENTER)
    actions.perform()


def start_dp4(client, driver, actions):
    firstname = client["firstname"]
    lastname = client["lastname"]
    id = client["account_number"]
    dob = client["birthdate"]
    gender = client["gender"]
    driver.find_element(By.ID, "newCase").click()
    first = driver.find_element(By.XPATH, "//td[@id='FirstName']/input")
    last = driver.find_element(By.XPATH, "//td[@id='LastName']/input")
    account = driver.find_element(By.XPATH, "//td[@id='CaseAltId']/input")
    first.send_keys(firstname)
    last.send_keys(lastname)
    account.send_keys(id)
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
    driver.find_element(By.ID, "clientSave").click()
    driver.find_element(By.XPATH, "//input[@id='successClientCreate']").click()
    driver.get("https://platform.wpspublish.com")
    search = driver.find_element(By.XPATH, "//input[@type='search']")
    search.send_keys(firstname, " ", lastname)
    driver.find_element(By.XPATH, "//table[@id='case']/tbody/tr/td/div").click()
    driver.find_element(By.XPATH, "//input[@id='newAdministration']").click()
    driver.find_element(
        By.XPATH,
        "//img[contains(@src,'https://oes-cdn01.wpspublish.com/content/img/DP-4.png')]",
    ).click()
    driver.find_element(By.ID, "addForm").click()
    form_element = driver.find_element(By.ID, "TestFormId")
    form = Select(form_element)
    sleep(1)
    form.select_by_visible_text("Parent/Caregiver Checklist")
    driver.find_element(By.ID, "DeliveryMethod").click()
    driver.find_element(By.ID, "RaterName").send_keys("Parent/Caregiver")
    driver.find_element(By.ID, "RemoteAdminEmail_ToEmail").send_keys(
        "maddy@driftwoodeval.com"
    )
    driver.find_element(By.ID, "RemoteAdminEmail_CopyMe").click()
    driver.find_element(By.XPATH, "//input[@value='Send Form']").click()
    driver.find_element(
        By.XPATH, "//td[contains(.,'Parent/Caregiver Checklist')]"
    ).click()
    driver.find_element(By.ID, "DeliveryMethod").click()
    sleep(3)
    body = driver.find_element(By.ID, "RemoteAdminEmail_Content").get_attribute("value")
    body = body.split()
    body = body[3]
    link = body[6:-1]
    print(link)
    return link


def go_to_client(firstname, lastname, driver, actions):
    clients_button = driver.find_element(
        By.XPATH, value="//*[contains(text(), 'Clients')]"
    )
    clients_button.click()

    sleep(2)

    actions.send_keys(Keys.ESCAPE)
    actions.perform()

    firstname_label = driver.find_element(By.XPATH, "//label[text()='First Name']")
    firstname_field = firstname_label.find_element(
        By.XPATH, "./following-sibling::input"
    )
    firstname_field.send_keys(firstname)

    lastname_label = driver.find_element(By.XPATH, "//label[text()='Last Name']")
    lastname_field = lastname_label.find_element(By.XPATH, "./following-sibling::input")
    lastname_field.send_keys(lastname)

    search_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Search'")
    search_button.click()

    driver.find_element(
        By.CSS_SELECTOR, "a[aria-description*='Press Enter to view the profile of"
    ).click()


def extract_client_data(driver):
    name = driver.find_element(By.CLASS_NAME, "text-h4").text
    firstname = name.split(" ")[0]
    lastname = name.split(" ")[-1]
    account_number_element = driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'Account #')]"
    ).text
    account_number = account_number_element.split(" ")[-1]
    birthdate_element = driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'DOB ')]"
    ).text
    birthdate_str = birthdate_element.split(" ")[-1]
    birthdate = strftime("%Y/%m/%d", strptime(birthdate_str, "%m/%d/%Y"))
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
    return {
        "firstname": firstname,
        "lastname": lastname,
        "account_number": account_number,
        "birthdate": birthdate,
        "gender": gender,
        "age": age,
    }


def get_questionnaires(age, check, daeval, vineland):
    if daeval == "EVAL":
        if check == "ASD":
            if age < 2:  # 1.5
                print("Too young, note Asana")
            elif age < 6:
                qs = ["DP4", "BASC Preschool", "Conners EC"]
                if vineland:
                    qs.append("ASRS (2-5 Years)")
                else:
                    qs.append("Vineland")
                return qs
            elif age < 7:
                qs = ["BASC Child", "Conners 4"]
                if vineland:
                    qs.append("ASRS (6-18 Years)")
                else:
                    qs.append("Vineland")
                return qs
            elif age < 12:
                qs = [
                    "BASC Child",
                    "Conners 4 Self",
                    "Conners 4",
                ]
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
                return ["ABAS 3", "BASC Adolescent", "PAI", "CAARS 2"]
            elif age < 22:
                return ["ABAS 3", "BASC Adolescent", "SRS-2", "CAARS 2", "PAI"]
            else:
                return ["ABAS 3", "SRS-2", "CAARS 2PAI"]
        else:
            return
    elif daeval == "DA":
        if check == "ASD":
            if age < 2:  # 1.5
                print("Too young, note ASANA")
            elif age < 6:
                return ["BASC Preschool"]
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
                print("Too young, note Asana")
            elif age < 6:
                return ["Conners EC"]
            elif age < 8:
                return ["Conners 4"]
            elif age < 18:
                return ["Conners 4", "Conners 4 Self"]
            else:
                return ["CAARS 2"]
    elif daeval == "DAEVAL":
        if age < 2:  # 1.5
            print("Too young, note ASANA")
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


def add_client_to_mhs(client, Q, driver, actions):
    firstname = client["firstname"]
    lastname = client["lastname"]
    id = client["account_number"]
    dob = client["birthdate"]
    gender = client["gender"]
    driver.find_element(
        By.XPATH, "//div[@class='pull-right']//input[@type='submit']"
    ).click()

    firstname_label = driver.find_element(By.XPATH, "//label[text()='FIRST NAME']")
    firstname_field = firstname_label.find_element(
        By.XPATH, "./following-sibling::input"
    )
    firstname_field.send_keys(firstname)

    lastname_label = driver.find_element(By.XPATH, "//label[text()='LAST NAME']")
    lastname_field = lastname_label.find_element(By.XPATH, "./following-sibling::input")
    lastname_field.send_keys(lastname)

    id_label = driver.find_element(By.XPATH, "//label[text()='ID']")
    id_field = id_label.find_element(By.XPATH, "./following-sibling::input")
    id_field.send_keys(id)

    date_of_birth_field = driver.find_element(
        By.CSS_SELECTOR, "input[placeholder='YYYY/Mmm/DD']"
    )
    date_of_birth_field.send_keys(dob)

    if Q == "Conners EC" or Q == "ASRS":
        male_label = driver.find_element(By.XPATH, "//label[text()='Male']")
        female_label = driver.find_element(By.XPATH, "//label[text()='Female']")
        if gender == "Male":
            male_label.click()
        else:
            female_label.click()
    else:
        gender_element = driver.find_element(
            By.CSS_SELECTOR,
            "select[aria-label*='Gender selection dropdown']",
        )
        gender_select = Select(gender_element)
        sleep(1)
        if gender == "Male":
            gender_select.select_by_visible_text("Male")
        elif gender == "Female":
            gender_select.select_by_visible_text("Female")
        else:
            gender_select.select_by_visible_text("Other")

    purpose_element = driver.find_element(
        By.CSS_SELECTOR, "select[placeholder='Select an option']"
    )
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Psychoeducational Evaluation")

    driver.find_element(By.CSS_SELECTOR, ".pull-right > input[type='submit']").click()
    try:
        error = driver.find_element(
            By.XPATH,
            "//span[contains(text(), 'A client with the same ID already exists')]",
        )
    except:  # noqa: E722
        return 0
    if error:
        driver.find_element(
            By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
        ).click()
        driver.find_element(
            By.XPATH, f"//span[contains(normalize-space(text()), '{Q}')]"
        ).click()
        driver.find_element(
            By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
        ).click()
        if Q == "ASRS":
            search = driver.find_element(
                By.ID,
                "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_SelectClient_clientSearchBox_Input",
            )
        else:
            search = driver.find_element(
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_SelectClient_clientSearchBox_Input']",
            )
        search.send_keys(id)
        actions.send_keys(Keys.ENTER)
        actions.perform()
        if Q == "ASRS":
            driver.find_element(
                By.XPATH,
                "//tr[@id='ctrlControls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_SelectClient_gdClients_ctl00__0']/td[2]",
            ).click()
            driver.find_element(
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext']",
            ).click()
        else:
            driver.find_element(
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_SelectClient_gdClients_ctl00_ctl04_ClientSelectSelectCheckBox']",
            ).click()
            driver.find_element(
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_btnNext']",
            ).click()
        purpose_element = driver.find_element(
            By.CSS_SELECTOR, "select[placeholder='Select an option']"
        )
        purpose = Select(purpose_element)
        sleep(1)
        purpose.select_by_visible_text("Psychoeducational Evaluation")
        if Q == "ASRS":
            driver.find_element(
                By.ID,
                "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext",
            ).click()
        else:
            driver.find_element(
                By.XPATH,
                "//input[@id='ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_ClientProfile_btnNext']",
            ).click()


def start_conners_ec(client, driver, actions):
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    ).click()
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'Conners EC')]"
    ).click()
    driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    ).click()
    add_client_to_mhs(client, "Conners EC", driver, actions)
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Conners EC")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Parent")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("English")
    driver.find_element(By.ID, "txtRaterName").send_keys("Parent/Caregiver")
    driver.find_element(By.ID, "_btnnext").click()
    driver.find_element(By.ID, "btnGenerateLinks").click()
    sleep(3)
    link = driver.find_element(By.ID, "txtLink").get_attribute("value")
    print(link)
    return link


def start_conners_4(client, driver, actions):
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    ).click()
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'Conners 4')]"
    ).click()
    driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    ).click()
    add_client_to_mhs(client, "Conners 4", driver, actions)
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Conners 4")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Parent")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("English")
    driver.find_element(By.ID, "txtRaterName").send_keys("Parent/Caregiver")
    driver.find_element(By.ID, "_btnnext").click()
    driver.find_element(By.ID, "btnGenerateLinks").click()
    sleep(3)
    link = driver.find_element(By.ID, "txtLink").get_attribute("value")
    print(link)
    return link


def start_conners_4_self(client, driver, actions):
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    ).click()
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'Conners 4')]"
    ).click()
    driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    ).click()
    add_client_to_mhs(client, "Conners 4", driver, actions)
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Conners 4")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Self-Report")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("English")
    driver.find_element(By.ID, "txtRaterName").send_keys("Parent/Caregiver")
    driver.find_element(By.ID, "_btnnext").click()
    driver.find_element(By.ID, "btnGenerateLinks").click()
    sleep(3)
    link = driver.find_element(By.ID, "txtLink").get_attribute("value")
    print(link)
    return link


def start_asrs_2_5(client, driver, actions):
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    ).click()
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'ASRS')]"
    ).click()
    driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    ).click()
    add_client_to_mhs(client, "ASRS", driver, actions)
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("ASRS (2-5 Years)")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Parent")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("English")
    driver.find_element(By.ID, "txtRaterName").send_keys("Parent/Caregiver")
    driver.find_element(
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext",
    ).click()
    driver.find_element(
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_btnGenerateLinks",
    ).click()
    sleep(3)
    link = driver.find_element(
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_rptraters_txtLink_0",
    ).get_attribute("value")
    print(link)
    return link


def start_asrs_6_18(client, driver, actions):
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    ).click()
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'ASRS')]"
    ).click()
    driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    ).click()
    add_client_to_mhs(client, "ASRS", driver, actions)
    sleep(1)
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    purpose.select_by_visible_text("ASRS (6-18 Years)")
    sleep(1)
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    purpose.select_by_visible_text("Parent")
    sleep(1)
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    purpose.select_by_visible_text("English")
    driver.find_element(By.ID, "txtRaterName").send_keys("Parent/Caregiver")
    driver.find_element(
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_btnNext",
    ).click()
    driver.find_element(
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_btnGenerateLinks",
    ).click()
    sleep(3)
    link = driver.find_element(
        By.ID,
        "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_CreateLink_rptraters_txtLink_0",
    ).get_attribute("value")
    print(link)
    return link


def add_client_to_qglobal(client, driver, actions):
    firstname = client["firstname"]
    lastname = client["lastname"]
    id = client["account_number"]
    dob = client["birthdate"]
    gender = client["gender"]
    driver.find_element(By.ID, "searchForm:newExamineeButton").click()
    first = driver.find_element(By.ID, "firstName")
    last = driver.find_element(By.ID, "lastName")
    examineeID = driver.find_element(By.ID, "examineeId")
    birth = driver.find_element(By.ID, "calendarInputDate")
    first.send_keys(firstname)
    last.send_keys(lastname)
    examineeID.send_keys(id)
    purpose_element = driver.find_element(By.ID, "genderMenu")
    purpose = Select(purpose_element)
    sleep(1)
    if gender == "Male":
        purpose.select_by_visible_text("Male")
    elif gender == "Female":
        purpose.select_by_visible_text("Female")
    else:
        print("edge case")
    dob = rearrangedob(dob)
    birth.send_keys(dob)
    driver.find_element(By.ID, "save").click()
    try:
        error = driver.find_element(By.NAME, "j_id201")
        exists = True
    except:  # noqa: E722
        exists = False
        return 0
    if error:
        sleep(3)
        try:
            driver.find_element(By.NAME, "j_id201").click()
            driver.find_element(By.ID, "j_id182").click()
            driver.find_element(By.ID, "unSavedChangeForm:YesUnSavedChanges").click()
        except:  # noqa: E722
            driver.find_element(By.NAME, "j_id209").click()
    sleep(2)
    return exists


def start_basc_preschool(client, driver, actions):
    search_qglobal(client, driver, actions)
    sleep(3)
    driver.find_element(By.XPATH, "//tr[2]/td[5]").click()
    driver.find_element(By.ID, "examAssessTabFormId:add_assessment").click()
    driver.find_element(By.ID, "2600_radio").click()
    driver.find_element(By.ID, "examAssessTabFormId:assignAssessmentBtn").click()
    driver.find_element(
        By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    ).click()
    driver.find_element(By.ID, "respondentFirstName").send_keys("M")
    driver.find_element(By.ID, "respondentLastName").send_keys("P")
    driver.find_element(By.XPATH, "//button[contains(.,'Continue to E-mail')]").click()
    sleep(5)
    driver.find_element(By.XPATH, "//button[contains(.,'Copy link')]").click()
    sleep(3)
    link = pyperclip.paste()
    sleep(2)
    print(link)
    return link


def start_basc_child(client, driver, actions):
    search_qglobal(client, driver, actions)
    sleep(3)
    driver.find_element(By.XPATH, "//tr[2]/td[5]").click()
    driver.find_element(By.ID, "examAssessTabFormId:add_assessment").click()
    driver.find_element(By.ID, "2598_radio").click()
    driver.find_element(By.ID, "examAssessTabFormId:assignAssessmentBtn").click()
    driver.find_element(
        By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    ).click()
    driver.find_element(By.ID, "respondentFirstName").send_keys("M")
    driver.find_element(By.ID, "respondentLastName").send_keys("P")
    driver.find_element(By.XPATH, "//button[contains(.,'Continue to E-mail')]").click()
    sleep(5)
    driver.find_element(By.XPATH, "//button[contains(.,'Copy link')]").click()
    link = pyperclip.paste()
    sleep(2)
    print(link)
    return link


def start_basc_adolescent(client, driver, actions):
    search_qglobal(client, driver, actions)
    sleep(3)
    driver.find_element(By.XPATH, "//tr[2]/td[5]").click()
    driver.find_element(By.ID, "examAssessTabFormId:add_assessment").click()
    driver.find_element(By.ID, "2596_radio").click()
    driver.find_element(By.ID, "examAssessTabFormId:assignAssessmentBtn").click()
    driver.find_element(
        By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    ).click()
    driver.find_element(By.ID, "respondentFirstName").send_keys("M")
    driver.find_element(By.ID, "respondentLastName").send_keys("P")
    driver.find_element(By.XPATH, "//button[contains(.,'Continue to E-mail')]").click()
    sleep(5)
    driver.find_element(By.XPATH, "//button[contains(.,'Copy link')]").click()
    link = pyperclip.paste()
    sleep(2)
    print(link)
    return link


def start_vineland(client, driver, actions):
    search_qglobal(client, driver, actions)
    sleep(3)
    driver.find_element(By.XPATH, "//tr[2]/td[5]").click()
    driver.find_element(By.ID, "examAssessTabFormId:add_assessment").click()
    driver.find_element(By.ID, "2728_radio").click()
    driver.find_element(By.ID, "examAssessTabFormId:assignAssessmentBtn").click()
    driver.find_element(
        By.XPATH, "//button[contains(.,'Send the assessment link via e-mail')]"
    ).click()
    driver.find_element(By.ID, "respondentFirstName").send_keys("M")
    driver.find_element(By.ID, "respondentLastName").send_keys("P")
    driver.find_element(By.XPATH, "//button[contains(.,'Continue to E-mail')]").click()
    driver.find_element(By.XPATH, "//div/div[2]/label").click()
    driver.find_element(
        By.XPATH,
        "//div[2]/qg2-multi-column-layout/div/section[2]/div/qg2-form-radio-button/div/div/section[2]/div/div[2]/label",
    ).click()
    driver.find_element(By.XPATH, "//button[contains(.,'Continue to E-mail')]").click()
    sleep(5)
    driver.find_element(By.XPATH, "//button[contains(.,'Copy link')]").click()
    link = pyperclip.paste()
    sleep(2)
    print(link)
    return link


def start_caars_2(client, driver, actions):
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'My Assessments')]"
    ).click()
    driver.find_element(
        By.XPATH, "//span[contains(normalize-space(text()), 'CAARS 2')]"
    ).click()
    driver.find_element(
        By.XPATH, "//div[contains(normalize-space(text()), 'Email Invitation')]"
    ).click()
    add_client_to_mhs(client, "CAARS 2", driver, actions)
    purpose_element = driver.find_element(By.ID, "ddl_Description")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("CAARS 2")
    purpose_element = driver.find_element(By.ID, "ddl_RaterType")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("Self-Report")
    purpose_element = driver.find_element(By.ID, "ddl_Language")
    purpose = Select(purpose_element)
    sleep(1)
    purpose.select_by_visible_text("English")
    driver.find_element(By.ID, "_btnnext").click()
    driver.find_element(By.ID, "btnGenerateLinks").click()
    sleep(5)
    link = driver.find_element(
        By.NAME,
        "ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx$CreateLink$txtLink",
    ).get_attribute("value")
    sleep(2)
    print(link)
    return link


def assign_questionnaire(questionnaire, client, driver, actions):
    if questionnaire == "Conners EC":
        login_mhs(driver, actions)
        return start_conners_ec(client, driver, actions)
    elif questionnaire == "Conners 4":
        login_mhs(driver, actions)
        return start_conners_4(client, driver, actions)
    elif questionnaire == "Conners 4 Self":
        login_mhs(driver, actions)
        return start_conners_4_self(client, driver, actions)
    elif questionnaire == "BASC Preschool":
        login_qglobal(driver, actions)
        add_client_to_qglobal(client, driver, actions)
        return start_basc_preschool(client, driver, actions)
    elif questionnaire == "BASC Child":
        login_qglobal(driver, actions)
        add_client_to_qglobal(client, driver, actions)
        return start_basc_child(client, driver, actions)
    elif questionnaire == "BASC Adolescent":
        login_qglobal(driver, actions)
        add_client_to_qglobal(client, driver, actions)
        return start_basc_adolescent(client, driver, actions)
    elif questionnaire == "ASRS (2-5 Years)":
        login_mhs(driver, actions)
        return start_asrs_2_5(client, driver, actions)
    elif questionnaire == "ASRS (6-18 Years)":
        login_mhs(driver, actions)
        return start_asrs_6_18(client, driver, actions)
    elif questionnaire == "Vineland":
        driver.get("https://qglobal.pearsonassessments.com/")
        return start_vineland(client, driver, actions)
    elif questionnaire == "CAARS 2":
        login_mhs(driver, actions)
        return start_caars_2(client, driver, actions)
    elif questionnaire == "DP4":
        login_wps(driver, actions)
        return start_dp4(client, driver, actions)


def send_one(driver, first, last, check, daeval):
    # Initialize
    actions = ActionChains(driver)
    driver.implicitly_wait(10)
    login_ta(driver, actions)
    go_to_client(first, last, driver, actions)
    client = extract_client_data(driver)
    vineland = False
    login_qglobal(driver, actions)
    vineland = add_client_to_qglobal(client, driver, actions)
    questionnaires = get_questionnaires(client["age"], check, daeval, vineland)

    global links
    links = []
    if questionnaires != 0 and questionnaires is not None:
        for questionnaire in questionnaires:
            links.append(assign_questionnaire(questionnaire, client, driver, actions))

    for i in range(len(links)):
        print(f"{i + 1}) {links[i]}")
    driver.close()


def main():
    driver = webdriver.Chrome()
    f = open("automation.txt", "r")
    appointments = f.read().split(",")

    for appointment in appointments:
        appointment = appointment.split(" ")
        first = appointment[0]
        last = appointment[1]
        if appointment[-1] == "T":
            check = "ADHD"
            daeval = "DA"
        elif appointment[-1] == "DAEVAL":
            check = "ASD"
            daeval = "DAEVAL"
        elif appointment[-1] == "DA":
            check = "ASD"
            daeval = "DA"
        else:
            check = "ASD"
            daeval = "EVAL"
        try:
            send_one(driver, first, last, check, daeval)
        except:  # noqa: E722
            driver.close()
            driver = webdriver.Chrome()
            with open("failures.txt", "a") as f:
                f.write(f"{first} {last} {check} {daeval} {links}, ")


main()
