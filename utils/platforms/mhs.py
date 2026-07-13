import contextlib
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
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import Select, WebDriverWait

from utils.custom_types import Services
from utils.selenium import (
    click_element,
    find_element,
    wait_for_page_load,
)


def login_mhs(driver: WebDriver, services: Services) -> None:
    """Log in to MHS."""
    actions = ActionChains(driver)
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
    services: Services,
    first_time: bool = False,
) -> None:
    """Check if logged in to MHS and log in if not."""
    mhs_url = "https://assess.mhs.com"
    if first_time:
        logger.debug("First time login to MHS, logging in now.")
        driver.get(mhs_url)
        login_mhs(driver, services)
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
        login_mhs(driver, services)


def _select_and_wait_for_postback(
    driver: WebDriver, dropdown: WebElement, text: str, timeout: int = 5
) -> None:
    """Selects a dropdown option and waits for the ASP.NET UpdatePanel
    postback it triggers to finish.

    Selecting the description dropdown repopulates the rater type dropdown
    via an async postback. That postback replaces the rater dropdown's DOM
    node, so a fixed sleep can still lose the race: it's long enough to
    *look* fine to Selenium (no exception), but the site silently discards
    the selection because it landed on the node mid-replacement. Waiting
    for `dropdown` to go stale confirms the postback actually happened.
    """
    Select(dropdown).select_by_visible_text(text)
    with contextlib.suppress(TimeoutException):
        WebDriverWait(driver, timeout).until(ec.staleness_of(dropdown))


def _select_description(driver: WebDriver, description: str, timeout: int = 10) -> None:
    """Selects an option in ddl_Description, waiting for it to actually
    exist first.

    ddl_Description is itself populated asynchronously - e.g. filtered down
    to what's valid for the client's age - after the add/search-client flow
    completes. The existing-client path involves more postback round trips
    (search, age-mismatch correction, confirm) than adding a new client, so
    a fixed sleep before grabbing the dropdown can lose that race even
    though the option shows up moments later.
    """
    WebDriverWait(driver, timeout).until(
        ec.presence_of_element_located(
            (
                By.XPATH,
                f"//select[@id='ddl_Description']/option[normalize-space(text())='{description}']",
            )
        )
    )
    dropdown = find_element(driver, By.ID, "ddl_Description")
    _select_and_wait_for_postback(driver, dropdown, description)


def _find_link_in_pending_invitations(
    driver: WebDriver,
    services: Services,
    client: pd.Series,
    description: str,
    rater_type: str,
) -> str | None:
    """Fallback for when the invite wizard's own link field comes back
    empty even though the invitation was actually created: looks the
    invitation up on the Pending Invitations page, which lists its link too.

    The page is often frozen (mid-postback from whatever caused the link
    field to come back empty), so re-home via check_and_login_mhs first
    rather than trying to click around the stuck page.
    """
    hf_id = client["Human Friendly ID"]
    logger.warning(
        f"Link field was empty, checking Pending Invitations for {hf_id} "
        f"({description}/{rater_type})"
    )
    try:
        check_and_login_mhs(driver, services)
        click_element(
            driver,
            By.XPATH,
            "//span[contains(normalize-space(text()), 'Pending Invitations')]",
        )
        wait_for_page_load(driver)
        row = find_element(
            driver,
            By.XPATH,
            "//tr[@role='row']"
            f"[td[contains(normalize-space(text()), '{hf_id}')]]"
            f"[td[normalize-space(text())='{description}']]"
            f"[td[normalize-space(text())='{rater_type}']]",
            timeout=10,
        )
        link_cell = row.find_element(
            By.XPATH, ".//td[starts-with(normalize-space(text()), 'http')]"
        )
        link = link_cell.text.strip()
        if not link:
            return None
        logger.success(f"Found link {link} in Pending Invitations for {hf_id}")
        return link
    except (NoSuchElementException, TimeoutException):
        logger.error(f"Could not find a pending invitation for {hf_id} either")
        return None


def delete_client_from_mhs(
    driver: WebDriver, services: Services, client: pd.Series
) -> bool:
    """Search for a client, confirm with a human that it's the right one,
    then delete it from MHS.

    Returns:
        bool: True if the client was deleted, False if a human skipped it
        (e.g. it turned out to be the wrong client).
    """
    check_and_login_mhs(driver, services)
    hf_id = client["Human Friendly ID"]

    logger.debug("Navigating to My Clients")
    click_element(
        driver,
        By.XPATH,
        "//span[contains(normalize-space(text()), 'My Clients')]",
    )
    wait_for_page_load(driver)

    logger.debug("Searching for client")
    search_box = find_element(
        driver, By.ID, "searchBox", condition=ec.element_to_be_clickable
    )
    search_box.click()
    search_box.clear()
    search_box.send_keys(hf_id)
    sleep(1.5)

    logger.debug("Opening client")
    click_element(
        driver,
        By.XPATH,
        f"//td[@role='gridcell'][normalize-space(text())='{hf_id}']",
    )
    wait_for_page_load(driver)

    expected_name = f"{client['TA First Name']} {client['TA Last Name']}"
    heading_name = find_element(driver, By.ID, "titleHeading").text.strip()
    if heading_name != expected_name:
        logger.error(
            f"Opened client page shows '{heading_name}', expected "
            f"'{expected_name}' for {hf_id}. Skipping deletion."
        )
        return False

    confirmation = input(
        f"MHS: about to delete client {hf_id} ({heading_name}). "
        "Is this the correct client? [y/N] "
    )
    if confirmation.strip().lower() != "y":
        logger.warning(f"Skipping deletion of {hf_id}, not confirmed by user")
        return False

    logger.debug("Clicking Delete")
    click_element(driver, By.ID, "btnDelete")

    logger.debug("Confirming deletion")
    click_element(
        driver,
        By.ID,
        "ctrl__Controls_Common_ClientManagement_AddEditClient_ascx_btnDeleteClient",
    )

    logger.success(f"Deleted client {hf_id} from MHS")
    return True


def empty_mhs_deleted_items(driver: WebDriver, services: Services) -> None:
    """Navigate to Deleted Items and wait for a human to permanently trash
    everything there.

    MHS moves deleted clients to a "Deleted Items" holding area rather than
    removing them right away, so call this once after all pending clients
    for a run have been deleted via delete_client_from_mhs.
    """
    check_and_login_mhs(driver, services)
    logger.debug("Navigating to Deleted Items")
    click_element(
        driver,
        By.XPATH,
        "//span[contains(normalize-space(text()), 'Deleted Items')]",
    )
    input("MHS: please trash all deleted items and press enter...")


def add_client_to_mhs(
    driver: WebDriver,
    client: pd.Series,
    questionnaire: str,
    accounts_created: dict[str, bool],
) -> bool:
    """Add a client to MHS, or goes to the existing client.

    Returns:
        bool: True if successful, False otherwise.
    """

    def _add_to_existing(
        driver: WebDriver, client: pd.Series, questionnaire: str
    ) -> bool:
        actions = ActionChains(driver)
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
        return _add_to_existing(driver, client, questionnaire)

    logger.info(
        f"Attempting to add {client['TA First Name']} {client['TA Last Name']} to MHS"
    )
    firstname = client["TA First Name"]
    lastname = client["TA Last Name"]
    hf_id = client["Human Friendly ID"]
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
    id_field.send_keys(hf_id)

    logger.debug("Entering birthdate")
    date_of_birth_field = find_element(
        driver, By.CSS_SELECTOR, "input[placeholder='YYYY/Mmm/DD']"
    )
    date_of_birth_field.send_keys(dob)

    if questionnaire in {"Conners EC", "ASRS"}:
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
        logger.debug("Checking for duplicate name confirmation")
        confirm_id = (
            "ctrl__Controls_Product_Custom_ASRS_Wizard_InviteWizardContainer_ascx_ClientProfile_confirm"
            if questionnaire == "ASRS"
            else "ctrl__Controls_Product_Wizard_InviteWizardContainer_ascx_ClientProfile_confirm"
        )
        click_element(driver, By.ID, confirm_id, timeout=2)
        logger.debug("Confirmed same-name client is not a duplicate")
    except (NoSuchElementException, TimeoutException):
        pass
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
    return _add_to_existing(driver, client, questionnaire)


# Maps our internal questionnaire type names to their MHS Completed Assessments display
# prefix. The prefix is used with XPath contains() so it only needs to be specific enough
# to avoid matching a different rater type for the same assessment family.
_MHS_DISPLAY_PREFIX: dict[str, str] = {
    "Conners EC": "Conners EC",
    "Conners 4": "Conners 4 Parent",
    "Conners 4 Self": "Conners 4 Self",
    "ASRS (2-5 Years)": "ASRS (2-5 Years) Parent",
    "ASRS (6-18 Years)": "ASRS (6-18 Years) Parent",
    "CAARS 2": "CAARS 2 Self",
}


def check_mhs_completed(
    driver: WebDriver,
    services: Services,
    client_id: int,
    q_type: str,
) -> bool:
    """Fallback check: searches MHS Completed Assessments page for a client's finished questionnaire.

    Returns True if a matching completed assessment row is found.
    """
    mhs_hf_id = f"C{str(client_id).zfill(9)}"
    mhs_type_prefix = _MHS_DISPLAY_PREFIX.get(q_type, q_type)
    logger.info(
        f"MHS fallback check: client {mhs_hf_id}, type '{q_type}' (searching '{mhs_type_prefix}')"
    )

    try:
        check_and_login_mhs(driver, services)

        click_element(
            driver,
            By.XPATH,
            "//span[contains(normalize-space(text()), 'Completed Assessments')]",
        )
        wait_for_page_load(driver)

        search_box = find_element(
            driver, By.ID, "searchBox", condition=ec.element_to_be_clickable
        )
        search_box.click()
        search_box.clear()
        search_box.send_keys(mhs_hf_id)
        sleep(1.5)

        find_element(
            driver,
            By.XPATH,
            f"//tr[@data-uid and @role='row']"
            f"[td[@role='gridcell'][contains(normalize-space(text()), '{mhs_hf_id}')]]"
            f"[td[@role='gridcell'][contains(normalize-space(text()), '{mhs_type_prefix}')]]",
            timeout=5,
        )
        logger.info(f"Found '{q_type}' in completed assessments on MHS for {mhs_hf_id}")
        return True

    except (NoSuchElementException, TimeoutException):
        logger.info(
            f"Did not find '{q_type}' in completed assessments on MHS for {mhs_hf_id}"
        )
        return False
    except Exception:
        logger.exception(
            f"Error during MHS Completed Assessments fallback for {mhs_hf_id}"
        )
        return False


def gen_conners_ec(
    driver: WebDriver,
    services: Services,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates a Conners EC assessment for the given client and returns the link."""
    check_and_login_mhs(driver, services)
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
        driver, client, "Conners EC", accounts_created
    )

    logger.debug("Selecting assessment description")
    _select_description(driver, "Conners EC")

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")

    logger.debug("Selecting Parent")
    _select_and_wait_for_postback(driver, rater_type_element, "Parent")

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
    if not link:
        link = _find_link_in_pending_invitations(
            driver, services, client, "Conners EC", "Parent"
        )
    if not link:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_conners_4(
    driver: WebDriver,
    services: Services,
    client: pd.Series,
    accounts_created: dict,
    self_report: bool = False,
) -> tuple[str, dict[str, bool]]:
    """Generates a Conners 4 (or Conners 4 Self-Report) assessment for the given client and returns the link."""
    check_and_login_mhs(driver, services)
    logger.info(
        f"Generating Conners 4{'Self' if self_report else ''} for {client['TA First Name']} {client['TA Last Name']}"
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
        driver, client, "Conners 4", accounts_created
    )

    logger.debug("Selecting assessment description")
    _select_description(driver, "Conners 4")

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")
    rater_type_text = "Self-Report" if self_report else "Parent"
    _select_and_wait_for_postback(driver, rater_type_element, rater_type_text)

    language_element = find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
    if client["Language"] != "Spanish":
        language_select.select_by_visible_text("English")
    else:
        language_select.select_by_visible_text("Spanish")

    if not self_report:
        logger.debug("Entering rater name")
        if client["Language"] != "Spanish":
            find_element(driver, By.ID, "txtRaterName").send_keys("Parent/Caregiver")
        else:
            find_element(driver, By.ID, "txtRaterName").send_keys(
                "Madre/Padre/Cuidador"
            )

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
    if not link:
        link = _find_link_in_pending_invitations(
            driver, services, client, "Conners 4", rater_type_text
        )
    if not link:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_asrs_2_5(
    driver: WebDriver,
    services: Services,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates an ASRS 2-5 assessment for the given client and returns the link."""
    check_and_login_mhs(driver, services)
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
        driver, client, "ASRS", accounts_created
    )

    logger.debug("Selecting assessment description")
    _select_description(driver, "ASRS (2-5 Years)")

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")
    _select_and_wait_for_postback(driver, rater_type_element, "Parent")

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
    if not link:
        link = _find_link_in_pending_invitations(
            driver, services, client, "ASRS (2-5 Years)", "Parent"
        )
    if not link:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_asrs_6_18(
    driver: WebDriver,
    services: Services,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates an ASRS 6-18 assessment for the given client and returns the link."""
    check_and_login_mhs(driver, services)
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
        driver, client, "ASRS", accounts_created
    )
    sleep(1)

    logger.debug("Selecting assessment description")
    _select_description(driver, "ASRS (6-18 Years)")

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")
    _select_and_wait_for_postback(driver, rater_type_element, "Parent")

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
    if not link:
        link = _find_link_in_pending_invitations(
            driver, services, client, "ASRS (6-18 Years)", "Parent"
        )
    if not link:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created


def gen_caars_2(
    driver: WebDriver,
    services: Services,
    client: pd.Series,
    accounts_created: dict[str, bool],
) -> tuple[str, dict[str, bool]]:
    """Generates a CAARS 2 assessment for the given client and returns the link."""
    check_and_login_mhs(driver, services)
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
        driver, client, "CAARS 2", accounts_created
    )

    logger.debug("Selecting assessment description")
    _select_description(driver, "CAARS 2")

    logger.debug("Selecting rater type")
    rater_type_element = find_element(driver, By.ID, "ddl_RaterType")
    _select_and_wait_for_postback(driver, rater_type_element, "Self-Report")

    logger.debug("Selecting language")
    language_element = find_element(driver, By.ID, "ddl_Language")
    language_select = Select(language_element)
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
    if not link:
        link = _find_link_in_pending_invitations(
            driver, services, client, "CAARS 2", "Self-Report"
        )
    if not link:
        raise ValueError("Link is None")

    logger.success(f"Returning link {link} and accounts_created {accounts_created}")
    return link, accounts_created
