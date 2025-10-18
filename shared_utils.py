import base64
import hashlib
import io
import os
import re
import time
from datetime import date
from email.message import EmailMessage
from time import sleep
from typing import Annotated, Literal, Optional, TypedDict
from urllib.parse import urlparse

import pandas as pd
import pymysql.cursors
import yaml
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger
from pydantic import (
    BaseModel,
    EmailStr,
    StringConstraints,
    field_validator,
)
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


### TYPES ###
class Service(TypedDict):
    """A TypedDict containing service credentials."""

    username: str
    password: str


class ServiceWithAdmin(Service):
    """A TypedDict containing service credentials and an admin user."""

    admin_username: str
    admin_password: str


class OpenPhoneUser(TypedDict):
    """A TypedDict containing OpenPhone user information."""

    id: str
    phone: str


class OpenPhoneService(TypedDict):
    """A TypedDict containing OpenPhone API credentials and settings."""

    key: str
    main_number: str
    users: dict[str, OpenPhoneUser]


class Services(TypedDict):
    """A TypedDict containing all the service configurations and credentials."""

    mhs: Service
    openphone: OpenPhoneService
    qglobal: Service
    therapyappointment: ServiceWithAdmin
    wps: Service


class Config(BaseModel):
    """A Pydantic model representing the configuration of the application."""

    initials: Annotated[
        str,
        StringConstraints(strip_whitespace=True, to_upper=True, max_length=4),
    ]
    name: str
    email: EmailStr
    automated_email: EmailStr
    qreceive_emails: list[EmailStr]
    cc_emails: list[EmailStr]
    excluded_calendars: list[EmailStr]
    punch_list_id: str
    punch_list_range: Annotated[
        str,
        StringConstraints(pattern=r"^.+![A-z]+\d*(:[A-z]+\d*)?$"),
    ]
    failed_sheet_id: str
    database_url: str
    excluded_ta: list[str]
    records_folder_id: str
    sent_records_folder_id: str
    records_emails: dict[str, str]


class Questionnaire(TypedDict):
    """A TypedDict containing information about a questionnaire."""

    questionnaireType: str
    link: str
    sent: date
    status: Literal["COMPLETED", "PENDING", "RESCHEDULED"]
    reminded: int
    lastReminded: Optional[date]


class FailedClient(TypedDict):
    """A TypedDict containing information about a failed client."""

    firstName: str
    lastName: str
    fullName: str
    asdAdhd: str
    daEval: str
    failedDate: str
    error: str
    questionnaires_needed: Optional[list[str] | str]
    questionnaire_links_generated: Optional[list[dict[str, bool | str]]]


class _ClientBase(BaseModel):
    id: int
    dob: Optional[date] = None
    firstName: str
    lastName: str
    preferredName: Optional[str] = None
    fullName: str
    phoneNumber: Optional[str] = None
    gender: Optional[str] = None
    asdAdhd: Optional[str] = None


class ClientFromDB(_ClientBase):
    """A Pydantic model representing a client from the database."""

    questionnaires: Optional[list[Questionnaire]]


class ClientWithQuestionnaires(_ClientBase):
    """A Pydantic model representing a client with questionnaires."""

    questionnaires: list[Questionnaire]

    @field_validator("questionnaires")
    def validate_questionnaires(cls, v: list[Questionnaire]) -> list[Questionnaire]:
        """Validate that the client has questionnaires."""
        if not v:
            raise ValueError("Client has no questionnaires")
        return v


class AdminEmailInfo(TypedDict):
    """A TypedDict containing lists of clients grouped by status, for emailing."""

    reschedule: list[ClientWithQuestionnaires]
    failed: list[ClientWithQuestionnaires]
    call: list[ClientWithQuestionnaires]
    completed: list[ClientWithQuestionnaires]
    api_failure: Optional[str]


def load_config() -> tuple[Services, Config]:
    """Load and parse the configuration from the 'info.yml' file.

    Returns:
        tuple[Services, Config]: A tuple containing the initialized `Services`
        and `Config` instances.
    """
    with open("./config/info.yml", "r") as file:
        logger.debug("Loading config info file")
        info = yaml.safe_load(file)
        services = info["services"]
        config = info["config"]
        # Validate as Services and Config types
        try:
            services = Services(**services)
            config = Config(**config)
        except Exception as e:
            logger.exception(e)
            exit(1)
        return services, config


### SELENIUM ###
def initialize_selenium(save_profile: bool = False) -> tuple[WebDriver, ActionChains]:
    """Initialize a Selenium WebDriver with the given options.

    Args:
        save_profile (bool, optional): If true, save the browser profile to the
            `./config/chrome_profile` directory. Defaults to False.

    Returns:
        tuple[WebDriver, ActionChains]: A tuple containing the initialized WebDriver
        and ActionChains instances.
    """
    logger.info("Initializing Selenium")
    chrome_options: Options = Options()
    chrome_options.add_argument("--no-sandbox")
    if os.getenv("HEADLESS") == "true":
        chrome_options.add_argument("--headless")
    # /dev/shm partition can be too small in VMs, causing Chrome to crash, make a temp dir instead
    chrome_options.add_argument("--disable-dev-shm-usage")
    if save_profile:
        chrome_options.add_argument("--user-data-dir=./config/chrome_profile")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": f"{os.getcwd()}/put/downloads",
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    driver = webdriver.Chrome(options=chrome_options)
    actions = ActionChains(driver)
    driver.implicitly_wait(5)
    driver.set_window_size(1920, 1080)
    return driver, actions


def click_element(
    driver: WebDriver,
    by: str,
    locator: str,
    max_attempts: int = 3,
    delay: int = 1,
    refresh: bool = False,
) -> None:
    """Click on a web element located by the specified method within the given attempts.

    Raises:
        NoSuchElementException: If the element is not found after the specified
            number of attempts.
    """
    for attempt in range(max_attempts):
        try:
            element = driver.find_element(by, locator)
            element.click()
            return
        except (StaleElementReferenceException, NoSuchElementException) as e:
            f"Attempt {attempt + 1}/{max_attempts} failed: {type(e).__name__}. Retrying in {delay} seconds..."
            sleep(delay)
            if refresh:
                logger.info("Refreshing page")
                driver.refresh()
                sleep(delay)
    raise NoSuchElementException(f"Element not found after {max_attempts} attempts")


def find_element(
    driver: WebDriver,
    by: str,
    locator: str,
    max_attempts: int = 3,
    delay: int = 1,
) -> WebElement:
    """Find a web element with retries.

    Raises:
        NoSuchElementException: If the element is not found.
    """
    for attempt in range(max_attempts):
        try:
            element = driver.find_element(by, locator)
            return element
        except (StaleElementReferenceException, NoSuchElementException) as e:
            logger.warning(
                f"Attempt {attempt + 1}/{max_attempts} failed: {type(e).__name__}. Retrying in {delay} seconds..."
            )
            sleep(delay)

    raise NoSuchElementException(f"Element not found after {max_attempts} attempts")


def find_element_exists(
    driver: WebDriver,
    by: str,
    locator: str,
    max_attempts: int = 3,
    delay: int = 1,
) -> bool:
    """Check if a web element exists with retries.

    Returns:
        bool: True if the element is found, False otherwise.
    """
    try:
        find_element(driver, by, locator, max_attempts, delay)
        return True
    except NoSuchElementException:
        return False


### DATABASE ###
def get_db(config: Config):
    """Connect to the database and return a connection object.

    Returns:
        A pymysql connection object.
    """
    # TODO: Send email if unable to connect
    db_url = urlparse(config.database_url)
    connection = pymysql.connect(
        host=db_url.hostname,
        port=db_url.port or 3306,
        user=db_url.username,
        password=db_url.password or "",
        database=db_url.path[1:],
        cursorclass=pymysql.cursors.DictCursor,
    )
    return connection


def get_previous_clients(
    config: Config, failed: bool = False
) -> tuple[dict[int | str, ClientFromDB], dict[int | str, ClientFromDB]]:
    """Load previous clients from the database and a YAML file.

    Args:
        config (Config): The configuration object.
        failed (bool, optional): Whether to load failed clients from the YAML file. Defaults to False.

    Returns:
        tuple[dict[int | str, ClientFromDB], dict[int | str, ClientFromDB]]: A tuple containing two dictionaries.
            The first dictionary contains clients loaded from the database.
            The second dictionary contains failed clients loaded from the YAML file.
    """
    logger.info(
        f"Loading previous clients from DB{' and failed clients' if failed else ''}"
    )
    qfailure_filepath = "./put/qfailure.yml"
    failed_prev_clients = {}
    if failed:
        # Load failed clients from the YAML file
        try:
            with open(qfailure_filepath, "r") as file:
                failed_prev_clients = yaml.safe_load(file) or {}
        except FileNotFoundError:
            logger.info(f"{qfailure_filepath} does not exist.")

    # Load clients from the database
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = "SELECT * FROM emr_client"
            cursor.execute(sql)
            clients = cursor.fetchall()

            sql = "SELECT * FROM emr_questionnaire"
            cursor.execute(sql)
            questionnaires = cursor.fetchall()
            for client in clients:
                # Add the questionnaires to each client
                client["questionnaires"] = [
                    questionnaire
                    for questionnaire in questionnaires
                    if questionnaire["clientId"] == client["id"]
                ]

    # Create a dictionary of clients with their IDs as keys
    prev_clients = {}
    if clients:
        for client in clients:
            prev_clients[client["id"]] = {key: value for key, value in client.items()}

    return prev_clients, failed_prev_clients


def validate_questionnaires(
    clients: dict[int | str, ClientFromDB],
) -> dict[int | str, ClientWithQuestionnaires]:
    """Validate clients from the database and convert them to ClientWithQuestionnaires.

    Returns:
        A dictionary of validated clients, where the keys are the client IDs and the values
        are ClientWithQuestionnaires objects.
    """
    validated = {}
    for client_id, client in clients.items():
        try:
            validated[client_id] = ClientWithQuestionnaires.model_validate(client)
        except ValueError:
            continue  # Skip invalid clients
    return validated


def get_evaluator_npi(config: Config, evaluator_email) -> Optional[str]:
    """Get the NPI of an evaluator from the database.

    Args:
        config: The configuration.
        evaluator_email: The email address of the evaluator.

    Returns:
        The NPI of the evaluator, or None if not found.
    """
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = "SELECT npi FROM emr_evaluator WHERE email = %s"
            cursor.execute(sql, (evaluator_email))
            npi = cursor.fetchone()
            return npi["npi"] if npi else None


def insert_basic_client(
    config: Config,
    client_id: str,
    dob,
    first_name: str,
    last_name: str,
    asd_adhd: str,
    gender: str,
    phone_number,
):
    """Insert a client into the database, using only the data from sending a questionnaire.

    Args:
        config (Config): The configuration object.
        client_id (str): The client ID.
        dob: The date of birth of the client.
        first_name (str): The first name of the client.
        last_name (str): The last name of the client.
        asd_adhd (str): The type of condition the client has (ASD, ADHD, or ASD+ADHD).
        gender (str): The gender of the client.
        phone_number: The phone number of the client.

    Returns:
        None
    """
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = """
                INSERT INTO `emr_client` (id, hash, dob, firstName, lastName, fullName, asdAdhd, gender, phoneNumber)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE id=id, asdAdhd=VALUES(asdAdhd)
            """

            values = (
                int(client_id),
                hashlib.sha256(str(client_id).encode("utf-8")).hexdigest(),
                dob,
                first_name,
                last_name,
                f"{first_name} {last_name}",
                "Both" if asd_adhd == "ASD+ADHD" else asd_adhd,
                gender,
                phone_number,
            )

            cursor.execute(sql, values)

        db_connection.commit()


def put_questionnaire_in_db(
    config: Config,
    client_id: str,
    link: str,
    type: str,
    sent_date,
    status: Literal["COMPLETED", "PENDING", "RESCHEDULED"],
):
    """Insert a questionnaire into the database.

    Args:
        config (Config): The configuration object.
        client_id (str): The client ID.
        link (str): The link of the questionnaire.
        type (str): The type of the questionnaire.
        sent_date: The date the questionnaire was sent.
        status (Literal["COMPLETED", "PENDING", "RESCHEDULED"]): The status of the questionnaire.

    Returns:
        None
    """
    db_connection = get_db(config)

    with db_connection:
        with db_connection.cursor() as cursor:
            sql = """
                INSERT INTO emr_questionnaire (
                    clientId, link, questionnaireType, sent, status
                ) VALUES (%s, %s, %s, %s, %s)
            """

            values = (int(client_id), link, type, sent_date, status)

            cursor.execute(sql, values)
        db_connection.commit()


def update_questionnaires_in_db(
    config: Config, clients: list[ClientWithQuestionnaires]
):
    """Update questionnaires in the database, setting status, reminded count, and last reminded date.

    Args:
        config (Config): The configuration object.
        clients (list[ClientWithQuestionnaires]): A list of ClientWithQuestionnaires objects.
    """
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            for client in clients:
                for questionnaire in client.questionnaires:
                    sql = """
                        UPDATE `emr_questionnaire`
                        SET status=%s, reminded=%s, lastReminded=%s
                        WHERE clientId=%s AND sent=%s AND questionnaireType=%s
                    """

                    values = (
                        questionnaire["status"],
                        questionnaire["reminded"],
                        questionnaire["lastReminded"],
                        client.id,
                        questionnaire["sent"],
                        questionnaire["questionnaireType"],
                    )

                    cursor.execute(sql, values)
                    db_connection.commit()


def update_yaml(clients: dict, filepath: str) -> None:
    """Update a YAML file with a given dictionary.

    If the file does not exist, it will be created. If it does exist, the dictionary
    will be merged into the existing YAML.

    Args:
        clients (dict): The dictionary to update the YAML with.
        filepath (str): The path to the YAML file.

    Returns:
        None
    """
    try:
        with open(filepath, "r") as file:
            current_yaml = yaml.safe_load(file)
    except FileNotFoundError:
        logger.info(f"{filepath} does not exist, creating new file")
        current_yaml = None

    if current_yaml is None:
        logger.info(f"Dumping to {filepath}")
        with open(filepath, "w") as file:
            yaml.dump(clients, file, default_flow_style=False)
    else:
        current_yaml.update(clients)
        with open(filepath, "w") as file:
            logger.info(f"Dumping to {filepath}")
            yaml.dump(current_yaml, file, default_flow_style=False)


def add_failure(config: Config, client: dict[str, FailedClient]) -> None:
    """Add a client to the failure YAML files and the failure sheet.

    Args:
        config (Config): The configuration object.
        client (dict[str, FailedClient]): The client to add to the failure files and sheet.

    Returns:
        None
    """
    qfailure_filepath = "./put/qfailure.yml"
    qfailsend_filepath = "./put/qfailsend.yml"

    update_yaml(client, qfailure_filepath)
    update_yaml(client, qfailsend_filepath)

    add_qfailed_to_failure_sheet(config, client)


### QUESTIONNAIRES ###
def all_questionnaires_done(client: ClientWithQuestionnaires) -> bool:
    """Check if all questionnaires for the given client are completed.

    Args:
        client (ClientWithQuestionnaires): The client to check.

    Returns:
        bool: True if all questionnaires are completed, False otherwise.
    """
    return all(
        q["status"] == "COMPLETED" for q in client.questionnaires if isinstance(q, dict)
    )


def check_if_rescheduled(client: ClientWithQuestionnaires) -> bool:
    """Check if any questionnaire for the given client has been rescheduled.

    Args:
        client (ClientWithQuestionnaires): The client to check.

    Returns:
        bool: True if any questionnaire has been rescheduled, False otherwise.
    """
    return any(
        q["status"] == "RESCHEDULED"
        for q in client.questionnaires
        if isinstance(q, dict)
    )


def check_q_done(driver: WebDriver, q_link: str, q_type: str) -> bool:
    """Check if a questionnaire linked by `q_link` is completed.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
        q_link (str): The URL of the questionnaire.
        q_type (str): The type of the questionnaire.

    Returns:
        bool: True if the questionnaire is completed, False otherwise.

    Raises:
        Exception: If the questionnaire type does not match the URL.
    """
    driver.get(q_link)
    wait = WebDriverWait(driver, 15)

    url_patterns = {
        "ASRS (2-5 Years)": "/asrs_web/",
        "ASRS (6-18 Years)": "/asrs_web/",
        "Conners EC": "/CEC/",
        "Conners 4": "/conners4/",
        "DP-4": "respondent.wpspublish.com",
    }

    try:
        time.sleep(2)

        current_url = driver.current_url
        # logger.debug(f"Current URL: {current_url}")

        if q_type in url_patterns:
            expected_pattern = url_patterns[q_type]
            if expected_pattern not in current_url:
                error_msg = f"URL mismatch: Expected '{expected_pattern}' in URL for type '{q_type}', but got '{current_url}'"
                logger.error(error_msg)
                raise Exception(error_msg)
            # logger.debug(f"URL validation passed for type '{q_type}'")

        if "mhs.com" in q_link:
            logger.info(f"Checking MHS completion for {q_link}")
            wait.until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//*[contains(text(), 'Thank you for completing')] | //*[contains(text(), 'This link has already been used')] | //*[contains(text(), 'We have received your answers')]",
                    )
                )
            )
            return True

        elif "pearsonassessments.com" in q_link:
            logger.info(f"Checking Pearson completion for {q_link}")
            wait.until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//*[contains(text(), 'Test Completed!')]",
                    )
                )
            )
            return True

        elif "wpspublish" in q_link:
            logger.info(f"Checking WPS completion for {q_link}")
            wait.until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//*[contains(text(), 'This assessment is not available at this time')]",
                    )
                )
            )
            return True

        else:
            logger.warning(f"Unknown questionnaire link: {q_link}")
            return False

    except (TimeoutException, NoSuchElementException):
        logger.info(f"Questionnaire at {q_link} is likely not completed")
        return False

    except Exception as e:
        logger.error(f"Error checking questionnaire at {q_link}: {e}")
        return False


def check_questionnaires(
    driver: WebDriver,
    config: Config,
    clients: dict[int | str, ClientWithQuestionnaires],
) -> list[ClientWithQuestionnaires]:
    """Check if all questionnaires for the given clients are completed. This function will navigate to each questionnaire link and look for specific text on the page based on the URL.

    Args:
        driver (WebDriver): The Selenium WebDriver instance used for browser automation.
        config (Config): The configuration object.
        clients (dict[int | str, ClientWithQuestionnaires]): A dictionary of clients with their IDs as keys and ClientWithQuestionnaires objects as values.

    Returns:
        list[ClientWithQuestionnaires]: A list of clients whose questionnaires are all completed.
    """
    if not clients:
        return []
    completed_clients = []
    updated_clients = []
    for id in clients:
        client = clients[id]
        if all_questionnaires_done(client):
            logger.info(f"{client.fullName} has already completed their questionnaires")
            continue
        client_updated = False
        for questionnaire in client.questionnaires:
            if questionnaire["status"] == "COMPLETED":
                logger.info(
                    f"{client.fullName}'s {questionnaire['questionnaireType']} is already done"
                )
                continue
            logger.info(
                f"Checking {client.fullName}'s {questionnaire['questionnaireType']}"
            )
            if check_q_done(
                driver, questionnaire["link"], questionnaire["questionnaireType"]
            ):
                questionnaire["status"] = "COMPLETED"
                logger.info(
                    f"{client.fullName}'s {questionnaire['questionnaireType']} is {questionnaire['status']}"
                )
                client_updated = True
            else:
                questionnaire["status"] = "PENDING"
                logger.warning(
                    f"{client.fullName}'s {questionnaire['questionnaireType']} is {questionnaire['status']}"
                )
                logger.warning(
                    f"At least one questionnaire is not done for {client.fullName}"
                )
                break

        if client_updated:
            updated_clients.append(client)

        if all_questionnaires_done(client):
            completed_clients.append(client)

    if updated_clients:
        update_questionnaires_in_db(config, updated_clients)
    return completed_clients


### FORMATTING ###
def format_phone_number(phone_number: str) -> str:
    """Format a phone number string into (XXX) XXX-XXXX format.

    Args:
        phone_number (str): The phone number string to format.

    Returns:
        str: The formatted phone number string.
    """
    phone_number = re.sub(r"\D", "", phone_number)
    return f"({phone_number[:3]}) {phone_number[3:6]}-{phone_number[6:]}"


def check_distance(x: date) -> int:
    """Calculate the number of days between the given date and today.

    Args:
        x (date): The date to calculate the distance from.

    Returns:
        int: The number of days between x and today.
    """
    today = date.today()
    delta = today - x
    return delta.days


def get_most_recent_not_done(client: ClientWithQuestionnaires) -> Questionnaire:
    """Get the most recent questionnaire that is still PENDING from the given client by taking max of q["sent"].

    Args:
        client (ClientWithQuestionnaires): The client with questionnaires to check.

    Returns:
        Questionnaire: The most recent questionnaire that is still PENDING.
    """
    return max(
        (q for q in client.questionnaires if q["status"] == "PENDING"),
        key=lambda q: q["sent"],
    )


def get_reminded_ever(client: ClientWithQuestionnaires) -> bool:
    """Check if the client has ever been reminded of a questionnaire.

    Args:
        client (ClientWithQuestionnaires): The client with questionnaires to check.

    Returns:
        bool: True if the client has ever been reminded of a questionnaire, False otherwise.
    """
    return any(
        q["reminded"] != 0 and q["status"] == "PENDING"
        for q in client.questionnaires
        if isinstance(q, dict)
    )


### GOOGLE ###

# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


def google_authenticate():
    """Authenticate with Google using the credentials in ./config/credentials.json (obtained from Google Cloud Console) and ./config/token.json (user-specific).

    If the credentials are not valid, the user is prompted to log in.
    The credentials are then saved to ./config/token.json for the next run.
    Returns the authenticated credentials.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("./config/token.json"):
        creds = Credentials.from_authorized_user_file("./config/token.json", SCOPES)
    # If there are no valid credentials, start the authorization flow
    else:
        creds = None

    # If the credentials are invalid or have expired, refresh the credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        # If there are no credentials, start the manual login
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "./config/credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

    # Save the credentials for the next run
    with open("./config/token.json", "w") as token:
        token.write(creds.to_json())

    return creds


def send_gmail(
    message_text: str,
    subject: str,
    to_addr: str,
    from_addr: str,
    cc_addr: Optional[str] = None,
    html: Optional[str] = None,
    pdf_stream0: Optional[io.BytesIO] = None,
    filename0: Optional[str] = None,
    pdf_stream1: Optional[io.BytesIO] = None,
    filename1: Optional[str] = None,
):
    """Send an email using the Gmail API.

    Parameters:
        message_text (str): The text of the message
        subject (str): The subject of the message
        to_addr (str): The recipient's email address, can be a comma-separated list
        from_addr (str): The sender's email address
        cc_addr (Optional[str]): The CC recipient's email address, can be a comma-separated list (optional)
        html (Optional[str]): The HTML version of the message (optional)
        pdf_stream0 (Optional[io.BytesIO]): Possible pdf attachment taken from memory (optional)
        filename0 (Optional[str]): Name of pdf0 taken from memory (optional)
        pdf_stream1 (Optional[io.BytesIO]): Possible pdf attachment taken from memory (optional)
        filename1 (Optional[str]): Name of pdf1 taken from memory (optional)
    """
    creds = google_authenticate()

    try:
        service = build("gmail", "v1", credentials=creds)

        message = EmailMessage()
        message.set_content(message_text)
        message["Subject"] = subject
        message["To"] = to_addr
        message["From"] = from_addr
        if cc_addr:
            message["Cc"] = cc_addr

        if html:
            message.add_alternative(html, subtype="html")
        if pdf_stream0 and pdf_stream1 and filename0 and filename1:
            pdf_bytes0 = pdf_stream0.getvalue()
            pdf_bytes1 = pdf_stream1.getvalue()
            message.add_attachment(
                pdf_bytes0, maintype="application", subtype="pdf", filename=filename0
            )
            message.add_attachment(
                pdf_bytes1, maintype="application", subtype="pdf", filename=filename1
            )

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {"raw": encoded_message}

        send_message = (
            service.users().messages().send(userId="me", body=create_message).execute()
        )

        logger.info(f"Sent email to {to_addr}: {subject}")

    except HttpError as error:
        logger.exception(error)
        send_message = None
    return send_message


def build_admin_email(email_info: AdminEmailInfo) -> tuple[str, str]:
    """Builds an email to admin based on the grouped clients.

    Parameters:
        email_info (AdminEmailInfo): The grouped clients

    Returns:
        tuple[str, str]: A tuple of the text and HTML versions of the email message
    """
    email_text = ""
    email_html = ""

    if email_info["api_failure"]:
        email_text += f"OpenPhone API Failure:\n{email_info['api_failure']}\n"
        email_html += (
            f"<b>OpenPhone API Failure:</b><br>{email_info['api_failure']}<br>"
        )

    if email_info["completed"]:
        email_text += (
            "Download:\n"
            + "\n".join([f"- {client.fullName}" for client in email_info["completed"]])
            + "\n"
        )
        email_html += (
            "<h2>Download</h2><ul><li>"
            + "</li><li>".join(client.fullName for client in email_info["completed"])
            + "</li></ul>"
        )
    if email_info["reschedule"]:
        email_text += (
            "Check on rescheduled:\n"
            + "\n".join([f"- {client.fullName}" for client in email_info["reschedule"]])
            + "\n"
        )
        email_html += (
            "<h2>Check on rescheduled</h2><ul><li>"
            + "</li><li>".join(client.fullName for client in email_info["reschedule"])
            + "</li></ul>"
        )
    if email_info["failed"]:
        email_text += (
            "Failed to message:\n"
            + "\n".join([f"- {client.fullName}" for client in email_info["failed"]])
            + "\n"
        )
        email_html += (
            "<h2>Failed to message</h2><ul><li>"
            + "</li><li>".join(client.fullName for client in email_info["failed"])
            + "</li></ul>"
        )
    if email_info["call"]:
        email_text += (
            "Call:\n"
            + "\n".join(
                [
                    f"- {client.fullName} (sent on {most_recent_q['sent'].strftime('%m/%d') if most_recent_q else 'unknown date'}, reminded {str(most_recent_q['reminded']) + ' times' if most_recent_q else 'unknown number of times'})"
                    for client in email_info["call"]
                    if (most_recent_q := get_most_recent_not_done(client))
                ]
            )
            + "\n"
        )
        email_html += (
            "<h2>Call</h2><ul><li>"
            + "</li><li>".join(
                f"{client.fullName} (sent on {most_recent_q['sent'].strftime('%m/%d') if most_recent_q else 'unknown date'}, reminded {str(most_recent_q['reminded']) + ' times' if most_recent_q else 'unknown number of times'})"
                for client in email_info["call"]
                if (most_recent_q := get_most_recent_not_done(client))
            )
            + "</li></ul>"
        )
    return email_text, email_html


def get_punch_list(config: Config):
    """Downloads the punch list and returns it as a pandas DataFrame.

    Returns:
        pandas.DataFrame: A DataFrame containing the punch list data.
    """
    creds = google_authenticate()

    try:
        service = build("sheets", "v4", credentials=creds)

        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(
                spreadsheetId=config.punch_list_id,
                range=config.punch_list_range,
            )
            .execute()
        )
        values = result.get("values", [])

        if values:
            df = pd.DataFrame(values[1:], columns=values[0])

            # Rename the first column to "Client Name"
            df = df.rename(columns={df.columns[0]: "Client Name"})

            # Select only the columns we need
            df = df[
                [
                    "Client Name",
                    "Client ID",
                    "For",
                    "Language",
                    "Records Needed",
                    "Records Requested?",
                    "Records Reviewed?",
                    "DA Qs Needed",
                    "DA Qs Sent",
                    "EVAL Qs Needed",
                    "EVAL Qs Sent",
                ]
            ]

            # Drop any rows where the "Client ID" column is empty
            df = df[df["Client ID"].notna() & df["Client ID"].str.len().astype(bool)]

            # Convert "Human friendly" IDs to proper IDs
            df["Client ID"] = df["Client ID"].apply(
                lambda client_id: re.sub(r"^C?0*", "", client_id)
            )

            # Create a "Human Friendly ID" column
            df["Human Friendly ID"] = df["Client ID"].apply(
                lambda client_id: f"C{client_id.zfill(9)}"
            )

            return df
    except Exception as e:
        logger.exception(e)


def col_index_to_a1(col_index):
    """Converts a zero-based column index to A1 notation."""
    column_letter = ""
    while col_index >= 0:
        # Find the character for the current place
        remainder = col_index % 26
        column_letter = chr(ord("A") + remainder) + column_letter

        # Move to the next place (like carrying over in division)
        col_index = (col_index // 26) - 1

    return column_letter


def update_punch_list(
    config: Config, id_for_search: str, update_header: str, new_value: str
):
    """Updates the Punch List sheet with the given value.

    Args:
        config: The application configuration.
        id_for_search: The ID to search for in the Punch List.
        update_header: The header of the column to update.
        new_value: The new value to write to the cell.

    Raises:
        Exception: If anything goes wrong.
    """
    creds = google_authenticate()

    try:
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(
                spreadsheetId=config.punch_list_id,
                range=config.punch_list_range,
            )
            .execute()
        )
        values = result.get("values", [])

        # Find the row containing the client ID
        row_number = None
        for i, row in enumerate(values):
            if row and row[1] == id_for_search:
                row_number = i + 1  # Spreadsheets are 1-indexed
                break

        # Find the column containing the header
        update_column = None
        for i, header in enumerate(values[0]):
            if header == update_header:
                update_column = col_index_to_a1(i)
                break

        if row_number is not None and update_column is not None:
            sheet_name = config.punch_list_range.split("!")[0]
            update_range = f"{sheet_name}!{update_column}{row_number}"
            body = {"values": [[new_value]]}
            result = (
                sheet.values()
                .update(
                    spreadsheetId=config.punch_list_id,
                    range=update_range,
                    valueInputOption="USER_ENTERED",
                    body=body,
                )
                .execute()
            )
            logger.success(f"Updated {update_column} for {id_for_search} in Punch List")
        else:
            logger.error(f"Client {id_for_search} not found in Punch List")
    except Exception as e:
        logger.exception(e)


def update_punch_by_column(
    config: Config,
    client_id: str,
    daeval: Literal["DA", "EVAL", "DAEVAL"],
    sent_done: Literal["sent", "done"],
):
    """Updates the punch list for the given client ID.

    Args:
        config: The application configuration.
        client_id: The ID of the client to update.
        daeval: The type of questionnaire to update ("DA", "EVAL", or "DAEVAL").
        sent_done: Whether to update the "Sent" or "Done" column for the given type of questionnaire.
    """
    logger.info(f"Updating punch list for {client_id}: {daeval} {sent_done}")
    client_id = str(client_id)
    if daeval == "DA":
        if sent_done == "sent":
            update_punch_list(config, client_id, "DA Qs Sent", "TRUE")
        if sent_done == "done":
            update_punch_list(config, client_id, "DA Qs Done", "TRUE")
    elif daeval == "EVAL":
        if sent_done == "sent":
            update_punch_list(config, client_id, "EVAL Qs Sent", "TRUE")
        if sent_done == "done":
            update_punch_list(config, client_id, "EVAL Qs Done", "TRUE")
    elif daeval == "DAEVAL" and sent_done == "sent":
        if sent_done == "sent":
            update_punch_list(config, client_id, "DA Qs Sent", "TRUE")
            update_punch_list(config, client_id, "EVAL Qs Sent", "TRUE")
        if sent_done == "done":
            update_punch_list(config, client_id, "DA Qs Done", "TRUE")
            update_punch_list(config, client_id, "EVAL Qs Done", "TRUE")


def add_qfailed_to_failure_sheet(
    config: Config, failed_client_dict: dict[str, FailedClient]
):
    """Adds the given failed client to the failure sheet."""
    creds = google_authenticate()

    try:
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        client_id, failed_client = next(iter(failed_client_dict.items()))
        body = {
            "values": [
                [
                    client_id,
                    failed_client["asdAdhd"],
                    failed_client["daEval"],
                    failed_client["error"],
                    failed_client["failedDate"],
                    failed_client["fullName"],
                    ", ".join(failed_client.get("questionnaires_needed", []) or []),
                ]
            ]
        }

        questionnaire_links_generated = failed_client.get(
            "questionnaire_links_generated"
        )
        if questionnaire_links_generated:
            for link in questionnaire_links_generated:
                body["values"][0].extend([str(link.get("type")), str(link.get("link"))])

        sheet.values().append(
            spreadsheetId=config.failed_sheet_id,
            range="clients!A1:Z",
            body=body,
            valueInputOption="USER_ENTERED",
        ).execute()

    except Exception as e:
        logger.exception(e)


def add_simple_to_failure_sheet(
    config: Config,
    client_id: str,
    asdAdhd: str,
    daEval: str,
    error: str,
    failedDate: str,
    fullName: str,
):
    """Adds the information given to the failure sheet."""
    creds = google_authenticate()

    try:
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        body = {"values": [[client_id, asdAdhd, daEval, error, failedDate, fullName]]}

        sheet.values().append(
            spreadsheetId=config.failed_sheet_id,
            range="clients!A1:Z",
            body=body,
            valueInputOption="USER_ENTERED",
        ).execute()

    except Exception as e:
        logger.exception(e)


def move_file_in_drive(service, file_id: str, dest_folder_id: str):
    """Move a file from one folder to another in Google Drive."""
    # Retrieve the existing parents to remove
    file = service.files().get(fileId=file_id, fields="parents").execute()
    previous_parents = ",".join(file.get("parents"))

    # Move the file by updating its parents
    file = (
        service.files()
        .update(
            fileId=file_id,
            addParents=dest_folder_id,
            removeParents=previous_parents,
            fields="id, parents",
        )
        .execute()
    )
