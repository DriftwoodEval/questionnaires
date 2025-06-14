import base64
import hashlib
import os
import re
from datetime import date, datetime
from email.message import EmailMessage
from time import sleep
from typing import Literal
from urllib.parse import urlparse

import asana
import pandas as pd
import pymysql.cursors
import requests
import yaml
from asana.rest import ApiException
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement


def load_config() -> tuple[dict, dict]:
    with open("./config/info.yml", "r") as file:
        logger.debug("Loading config info file")
        info = yaml.safe_load(file)
        services = info["services"]
        config = info["config"]
        return services, config


### SELENIUM ###
def initialize_selenium() -> tuple[WebDriver, ActionChains]:
    logger.info("Initializing Selenium")
    chrome_options: Options = Options()
    chrome_options.add_argument("--no-sandbox")
    if os.getenv("HEADLESS") == "true":
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
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
    for attempt in range(max_attempts):
        try:
            element = driver.find_element(by, locator)
            element.click()
            return
        except (StaleElementReferenceException, NoSuchElementException) as e:
            logger.warning(f"Attempt {attempt + 1} failed: {type(e).__name__}.")
            sleep(delay)
            if refresh:
                logger.info("Refreshing page")
                driver.refresh()
            sleep(delay)
    raise NoSuchElementException(f"Element not found after {max_attempts} attempts")


def find_element(
    driver: WebDriver, by: str, locator: str, max_attempts: int = 3, delay: int = 1
) -> WebElement:
    for attempt in range(max_attempts):
        try:
            element = driver.find_element(by, locator)
            return element
        except (StaleElementReferenceException, NoSuchElementException) as e:
            logger.warning(
                f"Attempt {attempt + 1} failed: {type(e).__name__}. Retrying..."
            )
            sleep(delay)
    raise NoSuchElementException(f"Element not found after {max_attempts} attempts")


def check_if_element_exists(
    driver: WebDriver, by: str, locator: str, max_attempts: int = 3, delay: int = 1
) -> bool:
    for attempt in range(max_attempts):
        try:
            driver.find_element(by, locator)
            return True
        except (StaleElementReferenceException, NoSuchElementException) as e:
            logger.warning(
                f"Attempt {attempt + 1} failed: {type(e).__name__}. Retrying..."
            )
            sleep(delay)
    logger.error(f"Failed to find element after {max_attempts} attempts")
    return False


### DATABASE ###
def get_previous_clients(config, failed: bool = False) -> dict | None:
    logger.info("Loading previous clients")
    qfailure_filepath = "./put/qfailure.yml"

    prev_clients = {}

    if failed:
        try:
            with open(qfailure_filepath, "r") as file:
                prev_clients = yaml.safe_load(file) or {}
        except FileNotFoundError:
            logger.info(f"{qfailure_filepath} does not exist.")

    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = "SELECT * FROM emr_client"
            cursor.execute(sql)
            clients = cursor.fetchall()

            sql = "SELECT * FROM emr_questionnaire"
            cursor.execute(sql)
            questionnaires = cursor.fetchall()
            print(questionnaires)
            for client in clients:
                client["questionnaires"] = [
                    questionnaire
                    for questionnaire in questionnaires
                    if questionnaire["clientId"] == client["id"]
                ]

    if clients:
        for client in clients:
            prev_clients[client["id"]] = {
                key: value for key, value in client.items() if key != "id"
            }

    return prev_clients if prev_clients else None


def get_db(config):
    db_url = urlparse(config["database_url"])
    connection = pymysql.connect(
        host=db_url.hostname,
        port=db_url.port,
        user=db_url.username,
        password=db_url.password,
        database=db_url.path[1:],
        cursorclass=pymysql.cursors.DictCursor,
    )
    return connection


def get_evaluator_npi(config, evaluator_email) -> str | None:
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = "SELECT npi FROM emr_evaluator WHERE email = %s"
            cursor.execute(sql, (evaluator_email))
            npi = cursor.fetchone()
            return npi["npi"] if npi else None


def insert_basic_client(
    config,
    client_id: str,
    asana_id: str,
    dob,
    first_name: str,
    last_name: str,
    asd_adhd: str,
    gender: str,
    phone_number,
):
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = """
                INSERT INTO `emr_client` (id, hash, asanaId, dob, firstName, lastName, fullName, asdAdhd, gender, phoneNumber)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE id=id
            """

            values = (
                int(client_id),
                hashlib.sha256(str(client_id).encode("utf-8")).hexdigest(),
                asana_id if asana_id else None,
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
    config,
    client_id,
    link,
    type,
    sent_date,
    status: Literal["COMPLETED", "PENDING", "RESCHEDULED"],
):
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


def update_yaml(clients: dict, filepath: str) -> None:
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


def add_failure(client: dict) -> None:
    qfailure_filepath = "./put/qfailure.yml"
    qfailsend_filepath = "./put/qfailsend.yml"
    update_yaml(client, qfailure_filepath)
    update_yaml(client, qfailsend_filepath)


### ASANA ###
def init_asana(services: dict) -> asana.ProjectsApi:
    logger.info("Initializing Asana")
    configuration = asana.Configuration()
    configuration.access_token = services["asana"]["token"]
    projects_api = asana.ProjectsApi(asana.ApiClient(configuration))
    return projects_api


def fetch_project(
    projects_api: asana.ProjectsApi,
    project_gid: str,
    opt_fields: str = "name,color,permalink_url,notes,created_at",
) -> dict | None:
    """Fetch the latest version of a single project by its GID"""
    logger.info(f"Fetching project {project_gid}")
    try:
        return projects_api.get_project(
            project_gid,
            opts={"opt_fields": opt_fields},  # type: ignore
        )
    except ApiException as e:
        logger.exception(f"Exception when calling ProjectsApi->get_project: {e}")
        return None


def replace_notes(
    projects_api: asana.ProjectsApi, new_note: str, project_gid: str
) -> bool:
    """Update the notes field in a project."""
    logger.info(f"Updating project {project_gid} with note '{new_note}'")
    body = {"data": {"notes": new_note}}
    try:
        projects_api.update_project(
            body, project_gid, opts={"opt_fields": "name, notes"}
        )
        return True
    except ApiException as e:
        logger.exception(f"Exception when calling ProjectsApi->update_project: {e}")
        return False


def add_note(
    config: dict,
    projects_api: asana.ProjectsApi,
    project_gid: str,
    new_note: str,
    raw_note: bool = False,
):
    today_str = datetime.now().strftime("%m/%d")
    if not raw_note:
        new_note = today_str + " " + new_note
        initials = config["initials"]
        if initials:
            new_note += " ///" + initials

    current_project: dict[str, str] | None = fetch_project(projects_api, project_gid)
    if current_project:
        current_notes = current_project.get("notes", "")
        notes_by_line = current_notes.split("\n")
        # Check if there is a blank line in the first 5 lines
        blank_line_index = next(
            (i for i, line in enumerate(notes_by_line[:5]) if not line.strip()),
            None,
        )
        if blank_line_index is not None:
            # If there is a blank line in the first 5 line, insert the new note after it
            notes_by_line.insert(blank_line_index + 1, new_note)
        else:
            # Otherwise, add the note to the top as normal
            notes_by_line.insert(0, new_note)
        new_notes = "\n".join(notes_by_line)
        replace_notes(projects_api, new_notes, project_gid)


def search_by_name(
    projects_api: asana.ProjectsApi, services: dict, name: str
) -> dict | None:
    name = str(name)
    opts = {
        "limit": 100,
        "archived": False,
        "opt_fields": "name,color,permalink_url,notes",
    }
    try:
        logger.info(f"Searching projects for {name}...")

        api_response = list(
            projects_api.get_projects_for_workspace(
                services["asana"]["workspace"],
                opts,  # pyright: ignore (asana api is strange)
            )
        )

    except ApiException as e:
        logger.exception(
            "Exception when calling ProjectsApi->get_projects_for_workspace: %s\n" % e
        )
        return

    if api_response:
        filtered_projects = [
            data
            for data in api_response
            if name.lower()
            in re.sub(r"\s+", " ", data["name"].replace('"', "")).strip().lower()
        ]
        project_count = len(filtered_projects)

        correct_project = None

        if project_count == 0:
            logger.error(f"No projects found for {name}.")
        elif project_count == 1:
            logger.success(f"Found 1 project for {name}.")
            correct_project = filtered_projects[0]
        else:
            logger.error(f"Found {project_count} projects for {name}.")
        if correct_project:
            return correct_project
        else:
            return None


def search_and_add_note(
    projects_api: asana.ProjectsApi,
    services,
    config,
    name,
    note,
    raw_note: bool = False,
) -> str | bool:
    project = search_by_name(projects_api, services, name)
    if project:
        add_note(config, projects_api, project["gid"], note, raw_note)
        return project["gid"]
    else:
        return False


def search_and_add_questionnaires(
    projects_api: asana.ProjectsApi,
    services,
    config,
    client: pd.Series,
    questionnaires: list[dict],
) -> pd.Series:
    questionnaire_links_format = [
        f"{item['link']} - {item['type']}" for item in questionnaires
    ]
    questionnaire_links_str = "\n".join(questionnaire_links_format)
    questionnaire_links_str = (
        datetime.now().strftime("%m/%d")
        + " Qs sent automatically\n"
        + questionnaire_links_str
    )
    asana_link = search_and_add_note(
        projects_api,
        services,
        config,
        re.sub(r"C0+", "", client["Client ID"]),
        questionnaire_links_str,
        True,
    )
    if not asana_link:
        name = client["Client Name"]
        asana_link = search_and_add_note(
            projects_api,
            services,
            config,
            name,
            questionnaire_links_str,
            True,
        )
    if not asana_link:
        name = f"{client['TA First Name']} {client['TA Last Name']}"
        asana_link = search_and_add_note(
            projects_api,
            services,
            config,
            name,
            questionnaire_links_str,
            True,
        )
    client["Asana"] = asana_link
    return client


def mark_link_done(
    projects_api: asana.ProjectsApi, services, config, project_gid: str, link: str
) -> None:
    project = fetch_project(projects_api, project_gid)
    if project:
        notes = project["notes"]
        link_start = notes.find(link)
        if link_start == -1:
            logger.error(f"Link {link} not found in project notes")
            return
        logger.debug(f"Found link {link} in project notes")
        link_end = notes.find("\n", link_start)
        if link_end == -1:
            link_end = len(notes)
        link_done = notes[link_start:link_end].strip()
        if " - Ready to Download" in link_done:
            logger.debug(f"Link {link} is already marked as Ready to Download")
            return
        logger.debug(f"Marking link {link} as Ready to Download")
        link_done = f"{link_done} - Ready to Download"
        new_note = notes[:link_start] + link_done + notes[link_end:]
        replace_notes(projects_api, new_note, project_gid)


def mark_links_in_asana(
    projects_api: asana.ProjectsApi, client: dict, services: dict, config: dict
) -> None:
    if client.get("asana") and client["asana"]:
        for questionnaire in client["questionnaires"]:
            if questionnaire["done"]:
                mark_link_done(
                    projects_api,
                    services,
                    config,
                    client["asana"],
                    questionnaire["link"],
                )
    else:
        logger.error(
            f"Client {client['firstname']} {client['lastname']} has no Asana link"
        )


def sent_reminder_asana(
    config: dict, projects_api: asana.ProjectsApi, client: dict
) -> None:
    if client.get("asana") and client["asana"]:
        add_note(
            config,
            projects_api,
            client["asana"],
            "Sent reminder",
        )
    else:
        logger.error(
            f"Client {client['firstname']} {client['lastname']} has no Asana link"
        )


### QUESTIONNAIRES ###
def all_questionnaires_done(client) -> bool:
    for q in client["questionnaires"]:
        if not isinstance(q, dict):
            logger.error(
                f"{q} in {client['firstname']} {client['lastname']} is not a dictionary."
            )
            return False
    return all(q["done"] for q in client["questionnaires"] if isinstance(q, dict))


def check_q_done(driver: WebDriver, q_link: str) -> bool:
    driver.implicitly_wait(3)
    url = q_link
    driver.get(url)

    complete = False

    if "mhs.com" in url:
        logger.info(f"Checking MHS completion for {url}")
        complete = check_if_element_exists(
            driver,
            By.XPATH,
            "//*[contains(text(), 'Thank you for completing')] | //*[contains(text(), 'This link has already been used')] | //*[contains(text(), 'We have received your answers')]",
        )
    elif "pearsonassessments.com" in url:
        logger.info(f"Checking Pearson completion for {url}")
        complete = check_if_element_exists(
            driver, By.XPATH, "//*[contains(text(), 'Test Completed!')]"
        )
    elif "wpspublish" in url:
        logger.info(f"Checking WPS completion for {url}")
        complete = check_if_element_exists(
            driver,
            By.XPATH,
            "//*[contains(text(), 'This assessment is not available at this time')]",
        )

    return complete


def check_questionnaires(
    driver: WebDriver, config: dict, services: dict, clients: dict
) -> dict | None:
    if clients:
        completed_clients = {}
        for id in clients:
            client = clients[id]
            if all_questionnaires_done(client):
                if client["date"] == "Reschedule":
                    logger.info(
                        f"Client {client['firstname']} {client['lastname']} has rescheduled, but already completed their questionnaires for an appointment"
                    )
                    continue
                logger.info(
                    f"{client['firstname']} {client['lastname']} has already completed their questionnaires for an appointment on {format_appointment(client)}"
                )
                continue
            for questionnaire in client["questionnaires"]:
                if questionnaire["done"]:
                    logger.info(
                        f"{client['firstname']} {client['lastname']}'s {questionnaire['type']} is already done"
                    )
                    continue
                logger.info(
                    f"Checking {client['firstname']} {client['lastname']}'s {questionnaire['type']}"
                )
                questionnaire["done"] = check_q_done(driver, questionnaire["link"])
                logger.info(
                    f"{client['firstname']} {client['lastname']}'s {questionnaire['type']} is {'' if questionnaire['done'] else 'not '}done"
                )
                if not questionnaire["done"]:
                    logger.info(
                        f"At least one questionnaire is not done for {client['firstname']} {client['lastname']}"
                    )
                    break
            if all_questionnaires_done(client):
                distance = check_appointment_distance(
                    datetime.strptime(client["date"], "%Y/%m/%d").date()
                )
                if str(distance) not in completed_clients:
                    completed_clients[str(distance)] = []
                completed_clients[str(distance)].append(
                    f"{client['firstname']} {client['lastname']}"
                )
        update_yaml(clients, "./put/clients.yml")
        return completed_clients


### FORMATTING ###
def format_appointment(client: dict) -> str:
    appointment = client["date"]
    return datetime.strptime(appointment, "%Y/%m/%d").strftime("%A, %B %-d")


def format_phone_number(raw_number: str) -> str:
    return f"({raw_number[:3]}) {raw_number[3:6]}-{raw_number[6:]}"


def check_appointment_distance(appointment: date) -> int:
    today = date.today()
    delta = appointment - today
    return delta.days


### GMAIL ###

# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


def google_authenticate():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("./config/token.json"):
        creds = Credentials.from_authorized_user_file("./config/token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
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
    cc_addr: str | None = None,
    html: str | None = None,
):
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


def build_admin_email(email_info: dict) -> tuple[str, str]:
    email_text = ""
    email_html = ""
    if email_info["completed"]:
        completed_text = []
        completed_html = []
        for days, client_list in email_info["completed"].items():
            if days == "-1":
                completed_text.append("  Appointment yesterday:")
                completed_html.append("<h3>Appointment yesterday:</h3><ul>")
            elif days == "0":
                completed_text.append("  Appointment today:")
                completed_html.append("<h3>Appointment today:</h3><ul>")
            elif days == "1":
                completed_text.append("  Appointment tomorrow:")
                completed_html.append("<h3>Appointment tomorrow:</h3><ul>")
            elif int(days) < 0:
                completed_text.append(f"  Appointment {abs(int(days))} days ago:")
                completed_html.append(
                    f"<h3>Appointment {abs(int(days))} days ago:</h3><ul>"
                )
            else:
                completed_text.append(f"  Appointment in {days} days:")
                completed_html.append(f"<h3>Appointment in {days} days:</h3><ul>")
            for client in client_list:
                completed_text.append(f"    - {client}")
                completed_html.append(f"<li>{client}</li>")
            completed_html.append("</ul>")
        email_text += "Download:\n" + "\n".join(completed_text) + "\n"
        email_html += "<h2>Download</h2>" + "".join(completed_html)
    if email_info["reschedule"]:
        email_text += f"Check on rescheduled: {', '.join(email_info['reschedule'])}\n"
        email_html += f"<h2>Check on rescheduled</h2><ul><li>{'</li><li>'.join(email_info['reschedule'])}</li></ul>"
    if email_info["failed"]:
        email_text += f"Failed to message: {', '.join(email_info['failed'])}\n"
        email_html += f"<h2>Failed to message</h2><ul><li>{'</li><li>'.join(email_info['failed'])}</li></ul>"
    if email_info["call"]:
        call_text = []
        call_html = []
        for days, client_list in email_info["call"].items():
            if days == "-1":
                call_text.append("  Appointment yesterday:")
                call_html.append("<h3>Appointment yesterday:</h3><ul>")
            elif days == "0":
                call_text.append("  Appointment today:")
                call_html.append("<h3>Appointment today:</h3><ul>")
            elif days == "1":
                call_text.append("  Appointment tomorrow:")
                call_html.append("<h3>Appointment tomorrow:</h3><ul>")
            elif int(days) < 0:
                call_text.append(f"  Appointment {abs(int(days))} days ago:")
                call_html.append(f"<h3>Appointment {abs(int(days))} days ago:</h3><ul>")
            else:
                call_text.append(f"  Appointment in {days} days:")
                call_html.append(f"<h3>Appointment in {days} days:</h3><ul>")
            for client in client_list:
                call_text.append(f"    - {client}")
                call_html.append(f"<li>{client}</li>")
            call_html.append("</ul>")
        email_text += "Call:\n" + "\n".join(call_text)
        email_html += "<h2>Call</h2>" + "".join(call_html)
    return email_text, email_html


def get_punch_list(config):
    creds = google_authenticate()

    try:
        service = build("sheets", "v4", credentials=creds)

        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(
                spreadsheetId=config["punch_list_id"],
                range=config["punch_list_range"],
            )
            .execute()
        )
        values = result.get("values", [])

        if values:
            df = pd.DataFrame(values[1:], columns=values[0])
            df.to_csv("clients_to_send.csv", index=False)

            df = df.rename(columns={df.columns[0]: "Client Name"})

            df = df[
                [
                    "Client Name",
                    "Client ID",
                    "For",
                    "DA Qs Needed",
                    "DA Qs Sent",
                    "EVAL Qs Needed",
                    "EVAL Qs Sent",
                ]
            ]

            df["Client ID"] = df["Client ID"].apply(
                lambda client_id: re.sub(r"^C?0*", "", client_id)
            )

            df["Human Friendly ID"] = df["Client ID"].apply(
                lambda client_id: f"C{client_id.zfill(9)}"
            )

            return df
    except Exception as e:
        logger.exception(e)


def update_punch_list(config, name_for_search: str, update_header: str, new_value: str):
    creds = google_authenticate()

    try:
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(
                spreadsheetId=config["punch_list_id"],
                range=config["punch_list_range"],
            )
            .execute()
        )
        values = result.get("values", [])

        row_number = None
        for i, row in enumerate(values):
            if row and row[0] == name_for_search:
                row_number = i + 1  # Spreadsheets are 1-indexed
                break

        update_column = None
        for i, header in enumerate(values[0]):
            if header == update_header:
                update_column = chr(ord("A") + i)
                break

        if row_number is not None and update_column is not None:
            sheet_name = config["punch_list_range"].split("!")[0]
            update_range = f"{sheet_name}!{update_column}{row_number}"
            body = {"values": [[new_value]]}
            result = (
                sheet.values()
                .update(
                    spreadsheetId=config["punch_list_id"],
                    range=update_range,
                    valueInputOption="USER_ENTERED",
                    body=body,
                )
                .execute()
            )
            logger.success(
                f"Updated {update_column} for {name_for_search} in Punch List"
            )
        else:
            logger.error(f"Client {name_for_search} not found in Punch List")
    except Exception as e:
        logger.exception(e)


def update_punch_by_daeval(config, client_name, daeval):
    if daeval == "DA":
        update_punch_list(config, client_name, "DA Qs Sent", "TRUE")
    elif daeval == "EVAL":
        update_punch_list(config, client_name, "EVAL Qs Sent", "TRUE")
    elif daeval == "DAEVAL":
        update_punch_list(config, client_name, "DA Qs Sent", "TRUE")
        update_punch_list(config, client_name, "EVAL Qs Sent", "TRUE")
