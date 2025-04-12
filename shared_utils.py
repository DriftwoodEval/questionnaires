import logging
import os
import re
from datetime import datetime
from time import sleep

import asana
import yaml
from asana.rest import ApiException
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains


def load_config():
    with open("./config/info.yml", "r") as file:
        logging.info("Loading info file")
        info = yaml.safe_load(file)
        services = info["services"]
        config = info["config"]
        return services, config


def initialize_selenium():
    logging.info("Initializing Selenium")
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    if os.getenv("HEADLESS") == "true":
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    actions = ActionChains(driver)
    driver.implicitly_wait(5)
    driver.set_window_size(1920, 1080)
    return driver, actions


def click_element(driver, by, locator, max_attempts=3, delay=1):
    for attempt in range(max_attempts):
        try:
            element = driver.find_element(by, locator)
            element.click()
            return True
        except (StaleElementReferenceException, NoSuchElementException) as e:
            logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
            sleep(delay)
    logging.error(f"Failed to click element after {max_attempts} attempts")
    return False


def find_element(driver, by, locator, max_attempts=3, delay=1):
    for attempt in range(max_attempts):
        try:
            driver.find_element(by, locator)
            return True
        except (StaleElementReferenceException, NoSuchElementException) as e:
            logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
            sleep(delay)
    logging.error(f"Failed to find element after {max_attempts} attempts")
    return False


def get_previous_clients(failed=False):
    logging.info("Loading previous clients")
    clients_filepath = "./put/clients.yml"
    qfailure_filepath = "./put/qfailure.yml"

    prev_clients = {}

    if failed:
        try:
            with open(qfailure_filepath, "r") as file:
                prev_clients = yaml.safe_load(file) or {}
        except FileNotFoundError:
            logging.info(f"{qfailure_filepath} does not exist.")

    try:
        with open(clients_filepath, "r") as file:
            clients_data = yaml.safe_load(file) or {}
            prev_clients.update(clients_data)
    except FileNotFoundError:
        logging.info(f"{clients_filepath} does not exist.")

    return prev_clients if prev_clients else None


def update_yaml(clients, filepath):
    try:
        with open(filepath, "r") as file:
            current_yaml = yaml.safe_load(file)
    except FileNotFoundError:
        logging.info(f"{filepath} does not exist, creating new file")
        current_yaml = None

    if current_yaml is None:
        logging.info(f"Dumping {clients} to {filepath}")
        with open(filepath, "w") as file:
            yaml.dump(clients, file, default_flow_style=False)
    else:
        logging.info(f"Updating {filepath}")
        current_yaml.update(clients)
        with open(filepath, "w") as file:
            logging.info(f"Dumping {clients} to {filepath}")
            yaml.dump(current_yaml, file, default_flow_style=False)


def init_asana(services):
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
    logging.info(f"Fetching project {project_gid}")
    try:
        return projects_api.get_project(
            project_gid,
            opts={"opt_fields": opt_fields},  # type: ignore
        )
    except ApiException as e:
        logging.exception(f"Exception when calling ProjectsApi->get_project: {e}")
        return None


def replace_notes(projects_api: asana.ProjectsApi, new_note: str, project_gid: str):
    """Update the notes field in a project."""
    logging.info(f"Updating project {project_gid} with note '{new_note}'")
    body = {"data": {"notes": new_note}}
    try:
        projects_api.update_project(
            body, project_gid, opts={"opt_fields": "name, notes"}
        )
        return True
    except ApiException as e:
        logging.exception(f"Exception when calling ProjectsApi->update_project: {e}")
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


def search_by_name(projects_api: asana.ProjectsApi, services, name):
    opts = {
        "limit": 100,
        "archived": False,
        "opt_fields": "name,color,permalink_url,notes",
    }
    try:
        print(f"Searching projects for {name}...")

        api_response = list(
            projects_api.get_projects_for_workspace(
                services["asana"]["workspace"],
                opts,  # pyright: ignore (asana api is strange)
            )
        )

    except ApiException as e:
        print(
            "Exception when calling ProjectsApi->get_projects_for_workspace: %s\n" % e
        )
        return

    if api_response:
        filtered_projects = [
            data
            for data in api_response
            if name.lower() in re.sub(r"\s+", " ", data["name"]).strip().lower()
        ]
        project_count = len(filtered_projects)

        correct_project = None

        if project_count == 0:
            logging.warning(f"No projects found for {name}.")
        elif project_count == 1:
            logging.info(f"Found 1 project for {name}.")
            correct_project = filtered_projects[0]
        else:
            logging.warning(f"Found {project_count} projects for {name}.")
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
):
    project = search_by_name(projects_api, services, name)
    if project:
        add_note(config, projects_api, project["gid"], note, raw_note)
        return project["gid"]
    else:
        return False


def search_and_add_questionnaires(
    projects_api: asana.ProjectsApi, services, config, client: dict
):
    questionnaire_links_format = [
        f"{item['link']} - {item['type']}" for item in client["questionnaires"]
    ]
    questionnaire_links_str = "\n".join(questionnaire_links_format)
    questionnaire_links_str = (
        datetime.now().strftime("%m/%d")
        + f" Qs sent {config['initials']}\n"
        + questionnaire_links_str
    )
    asana_link = search_and_add_note(
        projects_api,
        services,
        config,
        f"{client['firstname']} {client['lastname']}",
        questionnaire_links_str,
        True,
    )
    client["asana"] = asana_link
    return client


def mark_link_done(
    projects_api: asana.ProjectsApi, services, config, project_gid: str, link: str
):
    project = fetch_project(projects_api, project_gid)
    if project:
        notes = project["notes"]
        link_start = notes.find(link)
        if link_start == -1:
            logging.warning(f"Link {link} not found in project notes")
            return
        link_end = notes.find("\n", link_start)
        if link_end == -1:
            link_end = len(notes)
        link_done = notes[link_start:link_end].strip()
        if " - DONE" in link_done:
            logging.info(f"Link {link} is already marked as DONE")
            return
        link_done = f"{link_done} - DONE"
        new_note = notes[:link_start] + link_done + notes[link_end:]
        replace_notes(projects_api, new_note, project_gid)
