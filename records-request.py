import io
from base64 import b64decode
from datetime import date
from pathlib import Path

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from loguru import logger
from nameparser import HumanName
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.print_page_options import PrintOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import shared_utils as utils

SUCCESS_FILE = Path("put/savedrecords.txt")
FAILURE_FILE = Path("put/recordfailures.txt")
OUTPUT_DIR = Path("School Records Requests")
WAIT_TIMEOUT = 15  # seconds


def get_clients_to_request(config: utils.Config) -> pd.DataFrame | None:
    """Gets a list of clients from the punch list who need to have their records requested.

    The list is filtered to only include clients who have a "TRUE" value in the "Records Needed" column, but not in the "Records Requested?" or "Records Reviewed?" columns.

    Returns:
        pandas.DataFrame | None: A DataFrame containing the punch list data, or None if the punch list is empty.
    """
    punch_list = utils.get_punch_list(config)

    if punch_list is None:
        logger.critical("Punch list is empty")
        return None

    punch_list = punch_list[
        (punch_list["Records Needed"] == "TRUE")
        & (punch_list["Records Requested?"] != "TRUE")
        & (punch_list["Records Reviewed?"] != "TRUE")
    ]

    return punch_list


def append_to_csv_file(filepath: Path, data: str):
    """Appends data to a comma-separated file, handling separators correctly."""
    prefix = ""
    # Add a separator only if the file already exists and is not empty
    if filepath.exists() and filepath.stat().st_size > 0:
        prefix = ", "

    with open(filepath, "a") as f:
        f.write(f"{prefix}{data}")


class TherapyAppointmentBot:
    """A bot to automate downloading client documents from TherapyAppointment."""

    def __init__(self, services: utils.Services, config: utils.Config):
        """Initializes the TherapyAppointmentBot."""
        self.taconfig = services["therapyappointment"]
        self.config = config
        self.driver = self._initialize_driver()
        self.wait = WebDriverWait(self.driver, WAIT_TIMEOUT)

    def _initialize_driver(self) -> WebDriver:
        """Initializes the Chrome WebDriver."""
        chrome_options = Options()
        driver = webdriver.Chrome(options=chrome_options)
        return driver

    def __enter__(self):
        """Allows using the bot as a context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures the driver is closed properly on exit."""
        logger.info("Closing WebDriver.")
        if self.driver:
            self.driver.quit()

    def login(self):
        """Logs into the TherapyAppointment portal."""
        logger.info("Logging into TherapyAppointment...")
        self.driver.get("https://portal.therapyappointment.com")
        self.driver.maximize_window()

        username_field = self.wait.until(
            EC.presence_of_element_located((By.NAME, "user_username"))
        )
        username_field.send_keys(self.taconfig["username"])

        password_field = self.driver.find_element(By.NAME, "user_password")
        password_field.send_keys(self.taconfig["password"])
        password_field.submit()
        logger.success("Login successful.")

    def go_to_client(self, client_id: str) -> bool:
        """Navigates to a specific client in TherapyAppointment."""
        logger.info(f"Searching for client: {client_id}...")
        try:
            # Wait for the main navigation to be clickable
            clients_button = self.wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//*[contains(text(), 'Clients')]")
                )
            )
            clients_button.click()

            # Wait for the search form to be ready
            client_id_field = self.wait.until(
                EC.visibility_of_element_located(
                    (
                        By.XPATH,
                        "//label[text()='Account Number']/following-sibling::input",
                    )
                )
            )
            client_id_field.send_keys(client_id)

            search_button = self.driver.find_element(
                By.CSS_SELECTOR, "button[aria-label='Search']"
            )
            search_button.click()

            # Wait for the search result link and click it
            client_link = self.wait.until(
                EC.element_to_be_clickable(
                    (
                        By.CSS_SELECTOR,
                        "a[aria-description*='Press Enter to view the profile of']",
                    )
                )
            )
            client_link.click()
            return True
        except TimeoutException:
            logger.warning(f"Client not found or search failed for: {client_id}")
            return False
        except NoSuchElementException:
            logger.warning(f"Could not find a search element for: {client_id}")
            return False

    def extract_client_data(self) -> dict:
        """Extracts and returns client data from their TherapyAppointment page."""
        logger.info("Extracting client data...")
        # Wait for the name to ensure the page is loaded
        name_element = self.wait.until(
            EC.visibility_of_element_located((By.CLASS_NAME, "text-h4"))
        )
        name = HumanName(name_element.text)

        birthdate = (
            self.driver.find_element(
                By.XPATH, "//div[contains(normalize-space(text()), 'DOB ')]"
            )
            .text.split()[-1]
            .replace("/", "")
        )

        keepcharacters = (" ", ".", "_")
        safe_fullname = "".join(
            c for c in f"{name.first} {name.last}" if c.isalnum() or c in keepcharacters
        ).rstrip()

        data = {
            "fullname": safe_fullname,
            "birthdate": birthdate,
        }
        logger.info(f"Client data extracted: {data}")
        return data

    def check_if_opened_portal(self) -> bool:
        """Check if the TA portal has been opened by the client."""
        try:
            self.driver.find_element(By.CSS_SELECTOR, "input[aria-checked='true']")
            return True
        except NoSuchElementException:
            raise Exception("Portal not opened.")

    def check_if_docs_signed(self) -> bool:
        """Check if the TA docs have been signed by the client."""
        try:
            self.driver.find_element(
                By.XPATH,
                "//div[contains(normalize-space(text()), 'has completed registration')]",
            )
            return True
        except NoSuchElementException:
            raise Exception("Docs not signed.")

    def download_consent_forms(self, client_data: dict):
        """Navigates to Docs & Forms and saves consent forms as PDFs."""
        creds = utils.google_authenticate()

        service = build("drive", "v3", credentials=creds)
        filename = f"{client_data['fullname'].title()} {client_data['birthdate']} Receiving.pdf"
        receive = self.file_exists(service, filename, self.config.records_folder_id)
        filename = (
            f"{client_data['fullname'].title()} {client_data['birthdate']} Sending.pdf"
        )
        send = self.file_exists(service, filename, self.config.records_folder_id)
        if receive or send:
            logger.info("Files already exist, skipping download.")
            return

        logger.info("Navigating to Docs & Forms...")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        self.save_document_as_pdf(
            "Receiving Consent to Release of Information", client_data
        )
        self.save_document_as_pdf(
            "Sending Consent to Release of Information", client_data
        )

        clients_button = self.driver.find_element(
            By.XPATH, "//*[contains(text(), 'Clients')]"
        )
        clients_button.click()

    def upload_pdf_from_driver(self, filename, folder_id):
        """Prints page as PDF (in memory) and uploads to Drive."""
        pdf_options = PrintOptions()
        pdf_options.orientation = "portrait"

        pdf_base64 = self.driver.print_page(pdf_options)

        pdf_bytes = b64decode(pdf_base64)
        pdf_stream = io.BytesIO(pdf_bytes)

        creds = utils.google_authenticate()

        service = build("drive", "v3", credentials=creds)
        file_metadata = {
            "name": filename,
            "parents": [folder_id],
        }
        media = MediaIoBaseUpload(pdf_stream, mimetype="application/pdf")

        uploaded_file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id, webViewLink")
            .execute()
        )

        return uploaded_file

    def save_document_as_pdf(self, link_text: str, client: dict):
        """Helper function to find, print, and save a single document."""
        logger.info(f"Opening {link_text}...")
        try:
            docs_button = self.wait.until(
                EC.element_to_be_clickable((By.LINK_TEXT, "Docs & Forms"))
            )
            docs_button.click()

            document_link = self.wait.until(
                EC.element_to_be_clickable((By.LINK_TEXT, link_text))
            )
            document_link.click()

            self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//*[contains(text(), 'I authorize')]")
                )
            )

            doc_type = link_text.split(" ")[0]

            filename = (
                f"{client['fullname'].title()} {client['birthdate']} {doc_type}.pdf"
            )

            logger.info(f"Saving {filename}...")
            self.upload_pdf_from_driver(filename, self.config.records_folder_id)

        except TimeoutException:
            logger.error(f"Could not find or load document: {link_text}")
        finally:
            # Go back to the Docs & Forms list
            self.driver.back()

    def file_exists(self, service, filename, folder_id):
        """Helper function to check if a file exists in Drive."""
        query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
        results = (
            service.files()
            .list(q=query, spaces="drive", fields="files(id, name)", pageSize=1)
            .execute()
        )

        files = results.get("files", [])
        if files:
            return files[0]["id"]  # return the ID of the first matching file
        return None


def main():
    """Main function to run the automation script."""
    services, config = utils.load_config()

    clients_to_process = get_clients_to_request(config)
    new_clients = []

    if clients_to_process is None:
        logger.critical("No clients found.")
        return

    today = date.today()

    if not new_clients:
        logger.info("No new clients to process.")
        return

    logger.info(f"Found {len(new_clients)} new clients to process.")

    new_success_count = 0
    new_failure_count = 0

    with TherapyAppointmentBot(services, config) as bot:
        bot.login()
        for _, client in clients_to_process.iterrows():
            client_id = client["Client ID"]
            client_name = client["Client Name"]
            asdAdhd = client["For"]

            if bot.go_to_client(client_id):
                try:
                    client_data = bot.extract_client_data()
                    bot.check_if_opened_portal()
                    bot.check_if_docs_signed()
                    bot.download_consent_forms(client_data)
                    append_to_csv_file(Path(SUCCESS_FILE), client_name)
                    new_success_count += 1
                except Exception as e:
                    logger.error(
                        f"An error occurred while processing {client_name}: {e}"
                    )
                    utils.add_simple_to_failure_sheet(
                        config,
                        client_id,
                        asdAdhd,
                        "Records Request",
                        str(e),
                        str(today),
                        client_name,
                    )
                    new_failure_count += 1
            else:
                utils.add_simple_to_failure_sheet(
                    config,
                    client_id,
                    asdAdhd,
                    "Records Request",
                    "Client not found",
                    str(today),
                    client_name,
                )
                new_failure_count += 1

    logger.info(
        f"Process complete. Success: {new_success_count}, Failed: {new_failure_count}"
    )


if __name__ == "__main__":
    main()
