import io
import re
import time
from base64 import b64decode
from datetime import date
from pathlib import Path

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from loguru import logger
from PyPDF2 import PdfReader
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.print_page_options import PrintOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.database import get_previous_clients
from utils.google import (
    get_punch_list,
    google_authenticate,
    move_file_in_drive,
    send_gmail,
    update_punch_list,
)
from utils.misc import add_failure, load_config
from utils.types import ClientFromDB, Config, Services

SUCCESS_FILE = Path("put/savedrecords.txt")
FAILURE_FILE = Path("put/recordfailures.txt")
WAIT_TIMEOUT = 15  # seconds


def get_clients_to_request(config: Config) -> pd.DataFrame | None:
    """Gets a list of clients from the punch list who need to have their records requested.

    The list is filtered to only include clients who have a "TRUE" value in the "Records Needed" column, but not in the "Records Requested?" or "Records Reviewed?" columns.

    Returns:
        pandas.DataFrame | None: A DataFrame containing the punch list data, or None if the punch list is empty.
    """
    punch_list = get_punch_list(config)

    if punch_list is None:
        logger.critical("Punch list is empty")
        return None

    punch_list = punch_list[
        (punch_list["Records Needed"] == "TRUE")
        & (punch_list["Records Requested?"] != "TRUE")
        & (punch_list["Records Reviewed?"] != "TRUE")
        & (punch_list["For"] != "ADHD")
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

    def __init__(self, services: Services, config: Config):
        """Initializes the TherapyAppointmentBot."""
        self.taconfig = services["therapyappointment"]
        self.config = config
        self.driver = self._initialize_driver()
        self.wait = WebDriverWait(self.driver, WAIT_TIMEOUT)

    def _initialize_driver(self) -> WebDriver:
        """Initializes the Chrome WebDriver."""
        logger.debug("Initializing WebDriver...")
        chrome_options = Options()
        driver = webdriver.Chrome(options=chrome_options)
        return driver

    def __enter__(self):
        """Allows using the bot as a context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures the driver is closed properly on exit."""
        logger.debug("Closing WebDriver.")
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
            clients_button = self.wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//*[contains(text(), 'Clients')]")
                )
            )
            clients_button.click()

            client_id_field = self.wait.until(
                EC.visibility_of_element_located(
                    (
                        By.XPATH,
                        "//label[text()='Account Number']/following-sibling::input",
                    )
                )
            )
            client_id_field.send_keys(client_id)

            search_button = self.wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button[aria-label='Search']")
                )
            )
            search_button.click()

            logger.debug("Entering client's page...")
            client_link = self.wait.until(
                EC.element_to_be_clickable(
                    (
                        By.CSS_SELECTOR,
                        "a[aria-description*='Press Enter to view the profile of']",
                    )
                )
            )
            try:
                client_link.click()
            except ElementClickInterceptedException:
                logger.warning(
                    "Click intercepted by popup, trying ESC and clicking again..."
                )
                actions = ActionChains(self.driver)
                actions.send_keys(Keys.ESCAPE)
                actions.perform()
                time.sleep(1)
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
            logger.exception(f"Client not found or search failed for: {client_id}")
            return False
        except NoSuchElementException:
            logger.warning(f"Could not find a search element for: {client_id}")
            return False

    def check_if_opened_portal(self) -> bool:
        """Check if the TA portal has been opened by the client."""
        logger.info("Checking if portal has been opened...")
        try:
            self.wait.until(
                EC.visibility_of_element_located(
                    (
                        By.XPATH,
                        "//div[contains(normalize-space(text()), 'Username:')]",
                    )
                )
            )
            logger.debug("Client has opened portal")
            return True
        except (NoSuchElementException, TimeoutException):
            raise Exception("portal not opened")

    def check_if_docs_signed(self) -> bool:
        """Check if the TA docs have been signed by the client."""
        logger.info("Checking if docs have been signed...")
        try:
            self.wait.until(
                EC.visibility_of_element_located(
                    (
                        By.XPATH,
                        "//div[contains(normalize-space(text()), 'has completed registration')]",
                    )
                )
            )
            logger.debug("Docs have been signed")
            return True
        except (NoSuchElementException, TimeoutException):
            raise Exception("docs not signed")

    def download_consent_forms(
        self, client: ClientFromDB, school_contacts: dict
    ) -> bool:
        """Navigates to Docs & Forms and saves consent forms as PDFs."""
        creds = google_authenticate()

        service = build("drive", "v3", credentials=creds)

        logger.debug("Checking if files already exist...")
        check_filename = f"{client.firstName} {client.lastName} {client.dob.strftime('%m%d%Y')} Receiving.pdf"
        prev_receive = self.file_exists(
            service, check_filename, self.config.records_folder_id
        )
        check_filename = f"{client.firstName} {client.lastName} {client.dob.strftime('%m%d%Y')} Sending.pdf"
        prev_send = self.file_exists(
            service, check_filename, self.config.records_folder_id
        )

        if prev_receive or prev_send:
            logger.warning(
                f"Files already exist for {client.firstName} {client.lastName}, skipping download."
            )
            return True

        logger.info("Navigating to Docs & Forms...")

        receiving_stream, receiving_filename, receiving_school, receiving_drive_file = (
            self.save_document_as_pdf(
                "Receiving Consent to Release of Information", client
            )
        )
        sending_stream, sending_filename, sending_school, sending_drive_file = (
            self.save_document_as_pdf(
                "Sending Consent to Release of Information", client
            )
        )

        if sending_school != receiving_school:
            logger.warning(
                f"School on Sending, {sending_school}, is not the same as school on Receiving, {receiving_school}"
            )
            raise (Exception("District on receive does not match district on send"))
        else:
            try:
                school_address = school_contacts[sending_school]
            except KeyError:
                raise (
                    Exception(
                        f"School found, {sending_school}, has no email address assigned."
                    )
                )

        message_text = f"Re: Student: {client.firstName} {client.lastName}\nDate of Birth: {client.dob.strftime('%m/%d/%Y')}\n\nPlease find Consent to Release of Information attached for the above referenced student. Please send the most recent IEP, any Evaluation Reports, and any Reevaluation Review information.\n\nIf the child is currently undergoing evaluation, please provide the date of the Consent for Evaluation.\n\nThank you for your time!"

        send_gmail(
            message_text=message_text,
            subject=f"Re: Student: {client.firstName} {client.lastName}",
            to_addr=school_address,
            from_addr="records@driftwoodeval.com",
            pdf_stream0=receiving_stream,
            filename0=receiving_filename,
            pdf_stream1=sending_stream,
            filename1=sending_filename,
        )

        try:
            move_file_in_drive(
                service,
                receiving_drive_file["id"],
                self.config.sent_records_folder_id,
            )
            move_file_in_drive(
                service,
                sending_drive_file["id"],
                self.config.sent_records_folder_id,
            )
        except Exception as e:
            logger.error(f"Error moving files to sent folder: {e}")
            return False

        update_punch_list(self.config, str(client.id), "Records Requested?", "TRUE")

        return False

    def upload_pdf_from_driver(
        self, filename: str, folder_id: str
    ) -> tuple[io.BytesIO, str, str, dict]:
        """Prints page as PDF (in memory) and uploads to Drive."""
        pdf_options = PrintOptions()
        pdf_options.orientation = "portrait"

        pdf_base64 = self.driver.print_page(pdf_options)

        pdf_bytes = b64decode(pdf_base64)
        pdf_stream = io.BytesIO(pdf_bytes)

        school = self.extract_school_district_name(pdf_stream)

        creds = google_authenticate()

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

        return pdf_stream, filename, school, uploaded_file

    def extract_school_district_name(self, pdf_stream: io.BytesIO) -> str:
        """Use regex to extract the school district name from the PDF."""
        reader = PdfReader(pdf_stream)

        # Collect all text from the PDF
        full_text = ""
        for page in reader.pages:
            full_text += page.extract_text() or ""

        # Search for the line containing "School District"
        if "\nSchool District" in full_text:
            match = re.search(r"School District\r?\n(.+)", full_text)
            if match is not None:
                match = match.group(1).strip()
        elif "to receive educational records from:" in full_text:
            match = re.search(r"First, Last\r?\n(.+)", full_text)
            if match is not None:
                match = match.group(1)[:-15].strip()
        else:
            match = re.search(r"Other\r?\n(.+)", full_text)
            if match is not None:
                match = match.group(1)[:-15].strip()
        if match:
            return match.lower()
        else:
            return "Not Found"

    def save_document_as_pdf(
        self, link_text: str, client: ClientFromDB
    ) -> tuple[io.BytesIO, str, str, dict]:
        """Helper function to find, print, and save a single document."""
        logger.info(f"Opening {link_text}...")

        # Default values
        stream = io.BytesIO()
        stream_name = ""
        school = "Not Found"
        drive_file = {}

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

            filename = f"{client.firstName} {client.lastName} {client.dob.strftime('%m%d%Y')} {doc_type}.pdf"

            logger.info(f"Saving {filename}...")
            stream, stream_name, school, drive_file = self.upload_pdf_from_driver(
                filename, self.config.records_folder_id
            )

        except TimeoutException:
            logger.error(f"Could not find or load document: {link_text}")
        finally:
            # Go back to the Docs & Forms list
            self.driver.back()

        return stream, stream_name, school, drive_file

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
    services, config = load_config()

    school_contacts = config.records_emails

    clients_to_process = get_clients_to_request(config)

    if clients_to_process is None or clients_to_process.empty:
        logger.critical("No clients found.")
        return

    logger.info(f"Found {len(clients_to_process)} new clients to process.")

    today = date.today()
    new_success_count = 0
    new_failure_count = 0

    prev_clients, _ = get_previous_clients(config)

    with TherapyAppointmentBot(services, config) as bot:
        bot.login()
        for _, client in clients_to_process.iterrows():
            client_id = client["Client ID"]
            asdAdhd = client["For"]
            client = prev_clients[int(client_id)]
            client_name = client.fullName

            if bot.go_to_client(client_id):
                try:
                    bot.check_if_opened_portal()
                    bot.check_if_docs_signed()
                    skipped = bot.download_consent_forms(client, school_contacts)
                    append_to_csv_file(Path(SUCCESS_FILE), client_name)
                    if not skipped:
                        new_success_count += 1

                except Exception as e:
                    logger.error(
                        f"An error occurred while processing {client_name}: {e}"
                    )
                    add_failure(
                        config=config,
                        client_id=client_id,
                        error=str(e),
                        failed_date=today,
                        full_name=client_name,
                        asd_adhd=asdAdhd,
                        daeval="Records",
                    )
                    new_failure_count += 1
            else:
                add_failure(
                    config=config,
                    client_id=client_id,
                    error="Client not found",
                    failed_date=today,
                    full_name=client_name,
                    asd_adhd=asdAdhd,
                    daeval="Records",
                )
                new_failure_count += 1

    logger.info(
        f"Downloads complete. Success: {new_success_count}, Failed: {new_failure_count}\n\n{new_success_count} email(s) sent."
    )


if __name__ == "__main__":
    main()
