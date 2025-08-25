from base64 import b64decode
from pathlib import Path

import yaml
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

SOURCE_FILE = "records.txt"
SUCCESS_FILE = "savedrecords.txt"
FAILURE_FILE = "recordfailures.txt"
OUTPUT_DIR = Path("School Records Requests")
WAIT_TIMEOUT = 15  # seconds


def load_previous_csv(filepath: Path) -> set:
    """Reads a comma-separated file and returns a set of its items."""
    if not filepath.exists():
        return set()
    with open(filepath, "r") as file:
        content = file.read().strip()
        if not content:
            return set()
        # Split by comma and space, and strip extra whitespace from each item
        return {item.strip() for item in content.split(",") if item.strip()}


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

    def __init__(self, services: utils.Services):
        """Initializes the TherapyAppointmentBot."""
        self.config = services["therapyappointment"]
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
        username_field.send_keys(self.config["username"])

        password_field = self.driver.find_element(By.NAME, "user_password")
        password_field.send_keys(self.config["password"])
        password_field.submit()
        logger.success("Login successful.")

    def go_to_client(self, first_name: str, last_name: str) -> bool:
        """Navigates to a specific client in TherapyAppointment."""
        logger.info(f"Searching for client: {first_name} {last_name}...")
        try:
            # Wait for the main navigation to be clickable
            clients_button = self.wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//*[contains(text(), 'Clients')]")
                )
            )
            clients_button.click()

            # Wait for the search form to be ready
            firstname_field = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//label[text()='First Name']/following-sibling::input")
                )
            )
            firstname_field.send_keys(first_name)

            lastname_field = self.driver.find_element(
                By.XPATH, "//label[text()='Last Name']/following-sibling::input"
            )
            lastname_field.send_keys(last_name)

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
            logger.warning(
                f"Client not found or search failed for: {first_name} {last_name}"
            )
            return False
        except NoSuchElementException:
            logger.warning(
                f"Could not find a search element for: {first_name} {last_name}"
            )
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

    def download_consent_forms(self, client_data: dict):
        """Navigates to Docs & Forms and saves consent forms as PDFs."""
        logger.info("Navigating to Docs & Forms...")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        self._save_document_as_pdf(
            "Receiving Consent to Release of Information", client_data
        )
        self._save_document_as_pdf(
            "Sending Consent to Release of Information", client_data
        )

        clients_button = self.driver.find_element(
            By.XPATH, "//*[contains(text(), 'Clients')]"
        )
        clients_button.click()

    def _save_document_as_pdf(self, link_text: str, client: dict):
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
                OUTPUT_DIR
                / f"{client['fullname']} {client['birthdate']} {doc_type}.pdf"
            )

            logger.info(f"Saving {filename}...")
            pdf_options = PrintOptions()
            pdf_options.orientation = "portrait"

            pdf_base64 = self.driver.print_page(pdf_options)
            with open(filename, "wb") as file:
                file.write(b64decode(pdf_base64))

            if filename.exists():
                logger.success(f"Saved {filename}")

        except TimeoutException:
            logger.error(f"Could not find or load document: {link_text}")
        finally:
            # Go back to the Docs & Forms list
            self.driver.back()


def main():
    """Main function to run the automation script."""
    services, config = utils.load_config()

    successful_clients = load_previous_csv(Path(SUCCESS_FILE))
    failed_clients = load_previous_csv(Path(FAILURE_FILE))
    already_processed = successful_clients.union(failed_clients)
    logger.info(f"Loaded {len(already_processed)} previously processed clients.")

    try:
        with open(SOURCE_FILE, "r") as file:
            content = file.read().strip()
            clients_to_process = {
                item.strip() for item in content.split(",") if item.strip()
            }
    except FileNotFoundError:
        logger.error(f"Source file not found: {SOURCE_FILE}")
        return

    new_clients = clients_to_process - already_processed
    if not new_clients:
        logger.info("No new clients to process.")
        return

    logger.info(f"Found {len(new_clients)} new clients to process.")

    new_success_count = 0
    new_failure_count = 0

    with TherapyAppointmentBot(services) as bot:
        bot.login()
        for client_name in new_clients:
            try:
                first, last = client_name.split()
            except ValueError:
                logger.warning(f"Skipping malformed name: '{client_name}'")
                append_to_csv_file(Path(FAILURE_FILE), client_name)
                new_failure_count += 1
                continue

            if bot.go_to_client(first, last):
                try:
                    client_data = bot.extract_client_data()
                    bot.download_consent_forms(client_data)
                    append_to_csv_file(Path(SUCCESS_FILE), client_name)
                    new_success_count += 1
                except Exception as e:
                    logger.error(
                        f"An error occurred while processing {client_name}: {e}"
                    )
                    append_to_csv_file(Path(FAILURE_FILE), client_name)
                    new_failure_count += 1
            else:
                append_to_csv_file(Path(FAILURE_FILE), client_name)
                new_failure_count += 1

    with open(SOURCE_FILE, "w") as f:
        f.truncate(0)

    logger.info(
        f"Process complete. Success: {new_success_count}, Failed: {new_failure_count}"
    )


if __name__ == "__main__":
    main()
