import io
import re
import sys
from base64 import b64decode
from datetime import date
from pathlib import Path

import pymupdf
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from loguru import logger
from selenium.webdriver.common.by import By
from selenium.webdriver.common.print_page_options import PrintOptions
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.custom_types import ClientFromDB, Config, RecordsContact
from utils.database import (
    get_clients_needing_records,
    update_external_record_in_db,
)
from utils.google import (
    google_authenticate,
    move_file_in_drive,
    send_gmail,
)
from utils.misc import NetworkSink, add_failure, load_config, load_local_settings
from utils.selenium import (
    check_and_login_ta,
    check_if_docs_signed,
    check_if_opened_portal,
    go_to_client,
    initialize_selenium,
)

logger.remove()
logger.add(
    sys.stdout,
    format="[<dim>{time:YY-MM-DD HH:mm:ss}</dim>] <level>{level: <8}</level> | <level>{message}</level>",
)

logger.add("logs/records-request.log", rotation="500 MB")

log_host = load_local_settings().log_host

network_sink = NetworkSink(log_host, 9999, app_name="records-request")

logger.add(
    network_sink.write,
    format="{time} | {level: <8} | {message}",
    enqueue=True,
)

WAIT_TIMEOUT = 15  # seconds


def normalize_district(name: str | None) -> str:
    if not name:
        return ""

    pattern = r"\b(county school district|school district|county)\b"

    clean = re.sub(rf"(?i){pattern}", "", name)

    return " ".join(clean.split()).lower()


def append_to_csv_file(filepath: Path, data: str):
    """Appends data to a comma-separated file, handling separators correctly."""
    prefix = ""
    # Add a separator only if the file already exists and is not empty
    if filepath.exists() and filepath.stat().st_size > 0:
        prefix = ", "

    with open(filepath, "a") as f:
        f.write(f"{prefix}{data}")


def resolve_school_contact(
    name: str, school_contacts: dict[str, RecordsContact]
) -> tuple[str, RecordsContact] | tuple[None, None]:
    """Helper to find a contact by name or alias."""
    name = name.lower().strip()
    if name in school_contacts:
        return name, school_contacts[name]
    for canonical_name, contact in school_contacts.items():
        if name in [a.lower().strip() for a in contact.aliases]:
            return canonical_name, contact
    return None, None


def download_consent_forms(
    driver: WebDriver,
    client: ClientFromDB,
    school_contacts: dict[str, RecordsContact],
    config: Config,
) -> bool:
    """Navigates to Docs & Forms and saves consent forms as PDFs."""
    creds = google_authenticate()

    service = build("drive", "v3", credentials=creds)

    logger.debug("Checking if files already exist...")
    check_filename = f"{client.firstName} {client.lastName} {client.dob.strftime('%m%d%Y')} Receiving.pdf"
    prev_receive = file_exists(service, check_filename, config.records_folder_id)
    check_filename = f"{client.firstName} {client.lastName} {client.dob.strftime('%m%d%Y')} Sending.pdf"
    prev_send = file_exists(service, check_filename, config.records_folder_id)

    if prev_receive or prev_send:
        logger.warning(
            f"Files already exist for {client.firstName} {client.lastName}, skipping download."
        )
        return True

    logger.info("Navigating to Docs & Forms...")

    receiving_stream, receiving_filename, receiving_school, receiving_drive_file = (
        save_document_as_pdf(
            driver, "Receiving Consent to Release of Information", client, config
        )
    )
    sending_stream, sending_filename, sending_school, sending_drive_file = (
        save_document_as_pdf(
            driver, "Sending Consent to Release of Information", client, config
        )
    )

    sending_school = sending_school.lower().strip()
    receiving_school = receiving_school.lower().strip()

    if "your relationship to client" in sending_school:
        raise (Exception("No school found on consent to send"))

    if "your relationship to client" in receiving_school:
        raise (Exception("No school found on consent to receive"))

    if sending_school != receiving_school:
        logger.warning(
            f"School on Sending, {sending_school}, is not the same as school on Receiving, {receiving_school}"
        )
        raise (Exception("District on receive does not match district on send"))

    canonical_sending, school_contact = resolve_school_contact(
        sending_school, school_contacts
    )

    if not school_contact:
        raise (
            Exception(f"School found, {sending_school}, has no email address assigned.")
        )

    if client.schoolDistrict is None:
        raise (
            Exception(
                "Client has no school district in DB, cannot verify if they are the same."
            )
        )

    db_district = normalize_district(client.schoolDistrict)

    if normalize_district(canonical_sending) != db_district:
        raise (
            Exception(
                f"School district on consent form does not match client's school district in DB, form is {sending_school}, DB is {db_district}."
            )
        )

    message_text = f"Re: Student: {client.firstName} {client.lastName}\nDate of Birth: {client.dob.strftime('%m/%d/%Y')}\n\nPlease find Consent to Release of Information attached for the above referenced student. Please send the most recent IEP, any Evaluation Reports, and any Reevaluation Review information.\n\nIf the child is currently undergoing evaluation, please provide the date of the Consent for Evaluation.\n\nThank you for your time!"

    attachments = [
        {"stream": receiving_stream, "filename": receiving_filename},
        {"stream": sending_stream, "filename": sending_filename},
    ]

    if school_contact.fax:
        logger.info("Fax number found, creating and prepending cover sheet...")
        fax_cover_stream, fax_cover_filename = create_fax_cover_sheet(client)
        if fax_cover_stream and fax_cover_filename:
            attachments.insert(
                0,
                {"stream": fax_cover_stream, "filename": fax_cover_filename},
            )

    send_gmail(
        message_text=message_text,
        subject=f"Re: Student: {client.firstName} {client.lastName}",
        to_addr=school_contact.email,
        from_addr="records@driftwoodeval.com",
        attachments=attachments,
    )

    try:
        move_file_in_drive(
            service,
            receiving_drive_file["id"],
            config.sent_records_folder_id,
        )
        move_file_in_drive(
            service,
            sending_drive_file["id"],
            config.sent_records_folder_id,
        )
    except Exception:
        logger.exception("Error moving files to sent folder")
        return False

    update_external_record_in_db(
        config,
        client.id,
        date.today(),
        is_second_request=bool(client.requested),
    )

    return False


def upload_pdf_from_driver(
    driver: WebDriver, filename: str, folder_id: str
) -> tuple[io.BytesIO, str, str, dict]:
    """Prints page as PDF (in memory) and uploads to Drive."""
    pdf_options = PrintOptions()
    pdf_options.orientation = "portrait"

    pdf_base64 = driver.print_page(pdf_options)

    pdf_bytes = b64decode(pdf_base64)
    pdf_stream = io.BytesIO(pdf_bytes)

    school = extract_school_district_name(pdf_stream)

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


def extract_school_district_name(pdf_stream: io.BytesIO) -> str:
    """Use regex to extract the school district name from the PDF."""
    pdf_stream.seek(0)

    doc = pymupdf.open(stream=pdf_stream, filetype="pdf")

    full_text = ""
    for page in doc:
        text = page.get_text()
        if isinstance(text, str):
            full_text += text

    doc.close()

    # Reset stream position to 0 so it can be uploaded to Drive later
    pdf_stream.seek(0)

    match = None
    if "School District" in full_text:
        after_match = re.search(r"School District\r?\n(.+)", full_text)
        before_match = re.search(r"(.+)\r?\nSchool District", full_text)

        cand_after = after_match.group(1).strip() if after_match else None
        cand_before = before_match.group(1).strip() if before_match else None

        if cand_after and "self" not in cand_after.lower():
            match = cand_after
        elif cand_before:
            match = cand_before
        elif cand_after:
            match = cand_after

    if match:
        return normalize_district(match)
    else:
        return "Not Found"


def save_document_as_pdf(
    driver: WebDriver, link_text: str, client: ClientFromDB, config: Config
) -> tuple[io.BytesIO, str, str, dict]:
    """Helper function to find, print, and save a single document."""
    logger.info(f"Opening {link_text}...")
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    # Default values
    stream = io.BytesIO()
    stream_name = ""
    school = "Not Found"
    drive_file = {}

    try:
        docs_button = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Docs & Forms"))
        )
        docs_button.click()

        document_link = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, link_text))
        )
        document_link.click()

        wait.until(
            EC.visibility_of_element_located(
                (By.XPATH, "//*[contains(text(), 'I authorize')]")
            )
        )

        doc_type = link_text.split(" ")[0]

        filename = f"{client.firstName} {client.lastName} {client.dob.strftime('%m%d%Y')} {doc_type}.pdf"

        logger.info(f"Saving {filename}...")
        stream, stream_name, school, drive_file = upload_pdf_from_driver(
            driver, filename, config.records_folder_id
        )

    except Exception:
        logger.error(f"Could not find or load document: {link_text}")
        raise
    finally:
        # Go back to the Docs & Forms list
        driver.back()

    return stream, stream_name, school, drive_file


def file_exists(service, filename, folder_id):
    """Helper function to check if a file exists in Drive."""
    query = f"name = '{filename.replace("'", "\\'")}' and '{folder_id}' in parents and trashed = false"
    results = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name)", pageSize=1)
        .execute()
    )

    files = results.get("files", [])
    if files:
        return files[0]["id"]  # return the ID of the first matching file
    return None


def create_fax_cover_sheet(
    client: ClientFromDB,
) -> tuple[io.BytesIO, str] | tuple[None, None]:
    """Create a fax cover sheet from a template, appending text to labels."""
    try:
        doc = pymupdf.open("templates/Fax Records.pdf")
        page = doc[0]

        def append_text(search_term, text_to_append):
            text_instances = page.search_for(search_term)
            for inst in text_instances:
                page.insert_text(
                    (inst.x1, inst.y0 + 10),
                    f" {text_to_append}",
                    fontsize=11,
                    fontname="helv",
                )

        append_text("Student-", client.fullName)

        append_text("Date of Birth-", client.dob.strftime("%m/%d/%Y"))

        pdf_stream = io.BytesIO(doc.tobytes())
        doc.close()
        pdf_stream.seek(0)

        filename = f"Fax Cover for {client.fullName}.pdf"
        return pdf_stream, filename

    except Exception as e:
        logger.error(f"Error creating fax cover sheet: {e}")
        return None, None


def main():
    """Main function to run the automation script."""
    services, config = load_config()

    school_contacts = config.records_emails
    school_contacts = {k.lower(): v for k, v in school_contacts.items()}

    clients_to_process = get_clients_needing_records(config)

    if not clients_to_process:
        logger.critical("No clients found.")
        return

    logger.info(f"Found {len(clients_to_process)} new clients to process.")

    today = date.today()
    new_success_count = 0
    new_failure_count = 0

    driver, actions = initialize_selenium()
    driver.maximize_window()

    try:
        check_and_login_ta(driver, actions, services, first_time=True)
        for client in clients_to_process:
            asdAdhd = client.asdAdhd or "Unknown"
            client_name = client.fullName

            if go_to_client(driver, actions, services, str(client.id)):
                try:
                    if not check_if_opened_portal(driver):
                        raise Exception("portal not opened")
                    if not check_if_docs_signed(driver):
                        raise Exception("docs not signed")

                    skipped = download_consent_forms(
                        driver, client, school_contacts, config
                    )
                    if not skipped:
                        new_success_count += 1

                except Exception as e:
                    logger.error(
                        f"An error occurred while processing {client_name}: {e}"
                    )
                    add_failure(
                        config=config,
                        client_id=client.id,
                        error=str(e),
                        failed_date=today,
                        add_to_sheet=True,
                        full_name=client_name,
                        asd_adhd=asdAdhd,
                        daeval="Records",
                    )
                    new_failure_count += 1
            else:
                add_failure(
                    config=config,
                    client_id=client.id,
                    error="unable to find client",
                    failed_date=today,
                    full_name=client_name,
                    asd_adhd=asdAdhd,
                    daeval="Records",
                )
                new_failure_count += 1
    finally:
        logger.debug("Closing WebDriver.")
        driver.quit()

    logger.info(
        f"Downloads complete. Success: {new_success_count}, Failed: {new_failure_count}\n\n{new_success_count} email(s) sent."
    )


if __name__ == "__main__":
    main()
