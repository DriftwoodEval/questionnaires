import base64
import io
import os
import re
from datetime import date
from email.message import EmailMessage
from pathlib import Path
from typing import Literal, Optional

import magic
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from loguru import logger

from utils.custom_types import (
    AdminEmailInfo,
    ClientWithQuestionnaires,
    Config,
)
from utils.questionnaires import get_most_recent_not_done

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

    except HttpError:
        logger.exception("Failed to send email")
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

    if email_info["errors"]:
        email_text += (
            "Errors:\n"
            + "\n".join([f"- {error}" for error in email_info["errors"]])
            + "\n"
        )
        email_html += (
            "<h2>Errors</h2><ul><li>"
            + "</li><li>".join(error for error in email_info["errors"])
            + "</li></ul>"
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
    if email_info["ignoring"]:
        email_text += (
            "Check on ignoring:\n"
            + "\n".join([f"- {client.fullName}" for client in email_info["ignoring"]])
            + "\n"
        )
        email_html += (
            "<h2>Check on ignoring</h2><ul><li>"
            + "</li><li>".join(client.fullName for client in email_info["ignoring"])
            + "</li></ul>"
        )
    if email_info["failed"]:
        email_text += (
            "Failed to message:\n"
            + "\n".join(
                [f"- {item[0].fullName} ({item[1]})" for item in email_info["failed"]]
            )
            + "\n"
        )
        email_html += (
            "<h2>Failed to message</h2><ul><li>"
            + "</li><li>".join(
                f"{item[0].fullName} ({item[1]})" for item in email_info["failed"]
            )
            + "</li></ul>"
        )
    if email_info["ifsp_download_needed"]:
        email_text += (
            "IFSP download needed:\n"
            + "\n".join(
                [
                    f"- {client.fullName}"
                    for client in email_info["ifsp_download_needed"]
                ]
            )
            + "\n"
        )
        email_html += (
            "<h2>IFSP download needed</h2><ul><li>"
            + "</li><li>".join(
                client.fullName for client in email_info["ifsp_download_needed"]
            )
            + "</li></ul>"
        )
    if email_info["call"]:
        email_text += (
            "Call:\n"
            + "\n".join(
                [
                    f"- {client.fullName} (sent on {most_recent_q['sent'] and most_recent_q['sent'].strftime('%m/%d') or 'unknown date'}, reminded {str(most_recent_q['reminded']) + ' times' if most_recent_q else 'unknown number of times'})"
                    if isinstance(client, ClientWithQuestionnaires)
                    else f"- {client.fullName} ({client.failure['reason'].capitalize()} on {client.failure['failedDate'].strftime('%m/%d')}, reminded {str(client.failure['reminded']) + ' times'})"
                    for client in email_info["call"]
                    if (
                        most_recent_q := get_most_recent_not_done(client)
                        if isinstance(client, ClientWithQuestionnaires)
                        else None
                    )
                ]
            )
            + "\n"
        )
        email_html += (
            "<h2>Call</h2><ul><li>"
            + "</li><li>".join(
                f"{client.fullName} (sent on {most_recent_q['sent'] and most_recent_q['sent'].strftime('%m/%d') or 'unknown date'}, reminded {str(most_recent_q['reminded']) + ' times' if most_recent_q else 'unknown number of times'})"
                if isinstance(client, ClientWithQuestionnaires)
                else f"{client.fullName} ({client.failure['reason'].capitalize()} on {client.failure['failedDate'].strftime('%m/%d')}, reminded {str(client.failure['reminded']) + ' times'})"
                for client in email_info["call"]
                if (
                    most_recent_q := get_most_recent_not_done(client)
                    if isinstance(client, ClientWithQuestionnaires)
                    else None
                )
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
                    "DA Qs Needed",
                    "DA Qs Sent",
                    "EVAL Qs Needed",
                    "EVAL Qs Sent",
                    "Evaluator",
                    "Assigned to OR added to report writing folder",
                    "Billed?",
                    "AJP Review Done/Hold for payroll",
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
    except Exception:
        logger.exception("Failed to download punch list")


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
            logger.error(f"{id_for_search} not found in Punch List")
    except Exception:
        logger.exception("Failed to update Punch List")


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


def add_to_failure_sheet(
    config: Config,
    client_id: int,
    error: str,
    failed_date: date,
    full_name: str,
    asd_adhd: Optional[str] = None,
    daeval: Optional[str] = None,
    questionnaires_needed: Optional[list[str]] = None,
    questionnaires_generated: Optional[list[dict[str, str]]] = None,
):
    """Adds the given failed client to the failure sheet."""
    creds = google_authenticate()

    try:
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        body = {
            "values": [
                [
                    client_id,
                    asd_adhd,
                    daeval,
                    error,
                    str(failed_date),
                    full_name,
                    ", ".join(questionnaires_needed or []),
                ]
            ]
        }

        if questionnaires_generated:
            for q in questionnaires_generated:
                body["values"][0].extend([str(q.get("type")), str(q.get("link"))])

        sheet.values().append(
            spreadsheetId=config.failed_sheet_id,
            range="failures!A1:Z",
            body=body,
            valueInputOption="USER_ENTERED",
        ).execute()

    except Exception:
        logger.exception("Failed to add to failure sheet")


def find_or_create_drive_folder(service, parent_folder_id: str, folder_name: str):
    """Finds an existing folder or creates a new one inside the parent folder and returns its ID."""
    try:
        query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        response = (
            service.files().list(q=query, spaces="drive", fields="files(id)").execute()
        )
        files = response.get("files", [])

        if files:
            return files[0]["id"]

        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        folder = service.files().create(body=file_metadata, fields="id").execute()
        logger.success(f"Created new Drive folder '{folder_name}'.")
        return folder.get("id")

    except Exception:
        logger.exception("An unexpected error occurred in Drive folder search/creation")
        return None


def upload_file_to_drive(
    file_path: Path, base_folder_id: str, subfolder: Optional[str] = None
):
    """Uploads a file to Google Drive in the specified folder."""

    def _get_filetype_by_magic(filepath: Path) -> str:
        """Returns the MIME type by inspecting the file's header (magic number)."""
        try:
            return magic.from_file(filepath, mime=True)
        except FileNotFoundError:
            return "File not found"
        except Exception as e:
            return f"Error: {e}"

    creds = google_authenticate()

    try:
        service = build("drive", "v3", credentials=creds)
    except Exception:
        logger.exception("Skipping Drive upload: Could not build Drive service")
        return

    target_folder_id = base_folder_id
    if subfolder:
        subfolder_id = find_or_create_drive_folder(service, base_folder_id, subfolder)
        if subfolder_id:
            target_folder_id = subfolder_id
        else:
            logger.warning(
                f"Failed to create/find Drive subfolder for '{subfolder}'. Uploading to base folder."
            )

    file_metadata = {"name": file_path.name, "parents": [target_folder_id]}
    media = MediaFileUpload(file_path, mimetype=_get_filetype_by_magic(file_path))

    try:
        file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id,webViewLink")
            .execute()
        )
        logger.success(
            f"File uploaded successfully to Drive: {file.get('webViewLink')}"
        )
    except Exception:
        logger.exception("An unexpected error occurred during Drive upload.")


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
