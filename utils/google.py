import base64
import io
import os
import re
from datetime import date
from email.message import EmailMessage
from typing import Literal, Optional

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger

from utils.questionnaires import get_most_recent_not_done
from utils.types import (
    AdminEmailInfo,
    ClientWithQuestionnaires,
    Config,
)

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
    if email_info["call"]:
        email_text += (
            "Call:\n"
            + "\n".join(
                [
                    f"- {client.fullName} (sent on {most_recent_q['sent'].strftime('%m/%d') if most_recent_q else 'unknown date'}, reminded {str(most_recent_q['reminded']) + ' times' if most_recent_q else 'unknown number of times'})"
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
                f"{client.fullName} (sent on {most_recent_q['sent'].strftime('%m/%d') if most_recent_q else 'unknown date'}, reminded {str(most_recent_q['reminded']) + ' times' if most_recent_q else 'unknown number of times'})"
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


def add_to_failure_sheet(
    config: Config,
    client_id: int,
    error: str,
    failed_date: date,
    full_name: str,
    asd_adhd: Optional[str] = None,
    type: Optional[str] = None,
    questionnaires_needed: Optional[list[str]] = None,
    questionnaire_links_generated: Optional[list[dict[str, str]]] = None,
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
                    type,
                    error,
                    str(failed_date),
                    full_name,
                    ", ".join(questionnaires_needed or []),
                ]
            ]
        }

        if questionnaire_links_generated:
            for link in questionnaire_links_generated:
                body["values"][0].extend([str(link.get("type")), str(link.get("link"))])

        if type == "Records":
            sheet.values().append(
                spreadsheetId=config.failed_sheet_id,
                range="records!A1:Z",
                body=body,
                valueInputOption="USER_ENTERED",
            ).execute()
        else:
            sheet.values().append(
                spreadsheetId=config.failed_sheet_id,
                range="questionnaires!A1:Z",
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
