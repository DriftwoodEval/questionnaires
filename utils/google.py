import base64
import mimetypes
import re
import time
from collections.abc import Sequence
from datetime import date
from email.message import EmailMessage
from functools import cache
from pathlib import Path
from typing import Any

import pandas as pd
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from loguru import logger

from utils.custom_types import (
    AdminEmailInfo,
    ClientWithQuestionnaires,
    Config,
)
from utils.database import get_most_recent_failure
from utils.questionnaires import get_most_recent_not_done

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


@cache
def google_authenticate():
    """Authenticate with Google using the credentials in ./config/credentials.json (obtained from Google Cloud Console) and ./config/token.json (user-specific).

    If the credentials are not valid, the user is prompted to log in.
    The credentials are then saved to ./config/token.json for the next run.
    Returns the authenticated credentials.
    """
    creds = None
    token_path = Path("config/token.json")
    if Path.exists(token_path):
        creds = Credentials.from_authorized_user_file("./config/token.json", SCOPES)
        if creds and set(creds.scopes or []) != set(SCOPES):
            logger.info("Scopes have changed, re-authenticating...")
            creds = None

    # If the credentials are invalid or have expired, refresh the credentials
    if not creds or not creds.valid:
        refreshed = False
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                refreshed = True
            except RefreshError:
                logger.warning("Token refresh failed, falling back to manual login")
        # If there are no usable credentials, start the manual login
        if not refreshed:
            flow = InstalledAppFlow.from_client_secrets_file(
                "./config/credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

    # Save the credentials for the next run
    with Path.open(token_path, "w") as token:
        token.write(creds.to_json())

    return creds


def send_gmail(
    message_text: str,
    subject: str,
    to_addr: str,
    from_addr: str,
    *,
    cc_addr: str | None = None,
    html: str | None = None,
    attachments: list[dict[str, Any]] | None = None,
):
    """Send an email using the Gmail API.

    Parameters:
        message_text (str): The text of the message
        subject (str): The subject of the message
        to_addr (str): The recipient's email address, can be a comma-separated list
        from_addr (str): The sender's email address
        cc_addr (Optional[str]): The CC recipient's email address, can be a comma-separated list (optional)
        html (Optional[str]): The HTML version of the message (optional)
        attachments (Optional[list[dict]]): A list of attachments, where each attachment is a dict with "stream" and "filename" keys (optional)
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

        if attachments:
            for attachment in attachments:
                if attachment.get("stream") and attachment.get("filename"):
                    pdf_bytes = attachment["stream"].getvalue()
                    message.add_attachment(
                        pdf_bytes,
                        maintype="application",
                        subtype="pdf",
                        filename=attachment["filename"],
                    )

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {"raw": encoded_message}

        send_message = (
            service.users().messages().send(userId="me", body=create_message).execute()
        )

        logger.info(f"Sent email to {to_addr}: {subject}")

    except Exception:
        logger.exception("Failed to send email")
        send_message = None
    return send_message


def _get_message_body(payload: dict) -> tuple[str | None, str | None]:
    """Walk a Gmail message payload's MIME parts, returning (text_plain, text_html)."""
    text_plain = None
    text_html = None

    def decode(data: str) -> str:
        return base64.urlsafe_b64decode(data.encode()).decode("utf-8", errors="replace")

    def walk(part: dict) -> None:
        nonlocal text_plain, text_html
        mime_type = part.get("mimeType", "")
        body_data = part.get("body", {}).get("data")
        if mime_type == "text/plain" and body_data and text_plain is None:
            text_plain = decode(body_data)
        elif mime_type == "text/html" and body_data and text_html is None:
            text_html = decode(body_data)
        for subpart in part.get("parts", []):
            walk(subpart)

    walk(payload)
    return text_plain, text_html


def list_gmail_messages(
    query: str | None = None,
    label_ids: Sequence[str] | None = None,
    max_results: int = 50,
) -> list[dict]:
    """List Gmail message id stubs matching a Gmail search query.

    Paginates until max_results is reached. Returns the raw {"id", "threadId"}
    stubs; use get_gmail_message() to fetch full content.
    """
    creds = google_authenticate()
    service = build("gmail", "v1", credentials=creds)
    messages: list[dict] = []
    page_token = None
    while len(messages) < max_results:
        response = (
            service.users()
            .messages()
            .list(
                userId="me",
                q=query,
                labelIds=label_ids,
                pageToken=page_token,
                maxResults=min(max_results - len(messages), 500),
            )
            .execute()
        )
        messages.extend(response.get("messages", []))
        page_token = response.get("nextPageToken")
        if page_token is None:
            break
    return messages[:max_results]


def get_gmail_message(message_id: str) -> dict:
    """Fetch and parse a single Gmail message by id.

    Returns a dict with subject, from, to, date, snippet, body_text, body_html.
    """
    creds = google_authenticate()
    service = build("gmail", "v1", credentials=creds)
    message = (
        service.users()
        .messages()
        .get(userId="me", id=message_id, format="full")
        .execute()
    )

    payload = message.get("payload", {})
    headers = {h["name"].lower(): h["value"] for h in payload.get("headers", [])}
    body_text, body_html = _get_message_body(payload)

    return {
        "id": message["id"],
        "thread_id": message.get("threadId"),
        "subject": headers.get("subject"),
        "from": headers.get("from"),
        "to": headers.get("to"),
        "date": headers.get("date"),
        "snippet": message.get("snippet"),
        "body_text": body_text,
        "body_html": body_html,
    }


def mark_gmail_message_read(message_id: str) -> None:
    """Remove the UNREAD label from a Gmail message."""
    creds = google_authenticate()
    service = build("gmail", "v1", credentials=creds)
    service.users().messages().modify(
        userId="me", id=message_id, body={"removeLabelIds": ["UNREAD"]}
    ).execute()


PEARSON_VERIFICATION_QUERY = (
    'to:ratingscales@driftwoodeval.com subject:"Pearson Verification Code Requested"'
)


def _parse_pearson_code(message: dict) -> str | None:
    text = message["body_text"] or message["snippet"] or ""
    match = re.search(r"verification code is:?\s*(\d+)", text, re.IGNORECASE)
    if not match:
        match = re.search(r"\b\d{6}\b", text)
    if not match:
        return None
    return match.group(1) if match.re.groups else match.group(0)


def get_pearson_verification_code(
    after_epoch: int | None = None,
    max_wait: int = 90,
    poll_interval: int = 5,
) -> str | None:
    """Fetch the Pearson (QGlobal) 2FA verification code from Gmail.

    Mirrors the logic in ../winnonah: find the message sent to
    ratingscales@driftwoodeval.com with the "Pearson Verification Code
    Requested" subject, mark it read, and pull the numeric code out of the body.

    Pearson emails the code a few seconds after the login form asks for it, so
    when after_epoch is given this polls (up to max_wait seconds) for a message
    newer than that timestamp instead of returning a stale code.
    """
    query = PEARSON_VERIFICATION_QUERY
    if after_epoch is not None:
        query = f"{query} after:{after_epoch}"

    deadline = time.monotonic() + max_wait
    while True:
        messages = list_gmail_messages(query=query, max_results=1)
        if messages:
            message = get_gmail_message(messages[0]["id"])
            mark_gmail_message_read(message["id"])
            code = _parse_pearson_code(message)
            if code:
                logger.info(f"Pearson verification code: {code}")
                return code
            logger.error("Could not parse verification code from Pearson email.")
            return None
        if after_epoch is None or time.monotonic() >= deadline:
            logger.error("No Pearson verification code email found.")
            return None
        time.sleep(poll_interval)


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
        call_clients_data = []
        for client in email_info["call"]:
            most_recent = (
                get_most_recent_not_done(client)
                if isinstance(client, ClientWithQuestionnaires)
                else get_most_recent_failure(client)
            )
            if most_recent:
                call_clients_data.append((client, most_recent))

        def _post_eval_note(client: Any, most_recent: Any) -> str:
            if (
                isinstance(client, ClientWithQuestionnaires)
                and most_recent["status"] == "POSTEVAL_PENDING"
            ):
                return ", post-eval"
            return ""

        email_text += (
            "Call:\n"
            + "\n".join(
                [
                    f"- {client.fullName} (sent on {(most_recent['sent'] and most_recent['sent'].strftime('%m/%d')) or 'unknown date'}, reminded {str(most_recent['reminded']) + ' times' if most_recent else 'unknown number of times'}{_post_eval_note(client, most_recent)})"
                    if isinstance(client, ClientWithQuestionnaires)
                    else f"- {client.fullName} ({most_recent['reason'].capitalize()} on {most_recent['failedDate'].strftime('%m/%d')}, reminded {str(most_recent['reminded']) + ' times'})"
                    for client, most_recent in call_clients_data
                ]
            )
            + "\n"
        )
        email_html += (
            "<h2>Call</h2><ul><li>"
            + "</li><li>".join(
                f"{client.fullName} (sent on {(most_recent['sent'] and most_recent['sent'].strftime('%m/%d')) or 'unknown date'}, reminded {str(most_recent['reminded']) + ' times' if most_recent else 'unknown number of times'}{_post_eval_note(client, most_recent)})"
                if isinstance(client, ClientWithQuestionnaires)
                else f"{client.fullName} ({most_recent['reason'].capitalize()} on {most_recent['failedDate'].strftime('%m/%d')}, reminded {str(most_recent['reminded']) + ' times'})"
                for client, most_recent in call_clients_data
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
                    "DA Qs Needed",
                    "DA Qs Sent",
                    "EVAL Qs Needed",
                    "EVAL Qs Sent",
                    "Evaluator",
                    "Assigned to OR added to report writing folder",
                    "Billed?",
                    "AJP Review Done/Hold for payroll",
                    "MCS Review Needed",
                ]
            ]

            # Drop any rows where the "Client ID" column is empty
            df = df[df["Client ID"].notna() & df["Client ID"].str.len().astype(bool)]

            # The may have IDs as "C" + zero-padded 9 digits (e.g. C000012345);
            # strip that down to the bare numeric ID used everywhere else in the codebase.
            df["Client ID"] = df["Client ID"].apply(
                lambda client_id: re.sub(r"^C?0*", "", client_id)
            )

            # Rebuild the "Human Friendly ID" — this is how TherapyAppointment displays
            # client IDs, and other platforms (qglobal, mhs, wps) are searched using it.
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
        remainder = col_index % 26
        column_letter = chr(ord("A") + remainder) + column_letter

        # Spreadsheet columns are base-26 with no zero digit (A, B, ... Z, AA, ...),
        # so the extra -1 corrects for the standard divmod carry assuming a zero digit.
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

        row_number = None
        for i, row in enumerate(values):
            if row and row[1] == id_for_search:
                row_number = i + 1  # Spreadsheets are 1-indexed
                break

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


def batch_update_punch_list(
    config: Config,
    updates: list[tuple[str, str, str]],
):
    """Update multiple cells in the Punch List in a single API call.

    Args:
        config: App config.
        updates: List of (id_for_search, column_header, new_value) tuples.
    """
    if not updates:
        return

    creds = google_authenticate()
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    result = (
        sheet.values()
        .get(spreadsheetId=config.punch_list_id, range=config.punch_list_range)
        .execute()
    )
    values = result.get("values", [])
    if not values:
        return

    headers = values[0]
    sheet_name = config.punch_list_range.split("!")[0]

    row_map = {row[1]: i + 1 for i, row in enumerate(values) if len(row) > 1}
    col_map = {header: col_index_to_a1(i) for i, header in enumerate(headers)}

    data = []
    for id_for_search, header, new_value in updates:
        row_number = row_map.get(id_for_search)
        col_letter = col_map.get(header)
        if row_number is not None and col_letter is not None:
            data.append(
                {
                    "range": f"{sheet_name}!{col_letter}{row_number}",
                    "values": [[new_value]],
                }
            )
        else:
            logger.debug(
                f"batch_update_punch_list: {id_for_search!r} / {header!r} not found, skipping"
            )

    if data:
        (
            sheet.values()
            .batchUpdate(
                spreadsheetId=config.punch_list_id,
                body={"valueInputOption": "USER_ENTERED", "data": data},
            )
            .execute()
        )
        logger.success(f"Batch updated {len(data)} cells in Punch List")


def add_to_failure_sheet(
    config: Config,
    client_id: int,
    error: str,
    failed_date: date,
    full_name: str,
    *,
    asd_adhd: str | None = None,
    daeval: str | None = None,
    questionnaires_needed: list[str] | None = None,
    questionnaires_generated: list[dict[str, str]] | None = None,
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
    """Finds an existing folder or creates a new one inside the parent folder and returns its ID and webViewLink."""
    try:
        query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        response = (
            service.files()
            .list(q=query, spaces="drive", fields="files(id, webViewLink)")
            .execute()
        )
        files = response.get("files", [])

        if files:
            return files[0]["id"], files[0].get("webViewLink")

        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        folder = (
            service.files()
            .create(body=file_metadata, fields="id, webViewLink")
            .execute()
        )
        logger.success(f"Created new Drive folder '{folder_name}'.")
        return folder.get("id"), folder.get("webViewLink")

    except Exception:
        logger.exception("An unexpected error occurred in Drive folder search/creation")
        return None, None


def upload_file_to_drive(
    file_path: Path, base_folder_id: str, subfolder: str | None = None
) -> tuple[str | None, str | None]:
    """Uploads a file to Google Drive in the specified folder, returning the file's and folder's web view links."""

    def _get_filetype(filepath: Path) -> str:
        try:
            mimetype, _ = mimetypes.guess_type(filepath)
            return mimetype or "application/octet-stream"
        except FileNotFoundError:
            return "File not found"
        except Exception as e:
            return f"Error: {e}"

    creds = google_authenticate()

    try:
        service = build("drive", "v3", credentials=creds)
    except Exception:
        logger.exception("Skipping Drive upload: Could not build Drive service")
        return None, None

    target_folder_id = base_folder_id
    folder_link = None
    if subfolder:
        subfolder_id, subfolder_link = find_or_create_drive_folder(
            service, base_folder_id, subfolder
        )
        if subfolder_id:
            target_folder_id = subfolder_id
            folder_link = subfolder_link
        else:
            logger.warning(
                f"Failed to create/find Drive subfolder for '{subfolder}'. Uploading to base folder."
            )

    file_metadata = {"name": file_path.name, "parents": [target_folder_id]}
    media = MediaFileUpload(file_path, mimetype=_get_filetype(file_path))

    try:
        file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id,webViewLink")
            .execute()
        )
        file_link = file.get("webViewLink")
        logger.success(f"File uploaded successfully to Drive: {file_link}")
        return file_link, folder_link
    except Exception:
        logger.exception("An unexpected error occurred during Drive upload.")
        return None, None


def move_file_in_drive(service, file_id: str, dest_folder_id: str):
    """Move a file from one folder to another in Google Drive."""
    file = service.files().get(fileId=file_id, fields="parents").execute()
    previous_parents = ",".join(file.get("parents"))

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
