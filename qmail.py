import base64
import logging
import os.path
from datetime import datetime
from email.message import EmailMessage
from email.mime.text import MIMEText

from dateutil.relativedelta import relativedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import shared_utils as utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("qmail.log"), logging.StreamHandler()],
)

services, config = utils.load_config()

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.compose"]


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


def send_gmail(message_text, subject, to_addr, from_addr):
    """Create and insert a draft email.
      Print the returned draft's message and id.
      Returns: Draft object, including draft id and message meta data.

    Load pre-authorized user credentials from the environment.
    """
    creds = google_authenticate()

    try:
        service = build("gmail", "v1", credentials=creds)

        message = EmailMessage()
        message.set_content(message_text)
        message["Subject"] = subject
        message["To"] = to_addr
        message["From"] = from_addr

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {"raw": encoded_message}

        send_message = (
            service.users().messages().send(userId="me", body=create_message).execute()
        )

        logging.info(f"Sent email to {to_addr}: {subject}")

    except HttpError as error:
        print(f"An error occurred: {error}")
        send_message = None
    return send_message


def get_tomorrow_clients():
    clients = utils.get_previous_clients()
    tomorrow_clients = {}
    if clients:
        for id in clients:
            client = clients[id]
            if (
                client.get("date")
                and datetime.strptime(client["date"], "%Y/%m/%d").date()
                == (datetime.now() + relativedelta(days=1)).date()
            ):
                tomorrow_clients[id] = client
    return tomorrow_clients


def generate_evaluator_email(evaluator_address):
    sorted_clients = sorted(evaluator_address, key=lambda k: k["lastname"])
    email_text = f"Here is the status of the questionnaires for tomorrow, {(datetime.now() + relativedelta(days=1)).strftime('%m/%d')}:\n\n"
    for client in sorted_clients:
        if client.get("asana") and client["asana"]:
            asana_link = f"https://app.asana.com/1/{services['asana']['workspace']}/project/{client['asana']}/overview"

        sent_date_str = (
            f" [sent on {datetime.strptime(client['sent_date'], '%Y/%m/%d').strftime('%m/%d')}]"
            if client.get("sent_date")
            else ""
        )

        email_text += f"{client['firstname']} {client['lastname']} ({asana_link}){sent_date_str}: \n"
        for questionnaire in client["questionnaires"]:
            email_text += f"  - {questionnaire['type']} - {'Done' if questionnaire['done'] else 'NOT DONE'}{f' - {questionnaire["link"]}' if not questionnaire['done'] else ''}\n"
        email_text += "\n"
    send_gmail(
        email_text,
        f"Questionnaires for {(datetime.now() + relativedelta(days=1)).strftime('%m/%d')}",
        evaluator_address[0]["evaluator_email"],
        config["email"],
    )


def generate_ipad_email(client):
    email_text = ""
    for questionnaire in client["questionnaires"]:
        if not questionnaire["done"]:
            email_text += f"- {questionnaire['type']} - {questionnaire['link']}\n"
    return email_text


def main():
    projects_api = utils.init_asana(services)
    driver, actions = utils.initialize_selenium()

    tomorrow_clients = get_tomorrow_clients()
    if tomorrow_clients:
        utils.check_questionnaires(driver, config, services, tomorrow_clients)

    tomorrow_clients = get_tomorrow_clients()
    clients_by_evaluator = {}
    if tomorrow_clients:
        for id in tomorrow_clients:
            client = tomorrow_clients[id]

            evaluator_email = client.get("evaluator_email")
            if evaluator_email:
                if evaluator_email not in clients_by_evaluator:
                    clients_by_evaluator[evaluator_email] = []
                clients_by_evaluator[evaluator_email].append(client)

            utils.mark_links_in_asana(projects_api, client, services, config)

            # TODO: send ipad emails?
            # if not utils.all_questionnaires_done(client):
            # print(generate_ipad_email(client))

    for evaluator in clients_by_evaluator:
        generate_evaluator_email(clients_by_evaluator[evaluator])


if __name__ == "__main__":
    main()
