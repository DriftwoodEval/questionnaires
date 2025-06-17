import base64
import os.path
import re
from datetime import datetime, timezone
from email.message import EmailMessage

from dateutil import parser
from dateutil.relativedelta import relativedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger

import shared_utils as utils

logger.add("logs/qmail.log", rotation="500 MB")

services, config = utils.load_config()


def get_calendar_list() -> list[str]:
    creds = utils.google_authenticate()

    calendar_list = []
    try:
        service = build("calendar", "v3", credentials=creds)

        page_token = None
        while True:
            calendar_list_response = (
                service.calendarList().list(pageToken=page_token).execute()
            )
            for calendar_list_entry in calendar_list_response["items"]:
                if calendar_list_entry["id"] not in config["excluded_calendars"]:
                    calendar_list.append(calendar_list_entry["id"])
            page_token = calendar_list_response.get("nextPageToken")
            if not page_token:
                break

    except HttpError as error:
        logger.exception(error)
    return calendar_list


def filter_events(events):
    return [
        event
        for event in events
        if parser.isoparse(event["start"].get("dateTime", event["start"].get("date")))
        != parser.isoparse(event["end"].get("dateTime", event["start"].get("date")))
        and not any(
            event["summary"].lower().strip() == word
            for word in [
                "ap",
                "columbia",
                "focus time",
                "follow up",
                "goose creek",
                "in office",
                "in the office",
                "leave open",
                "melissa questions",
                "office",
                "shannon in myb",
            ]
        )
        and not any(
            word in event["summary"].lower().strip()
            for word in [
                "available",
                "conference",
                "interview with",
                "meeting with",
                "meet with",
                "office hours",
                "out of office",
                "placeholder",
                "plchdr",
                "plchldr",
                "shadowing",
                "team meeting",
                "training",
                "webinar",
            ]
        )
    ]


def get_appointment_type(string: str) -> str:
    regex1 = r"\[(.*?)-(D|E|DE)]"
    regex2 = r"\[V\]"

    match1 = re.search(regex1, string)
    match2 = re.search(regex2, string)

    ADHDregex = r"ADHD"
    ADHDmatch = re.search(ADHDregex, string)

    interpreterRegex = r"\*I\*"
    interpreterMatch = re.search(interpreterRegex, string)

    wInterpRegex = r"w/interp (\w+)"
    wInterpMatch = re.search(wInterpRegex, string)

    LDregex = r"\sLD\s"
    LDmatch = re.search(LDregex, string)

    typeString = "Unknown"

    prefixAdditions = []
    additions = []

    if ADHDmatch:
        prefixAdditions.append("ADHD")
    if LDmatch:
        prefixAdditions.append("LD")

    if match1:
        letter = match1.group(2)

        if letter == "D":
            typeString = "DA"
        elif letter == "E":
            typeString = "Evaluation"
        elif letter == "DE":
            typeString = "DA + Evaluation"
    elif match2:
        typeString = "DA"

    daEvalRegex = r"(^|\s)(DA|EVAL|DA\+EVAL)($|\s)"
    daEvalMatch = re.search(daEvalRegex, string, re.IGNORECASE)

    if daEvalMatch:
        daEval = daEvalMatch.group(2).upper()
        if daEval == "DA":
            typeString = "DA"
        elif daEval == "EVAL":
            typeString = "Evaluation"
        elif daEval == "DA+EVAL":
            typeString = "DA + Evaluation"

    if wInterpMatch:
        additions.append(f"w/interp {wInterpMatch.group(1)}")
    elif interpreterMatch:
        additions.append("w/interp")

    if prefixAdditions:
        typeString = " ".join(prefixAdditions) + " " + typeString

    if additions:
        typeString += " " + " ".join(additions)

    return typeString


def get_probable_name(event_title: str) -> str:
    # Check for '[' before any stripping
    bracket_index = event_title.find("[")
    if bracket_index != -1:
        event_title = event_title[:bracket_index].strip()

    # Now, perform the stripping and cleaning
    event_title = re.sub(r"\s{2,}", " ", event_title)
    event_title = re.sub(r"{[^}]+}", "", event_title)
    event_title = re.sub(r"\*.*?\*", "", event_title)
    event_title = event_title.strip()

    appointment_type = get_appointment_type(event_title)

    type_parts = appointment_type.split(" ")
    for i, part in enumerate(type_parts):
        if part == "Evaluation":
            type_parts[i] = "EVAL"

    earliest_index = len(event_title)

    for part in type_parts:
        index = event_title.find(part)
        if index != -1 and index < earliest_index:
            earliest_index = index

    name = event_title
    if earliest_index != len(event_title):
        name = event_title[:earliest_index].strip()

    name_parts = name.split(" ")
    if len(name_parts) >= 2:
        last_part = name_parts[-1].lower()
        suffixes = ["jr", "sr", "iii", "iv", "v"]

        if last_part in suffixes and len(name_parts) >= 3:
            return name_parts[0] + " " + name_parts[-2]  # Use the second to last word
        else:
            return name_parts[0] + " " + name_parts[-1]
    else:
        return name


def get_events_for_tomorrow() -> list:
    creds = utils.google_authenticate()
    events_tomorrow = []

    try:
        service = build("calendar", "v3", credentials=creds)

        calendar_list = get_calendar_list()

        start_of_tomorrow = (
            (datetime.now(tz=timezone.utc) + relativedelta(days=1))
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat()
        )

        start_of_day_after = (
            (datetime.now(tz=timezone.utc) + relativedelta(days=2))
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat()
        )

        for calendar_id in calendar_list:
            events_result = (
                service.events()
                .list(
                    calendarId=calendar_id,
                    timeMin=start_of_tomorrow,
                    timeMax=start_of_day_after,
                    singleEvents=True,
                    orderBy="startTime",
                )
                .execute()
            )
            events = events_result.get("items", [])

            events = filter_events(events)

            for event in events:
                probable_name = get_probable_name(event["summary"])
                events_tomorrow.append(
                    {
                        "evaluator_email": calendar_id,
                        "start": event["start"].get(
                            "dateTime", event["start"].get("date")
                        ),
                        "firstname": probable_name.split(" ")[0],
                        "lastname": probable_name.split(" ")[1],
                    }
                )
            if not events:
                logger.warning("No events found tomorrow for calendar: " + calendar_id)

    except HttpError as error:
        logger.exception(error)

    return events_tomorrow


def get_tomorrow_clients_from_file():
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


def get_tomorrow_clients():
    file_clients = get_tomorrow_clients_from_file()
    calendar_events = get_events_for_tomorrow()

    clients = []
    for event in calendar_events:
        for id in file_clients:
            client = file_clients[id]
            if (
                (
                    client.get("cal_firstname")
                    and client["cal_firstname"].lower() == event["firstname"].lower()
                )
                or client["firstname"].lower() == event["firstname"].lower()
            ) and client["lastname"].lower() == event["lastname"].lower():
                if not client.get("evaluator_email"):
                    client["evaluator_email"] = event["evaluator_email"]
                client["start"] = event["start"]
                clients.append(client)
                break
        else:
            clients.append(event)

    return clients


def generate_evaluator_email(evaluator_address):
    evaluator_name = evaluator_address[0]["evaluator_email"].split("@")[0].title()
    if evaluator_name.lower() == "marycatherine":
        evaluator_name = "Mary-Catherine"
    email_text = f"Hi {evaluator_name},\n\n"
    email_text += f"Here is what we know about questionnaires for your appointments tomorrow, {(datetime.now() + relativedelta(days=1)).strftime('%m/%d')}:\n"
    for client in evaluator_address:
        sent_date_str = (
            f" [sent on {datetime.strptime(client['sent_date'], '%Y/%m/%d').strftime('%m/%d')}]"
            if client.get("sent_date")
            else ""
        )

        reminded_str = (
            f" [reminded at least {client['reminded']} time{'s' if client['reminded'] > 1 else ''}]"
            if client.get("reminded")
            else ""
        )

        start_time = parser.isoparse(client["start"]).strftime("%-I %p")
        email_text += f"\n{start_time} - {client['firstname']} {client['lastname']}{sent_date_str}{reminded_str}:\n"
        if client.get("questionnaires"):
            for questionnaire in client["questionnaires"]:
                email_text += f"  - {questionnaire['type']} - {'Done' if questionnaire['done'] else 'NOT DONE'}{f' - {questionnaire["link"]}' if not questionnaire['done'] else ''}\n"
        else:
            email_text += (
                "  - No questionnaire information found, may have been sent manually\n"
            )
    email_text += (
        "\nThis is an automated message, information may be incomplete or have changed."
    )
    utils.send_gmail(
        email_text,
        f"Questionnaires for {(datetime.now() + relativedelta(days=1)).strftime('%m/%d')}",
        evaluator_address[0]["evaluator_email"],
        config["automated_email"],
        ",".join(config["cc_emails"]),
    )


def generate_ipad_email(client):
    # TODO: this will require location...
    pass


def main():
    projects_api = utils.init_asana(services)
    driver, actions = utils.initialize_selenium()

    tomorrow_clients = get_tomorrow_clients_from_file()
    if tomorrow_clients:
        email_info = {
            "completed": utils.check_questionnaires(
                driver, config, services, tomorrow_clients
            )
        }
        admin_email_text, admin_email_html = utils.build_admin_email(email_info)
        if admin_email_text != "":
            utils.send_gmail(
                admin_email_text,
                f"Questionnaires completed on {datetime.today().strftime('%a, %b %-d')} for {(datetime.today() + relativedelta(days=1)).strftime('%m/%d')}",
                config["qreceive_emails"],
                config["automated_email"],
                html=admin_email_html,
            )

    driver.quit()

    tomorrow_clients = get_tomorrow_clients_from_file()
    if tomorrow_clients:
        for id in tomorrow_clients:
            client = tomorrow_clients[id]
            utils.mark_links_in_asana(projects_api, client, services, config)

    tomorrow_clients = get_tomorrow_clients()
    clients_by_evaluator = {}
    if tomorrow_clients:
        for client in tomorrow_clients:
            evaluator_email = client.get("evaluator_email")
            if evaluator_email:
                if evaluator_email not in clients_by_evaluator:
                    clients_by_evaluator[evaluator_email] = []
                clients_by_evaluator[evaluator_email].append(client)

    for evaluator in clients_by_evaluator:
        generate_evaluator_email(clients_by_evaluator[evaluator])


if __name__ == "__main__":
    main()
