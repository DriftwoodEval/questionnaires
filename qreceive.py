from datetime import date, datetime, timedelta
from typing import Optional, Union

from dateutil.relativedelta import relativedelta
from loguru import logger
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.remote.webdriver import WebDriver

from utils.database import (
    get_previous_clients,
    update_failure_in_db,
    update_questionnaires_in_db,
)
from utils.google import build_admin_email, send_gmail, update_punch_by_column
from utils.misc import check_distance, load_config
from utils.openphone import NotEnoughCreditsError, OpenPhone
from utils.questionnaires import (
    all_questionnaires_done,
    check_if_ignoring,
    check_questionnaires,
    get_most_recent_not_done,
)
from utils.selenium import (
    check_if_docs_signed,
    check_if_opened_portal,
    go_to_client,
    initialize_selenium,
    login_ta,
    resend_portal_invite,
)
from utils.types import (
    AdminEmailInfo,
    ClientWithQuestionnaires,
    Config,
    FailedClientFromDB,
    Questionnaire,
    Services,
    validate_questionnaires,
)

logger.add("logs/qreceive.log", rotation="500 MB")


def build_q_message(
    config: Config,
    client: ClientWithQuestionnaires,
    most_recent_q: Questionnaire,
    distance: int,
) -> Optional[str]:
    """Builds the message to be sent to the client based on their most recent questionnaire."""
    link_count = len([q for q in client.questionnaires if q["status"] == "PENDING"])

    if distance == 0:
        distance_phrase = "today"
    elif distance == -1:
        distance_phrase = f"on {most_recent_q['sent'].strftime('%m/%d')} (yesterday)"
    else:
        distance_phrase = (
            f"on {most_recent_q['sent'].strftime('%m/%d')} ({abs(distance)} days ago)"
        )

    message = None
    if most_recent_q["reminded"] == 0:
        message = f"Hello, this is {config.name} from Driftwood Evaluation Center. We are ready to schedule your appointment! In order for us to schedule your appointment, we need you to complete your {'questionnaire' if link_count == 1 else 'questionnaires'}. You can find {'it' if link_count == 1 else 'them'} in the messages tab in our patient portal: https://portal.therapyappointment.com Please reply to this text with any questions. Thank you for your help."
    elif most_recent_q["reminded"] == 1:
        message = f"Hello, this is {config.name} with Driftwood Evaluation Center. We are waiting for you to complete the questionnaire{'' if link_count == 1 else 's'} sent to you {distance_phrase}. We are unable to schedule your appointment until {'it is' if link_count == 1 else 'they are'} completed in {'its' if link_count == 1 else 'their'} entirety. You can find {'it' if link_count == 1 else 'them'} in the messages tab in our patient portal: https://portal.therapyappointment.com Please reply to this text with any questions. Thank you for your help."
    elif most_recent_q["reminded"] == 2:
        message = f"This is Driftwood Evaluation Center. If your questionnaire{' is' if link_count == 1 else 's are'} not completed by {(datetime.now() + timedelta(days=3)).strftime('%m/%d')} (3 days from now), we will close out your referral. Reply to this text with any concerns. You can find the questionnaire{'' if link_count == 1 else 's'} in the messages tab in our patient portal: https://portal.therapyappointment.com"

    return message


def build_failure_message(config: Config, client: FailedClientFromDB) -> Optional[str]:
    """Builds a message to be sent to the client based on their failure."""
    message = None
    if client.failure["reason"] == "portal not opened":
        message = f"Hi, this is {config.name} from Driftwood Evaluation Center. We noticed you haven't accessed the patient portal, TherapyAppointment as of yet. I resent the invite through email. We won't be able to move ahead with scheduling the appointment until this is done. Please let us know if you have any questions or need assistance. Thank you."
    elif client.failure["reason"] == "docs not signed":
        message = f'This is {config.name} from Driftwood Evaluation Center. We see that you signed into your portal at portal.therapyappointment.com but you didn\'t complete the Forms under the "Forms" section. Please sign back in, navigate to the Forms section, and complete the forms not marked as "Completed" to move forward with the evaluation process. Thank you!'

    return message


def should_send_reminder(reminded_count: int, last_reminded_distance: int) -> bool:
    """Checks if a reminder should be sent to the client, based on the last reminder distance."""
    reminder_schedule = {
        0: 0,  # Initial message (same day)
        1: 7,  # First follow-up (1 week later)
        2: 5,  # Second follow-up (5 days after first follow-up)
    }

    expected_day = reminder_schedule.get(reminded_count)
    if expected_day is not None and last_reminded_distance >= expected_day:
        logger.debug(
            f"Reminder should be sent because client has been reminded {reminded_count} times, and it has been {last_reminded_distance} days since the last reminder"
        )
        return True
    else:
        return False


def check_failures(
    config: Config,
    services: Services,
    driver: WebDriver,
    actions: ActionChains,
    failed_clients: dict[int, FailedClientFromDB],
):
    """Checks the failures of clients and updates them in the database."""
    login_ta(driver, actions, services)

    two_years_ago = date.today() - relativedelta(years=2)
    five_years_ago = date.today() - relativedelta(years=5)

    for client_id, client in failed_clients.items():
        reason = client.failure["reason"]
        is_resolved = False

        if reason in ["portal not opened", "docs not signed"]:
            go_to_client(driver, actions, str(client_id))
            if reason == "portal not opened":
                is_resolved = check_if_opened_portal(driver)
            elif reason == "docs not signed":
                is_resolved = check_if_docs_signed(driver)

        elif reason == "too young for asd" and client.dob is not None:
            is_resolved = client.dob < two_years_ago

        elif reason == "too young for adhd" and client.dob is not None:
            is_resolved = client.dob < five_years_ago

        if is_resolved:
            update_failure_in_db(config, client_id, reason, resolved=True)
            logger.info(f"Resolved failure for client {client.fullName}")


ClientType = Union[FailedClientFromDB, ClientWithQuestionnaires]


def main():
    """Main function for qreceive.py."""
    services, config = load_config()
    openphone = OpenPhone(config, services)
    driver, actions = initialize_selenium()
    email_info: AdminEmailInfo = {
        "ignoring": [],
        "failed": [],
        "call": [],
        "completed": [],
        "errors": [],
    }

    try:
        # Check on questionnaires and update DB
        clients, failed_clients = get_previous_clients(config, True)
        if clients is None:
            logger.critical("Failed to get previous clients")
            return

        clients = validate_questionnaires(clients)
        email_info["completed"], email_info["errors"] = check_questionnaires(
            driver, config, clients
        )
        driver.quit()

        # Check failures and update in DB
        driver, actions = initialize_selenium()
        check_failures(config, services, driver, actions, failed_clients)
        driver.quit()

        # Send reminders for failures and questionnaires

        driver, actions = initialize_selenium()
        login_ta(driver, actions, services)

        clients, failed_clients = get_previous_clients(config)

        messages_sent: list[
            tuple[FailedClientFromDB | ClientWithQuestionnaires, str]
        ] = []
        numbers_sent = []

        if failed_clients:
            for _, client in failed_clients.items():
                if client.failure["reason"] in ["portal not opened", "docs not signed"]:
                    if client.note and "app.pandadoc.com" in str(client.note):
                        logger.info(
                            f"Client {client.fullName} likely doesn't speak English, skipping"
                        )
                        continue

                    last_reminded = client.failure["lastReminded"]
                    if last_reminded is not None:
                        last_reminded_distance = check_distance(last_reminded)
                    else:
                        last_reminded_distance = 0

                    logger.info(
                        f"Client {client.fullName} has issue {client.failure['reason']}"
                    )

                    if not client.phoneNumber:
                        logger.warning(f"Client {client.fullName} has no phone number")
                        email_info["failed"].append((client, "No phone number"))
                        continue

                    already_messaged_today = client.phoneNumber in numbers_sent

                    if already_messaged_today:
                        logger.warning(
                            f"Already messaged {client.fullName} at {client.phoneNumber} today"
                        )

                    if client.failure["reminded"] == 3 and last_reminded_distance > 3:
                        email_info["call"].append(client)
                        client.failure["reminded"] += 1
                        client.failure["lastReminded"] = date.today()

                    elif (
                        client.failure["reminded"] < 3
                        and not already_messaged_today
                        and client.phoneNumber
                    ):
                        if should_send_reminder(
                            client.failure["reminded"], last_reminded_distance
                        ):
                            logger.info(f"Sending reminder TO {client.fullName}")
                            if client.failure["reason"] == "portal not opened":
                                resend_portal_invite(driver, actions, str(client.id))

                            message = build_failure_message(config, client)
                            # Redundant failsafe to super ensure we don't text people a message that just says "None"
                            if not message:
                                logger.error(
                                    f"Failed to build message for {client.fullName}"
                                )
                                continue

                            try:
                                attempt_text = openphone.send_text(
                                    message, client.phoneNumber
                                )

                                if attempt_text and "id" in attempt_text:
                                    numbers_sent.append(client.phoneNumber)
                                    messages_sent.append((client, attempt_text["id"]))
                                else:
                                    logger.error(
                                        f"Failed to send message to {client.fullName}"
                                    )
                                    email_info["failed"].append(
                                        (client, "Failed to send text request")
                                    )
                            except NotEnoughCreditsError:
                                logger.critical(
                                    "Aborting all further message sends due to insufficient credits."
                                )
                                email_info["errors"].append(
                                    "OpenPhone API needs more credits to send messages."
                                )
                                break

        if clients:
            clients = validate_questionnaires(clients)
            for _, client in clients.items():
                done = all_questionnaires_done(client)

                if check_if_ignoring(client):
                    logger.warning(f"Client {client.fullName} is being ignored.")
                    email_info["ignoring"].append(client)
                    continue

                if not done:
                    most_recent_q = get_most_recent_not_done(client)
                    distance = check_distance(most_recent_q["sent"])
                    last_reminded = most_recent_q.get("lastReminded")
                    if last_reminded is not None:
                        last_reminded_distance = check_distance(last_reminded)
                    else:
                        last_reminded_distance = 0

                    logger.info(
                        f"{client.fullName} had questionnaire sent on {most_recent_q['sent']} and isn't done"
                    )

                    if not client.phoneNumber:
                        logger.warning(f"Client {client.fullName} has no phone number")
                        email_info["failed"].append((client, "No phone number"))
                        continue

                    already_messaged_today = client.phoneNumber in numbers_sent

                    if already_messaged_today:
                        logger.warning(
                            f"Already messaged {client.fullName} at {client.phoneNumber} today"
                        )

                    if most_recent_q["reminded"] == 3 and last_reminded_distance >= 3:
                        email_info["call"].append(client)
                        for q in client.questionnaires:
                            if q["status"] == "PENDING":
                                q["reminded"] += 1
                                q["lastReminded"] = date.today()

                    elif (
                        most_recent_q["reminded"] < 3
                        and not already_messaged_today
                        and client.phoneNumber
                    ):
                        if should_send_reminder(
                            most_recent_q["reminded"], last_reminded_distance
                        ):
                            logger.info(f"Sending reminder TO {client.fullName}")
                            message = build_q_message(
                                config, client, most_recent_q, distance
                            )
                            # Redundant failsafe to super ensure we don't text people a message that just says "None"
                            if not message:
                                logger.error(
                                    f"Failed to build message for {client.fullName}"
                                )
                                continue

                            try:
                                attempt_text = openphone.send_text(
                                    message, client.phoneNumber
                                )

                                if attempt_text and "id" in attempt_text:
                                    numbers_sent.append(client.phoneNumber)
                                    messages_sent.append((client, attempt_text["id"]))
                                else:
                                    logger.error(
                                        f"Failed to send message to {client.fullName}"
                                    )
                                    email_info["failed"].append(
                                        (client, "Failed to send text request")
                                    )
                            except NotEnoughCreditsError:
                                logger.critical(
                                    "Aborting all further message sends due to insufficient credits."
                                )
                                email_info["errors"].append(
                                    "OpenPhone API needs more credits to send messages."
                                )
                                break
                elif client in email_info["completed"]:
                    if len(client.questionnaires) > 2:
                        update_punch_by_column(config, str(client.id), "DA", "done")
                        update_punch_by_column(config, str(client.id), "EVAL", "done")
                    else:
                        update_punch_by_column(config, str(client.id), "DA", "done")

        # Check message status
        logger.info(f"Starting status check for {len(messages_sent)} messages.")

        clients_to_update_db = []

        for client, message_id in messages_sent:
            try:
                delivered = openphone.check_text_delivered(message_id)

                if delivered:
                    logger.success(
                        f"Successfully delivered message to {client.fullName} ({message_id})"
                    )

                    if isinstance(client, FailedClientFromDB):
                        client.failure["reminded"] += 1
                        client.failure["lastReminded"] = date.today()
                        clients_to_update_db.append(client)
                    elif isinstance(client, ClientWithQuestionnaires):
                        for q in client.questionnaires:
                            if q["status"] == "PENDING":
                                q["reminded"] += 1
                                q["lastReminded"] = date.today()
                        clients_to_update_db.append(client)
                else:
                    logger.error(
                        f"Failed to deliver message to {client.fullName} ({message_id})"
                    )
                    email_info["failed"].append(
                        (client, "Did not deliver within timeout")
                    )
            except Exception as e:
                logger.error(
                    f"Error checking message status for {client.fullName} ({message_id}): {e}"
                )
                email_info["errors"].append(
                    f"Error checking message status for {client.fullName}: {e}"
                )

        # Update DB
        for client in clients_to_update_db:
            if isinstance(client, FailedClientFromDB):
                update_failure_in_db(
                    config,
                    client.id,
                    client.failure["reason"],
                    reminded=client.failure["reminded"],
                    last_reminded=client.failure["lastReminded"],
                )
            elif isinstance(client, ClientWithQuestionnaires):
                update_questionnaires_in_db(config, [client])

    except Exception as e:
        error_message = f"An unhandled exception occurred during the run: {e}"
        logger.exception(error_message)
        email_info["errors"].append(error_message)
        raise

    finally:
        admin_email_text, admin_email_html = build_admin_email(email_info)
        if admin_email_text != "":
            try:
                send_gmail(
                    admin_email_text,
                    f"Receive Run for {datetime.today().strftime('%a, %b')} {datetime.today().day}",
                    ",".join(config.qreceive_emails),
                    config.automated_email,
                    html=admin_email_html,
                )
            except Exception as e:
                logger.error(f"Failed to send the admin email: {e}")


if __name__ == "__main__":
    main()
