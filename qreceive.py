import sys
from datetime import date, datetime, timedelta
from typing import Optional, Union, cast

from dateutil.relativedelta import relativedelta
from loguru import logger
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.remote.webdriver import WebDriver

from utils.custom_types import (
    AdminEmailInfo,
    ClientWithQuestionnaires,
    Config,
    FailedClientFromDB,
    Failure,
    Questionnaire,
    Services,
    validate_questionnaires,
)
from utils.database import (
    get_most_recent_failure,
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
    filter_inactive_and_not_pending,
    get_most_recent_not_done,
)
from utils.selenium import (
    check_and_login_ta,
    check_if_docs_signed,
    check_if_opened_portal,
    go_to_client,
    initialize_selenium,
    resend_portal_invite,
)

logger.remove()
logger.add(
    sys.stdout,
    format="[<dim>{time:YY-MM-DD HH:mm:ss}</dim>] <level>{level: <8}</level> | <level>{message}</level>",
)

logger.add("logs/qreceive.log", rotation="500 MB")


def build_q_message(
    config: Config,
    client: ClientWithQuestionnaires,
    most_recent_q: Questionnaire,
    distance: int,
) -> Optional[str]:
    """Builds the message to be sent to the client based on their most recent questionnaire."""
    if not most_recent_q["sent"]:
        logger.warning(
            f"{client.fullName}'s {most_recent_q['questionnaireType']} has no sent date, cannot build message"
        )
        return None

    link_count = len(
        [
            q
            for q in client.questionnaires
            if q["status"]
            in [
                "PENDING",
                #  "SPANISH"
                "POSTEVAL_PENDING",
            ]
        ]
    )
    # is_spanish = any(q["status"] == "SPANISH" for q in client.questionnaires)
    is_spanish = False
    is_posteval = any(q["status"] == "POSTEVAL_PENDING" for q in client.questionnaires)
    portal_link = "https://portal.therapyappointment.com"

    if distance == 0:
        distance_phrase_en = "today"
        distance_phrase_es = "hoy"
    elif distance == -1:
        date_str = most_recent_q["sent"].strftime("%m/%d")
        distance_phrase_en = f"on {date_str} (yesterday)"
        distance_phrase_es = f"el {date_str} (ayer)"
    else:
        date_str = most_recent_q["sent"].strftime("%m/%d")
        days_ago = abs(distance)
        distance_phrase_en = f"on {date_str} ({days_ago} days ago)"
        distance_phrase_es = f"el {date_str} (hace {days_ago} días)"

    q_s_en = "questionnaire" if link_count == 1 else "questionnaires"
    it_them_en = "it" if link_count == 1 else "them"
    is_are_en = "is" if link_count == 1 else "are"
    its_their_en = "its" if link_count == 1 else "their"

    q_s_es = "cuestionario" if link_count == 1 else "cuestionarios"
    lo_los_es = "lo" if link_count == 1 else "los"
    esta_estan_es = "está" if link_count == 1 else "están"
    su_sus_es = "su" if link_count == 1 else "sus"
    sent_s_es = "" if link_count == 1 else "s"
    complete_s_es = "" if link_count == 1 else "s"
    sent_it_them_es = "Lo enviamos" if link_count == 1 else "Los enviamos"

    messages_en = {
        0: (
            f"Hello, this is {config.name} from Driftwood Evaluation Center. "
            f"{'We are ready to schedule your appointment! In order for us to schedule your appointment, ' if not is_posteval else 'In order to provide you with a comprehensive report, '}"
            f"we need you to complete your {q_s_en}. You can find {it_them_en} in the messages tab "
            f"in our patient portal: {portal_link} Please reply to this text with any questions. "
            f"Thank you for your help."
        ),
        1: (
            f"Hello, this is {config.name} with Driftwood Evaluation Center. "
            f"We are waiting for you to complete the {q_s_en} sent to you {distance_phrase_en}. "
            f"{'We are unable to schedule your appointment' if not is_posteval else 'We are unable to provide you with a comprehensive report'} until {it_them_en} {is_are_en} completed "
            f"in {its_their_en} entirety. You can find {it_them_en} in the messages tab in our "
            f"patient portal: {portal_link} Please reply to this text with any questions. "
            f"Thank you for your help."
        ),
        2: (
            f"This is Driftwood Evaluation Center. If your {q_s_en} {is_are_en} not completed by "
            f"{(datetime.now() + timedelta(days=3)).strftime('%m/%d')} (3 days from now), "
            f"we will {'close out your referral' if not is_posteval else 'provide you with an incomplete report'}. Reply to this text with any concerns. You can find the "
            f"{q_s_en} in the messages tab in our patient portal: {portal_link}"
        ),
    }

    messages_es = {
        0: (
            f"Hola, es {config.name} de Driftwood Evaluation Center. ¡Estamos listos para "
            f"programar su cita! Para poder programar su cita, necesitamos que complete {su_sus_es} "
            f"{q_s_es}. {sent_it_them_es} a su correo electrónico desde una dirección DriftwoodEval.com. "
            f"Por favor, responda a este mensaje con cualquier pregunta. Gracias."
        ),
        1: (
            f"Hola, es {config.name} de Driftwood Evaluation Center. Estamos esperando que "
            f"complete {su_sus_es} {q_s_es} enviado{sent_s_es} {distance_phrase_es}. "
            f"No podemos programar su cita hasta que {lo_los_es} {esta_estan_es} "
            f"completo{complete_s_es} en {su_sus_es} totalidad. {sent_it_them_es} a su correo electrónico "
            f"desde una dirección DriftwoodEval.com. Por favor, responda a este mensaje con "
            f"cualquier pregunta. Gracias."
        ),
        2: (
            f"Es Driftwood Evaluation Center. Si {su_sus_es} {q_s_es} no {esta_estan_es} "
            f"completo{complete_s_es} antes de "
            f"{(datetime.now() + timedelta(days=3)).strftime('%m/%d')} (en 3 días), "
            f"cerraremos su remisión. Responda a este mensaje con cualquier inquietud. "
            f"{sent_it_them_es} a su correo electrónico desde una dirección DriftwoodEval.com."
        ),
    }

    reminded_count = most_recent_q["reminded"]

    if is_spanish:
        message = messages_es.get(reminded_count)
    else:
        message = messages_en.get(reminded_count)

    return message


def build_failure_message(config: Config, client: FailedClientFromDB) -> Optional[str]:
    """Builds a message to be sent to the client based on their failure."""
    for failure_data in client.failure:
        reason = failure_data["reason"]
        message = None
        if reason == "portal not opened":
            message = f"Hi, this is {config.name} from Driftwood Evaluation Center. We noticed you haven't accessed the patient portal, TherapyAppointment as of yet. I resent the invite through email. We won't be able to move ahead with scheduling the appointment until this is done. Please let us know if you have any questions or need assistance. Thank you."
            return message
        elif reason == "docs not signed":
            message = f'This is {config.name} from Driftwood Evaluation Center. We see that you signed into your portal at portal.therapyappointment.com but you didn\'t complete the Forms under the "Forms" section. Please sign back in, navigate to the Forms section, and complete the forms not marked as "Completed" to move forward with the evaluation process. Thank you!'
            return message

    return None


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
    logger.debug("Checking on failures for clients")
    two_years_ago = date.today() - relativedelta(years=2)
    five_years_ago = date.today() - relativedelta(years=5)

    for client_id, client in failed_clients.items():
        for failure_data in client.failure:
            reason = failure_data["reason"]
            is_resolved = False

            if reason in ["portal not opened", "docs not signed"]:
                go_to_client(driver, actions, services, str(client_id))
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
                logger.info(f"Resolved failure for {client.fullName}")
            else:
                # This updates the checked date
                update_failure_in_db(config, client_id, reason)


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
        "ifsp_download_needed": [],
    }

    try:
        # Check on questionnaires and update DB
        clients, failed_clients = get_previous_clients(config, True)
        if clients is None:
            logger.critical("Failed to get previous clients")
            return

        email_info["ifsp_download_needed"] = [
            client
            for client_id, client in clients.items()
            if client.ifsp and not client.ifspDownloaded
        ]

        clients = validate_questionnaires(clients)
        clients = filter_inactive_and_not_pending(clients)

        email_info["completed"], email_info["errors"] = check_questionnaires(
            driver, config, clients
        )
        driver.quit()

        # Check failures and update in DB
        driver, actions = initialize_selenium()
        check_failures(config, services, driver, actions, failed_clients)

        # Send reminders for failures and questionnaires
        clients, failed_clients = get_previous_clients(config, failed=True)

        messages_sent: list[
            tuple[FailedClientFromDB | ClientWithQuestionnaires, str, str | None]
        ] = []
        numbers_sent = []

        if failed_clients:
            for _, client in failed_clients.items():
                if any(
                    failure["reason"] in ["portal not opened", "docs not signed"]
                    for failure in client.failure
                ):
                    if client.note and "app.pandadoc.com" in str(client.note):
                        logger.info(
                            f"{client.fullName} likely doesn't speak English, skipping"
                        )
                        continue

                    most_recent_failure = get_most_recent_failure(client)
                    if not most_recent_failure:
                        logger.warning(
                            f"{client.fullName} has no unresolved failures, skipping"
                        )
                        continue

                    reason = most_recent_failure["reason"]
                    reminded_count = most_recent_failure["reminded"]
                    last_reminded = most_recent_failure["lastReminded"]

                    if last_reminded is not None:
                        last_reminded_distance = check_distance(last_reminded)
                    else:
                        last_reminded_distance = 0

                    logger.info(
                        f"{client.fullName} has failure {reason}, checking if they should be reminded"
                    )

                    if not client.phoneNumber:
                        logger.warning(f"{client.fullName} has no phone number")
                        email_info["failed"].append((client, "No phone number"))
                        continue

                    already_messaged_today = client.phoneNumber in numbers_sent

                    if already_messaged_today:
                        logger.warning(
                            f"Already messaged {client.fullName} at {client.phoneNumber} today"
                        )

                    if reminded_count == 3 and last_reminded_distance > 3:
                        email_info["call"].append(client)
                        update_failure_in_db(
                            config,
                            client.id,
                            reason,
                            reminded=reminded_count + 1,
                            last_reminded=date.today(),
                        )

                    elif (
                        reminded_count < 3
                        and not already_messaged_today
                        and client.phoneNumber
                    ):
                        if should_send_reminder(reminded_count, last_reminded_distance):
                            logger.info(f"Sending reminder TO {client.fullName}")
                            if reason == "portal not opened":
                                try:
                                    resend_portal_invite(
                                        driver, actions, services, str(client.id)
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"Failed to resend invite for {client.fullName}: {e}"
                                    )
                                    email_info["failed"].append(
                                        (client, "Failed to resend invite")
                                    )
                                    continue

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
                                    messages_sent.append(
                                        (client, attempt_text["id"], reason)
                                    )

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
                    logger.warning(f"{client.fullName} is being ignored.")
                    email_info["ignoring"].append(client)
                    continue

                if any(client.fullName in error for error in email_info["errors"]):
                    logger.warning(f"{client.fullName} has an error, skipping")
                    continue

                if not done:
                    most_recent_q = get_most_recent_not_done(client)
                    if not most_recent_q or not most_recent_q["sent"]:
                        logger.warning(
                            f"{client.fullName} has no pending questionnaires with dates, skipping"
                        )
                        continue
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
                        logger.warning(f"{client.fullName} has no phone number")
                        email_info["failed"].append((client, "No phone number"))
                        continue

                    already_messaged_today = client.phoneNumber in numbers_sent

                    if already_messaged_today:
                        logger.warning(
                            f"Already messaged {client.fullName} at {client.phoneNumber} today"
                        )

                    if most_recent_q["reminded"] == 3 and last_reminded_distance >= 3:
                        email_info["call"].append(client)

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
                                    messages_sent.append(
                                        (client, attempt_text["id"], None)
                                    )
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

        for client, message_id, failure_reason in messages_sent:
            try:
                delivered = openphone.check_text_delivered(message_id)

                if delivered:
                    logger.success(
                        f"Successfully delivered message to {client.fullName} ({message_id})"
                    )

                    if failure_reason is not None and isinstance(
                        client, FailedClientFromDB
                    ):
                        failure_to_update = next(
                            (
                                f
                                for f in client.failure
                                if f.get("reason") == failure_reason
                            ),
                            None,
                        )
                        if failure_to_update:
                            new_reminded_count = failure_to_update["reminded"] + 1
                            today = date.today()

                            clients_to_update_db.append(
                                (
                                    client.id,
                                    failure_reason,
                                    new_reminded_count,
                                    today,
                                )
                            )
                        else:
                            logger.error(
                                f"Delivered message for unknown failure reason '{failure_reason}' for {client.fullName}"
                            )
                    elif isinstance(client, ClientWithQuestionnaires):
                        for q in client.questionnaires:
                            if (
                                q["status"] == "PENDING"
                                or q["status"] == "POSTEVAL_PENDING"
                                # or q["status"] == "SPANISH"
                            ):
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
            if isinstance(client, ClientWithQuestionnaires):
                update_questionnaires_in_db(config, [client])
            else:
                client_id, reason, reminded, last_reminded = client
                update_failure_in_db(
                    config,
                    client_id,
                    reason,
                    reminded=reminded,
                    last_reminded=last_reminded,
                )

    except Exception as e:
        error_message = f"An unhandled exception occurred during the run: {e}"
        logger.exception("Unhandled exception occurred during the run")
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
            except Exception:
                logger.exception("Failed to send the admin email")


if __name__ == "__main__":
    main()
