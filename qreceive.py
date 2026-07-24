import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import typer
from dateutil.relativedelta import relativedelta
from loguru import logger
from selenium.webdriver.remote.webdriver import WebDriver

from utils.custom_types import (
    AdminEmailInfo,
    ClientFromDB,
    ClientWithQuestionnaires,
    Config,
    FailedClientFromDB,
    Services,
    validate_questionnaires,
)
from utils.database import (
    get_most_recent_eval_appointment_dates,
    get_most_recent_failure,
    get_previous_clients,
    get_questionnaire_rules,
    get_sent_referral_client_ids,
    has_requested_records_date,
    log_questionnaire_msg,
    log_referral_msg,
    update_failure_in_db,
    update_questionnaires_in_db,
)
from utils.google import (
    batch_update_punch_list,
    build_admin_email,
    send_gmail,
)
from utils.messages import build_q_message
from utils.misc import check_distance, json_log_format, load_config
from utils.openphone import InvalidPhoneNumberError, NotEnoughCreditsError, OpenPhone
from utils.platforms.therapyappointment import (
    check_if_docs_signed,
    check_if_opened_portal,
    go_to_client,
    resend_portal_invite,
)
from utils.questionnaires import (
    all_questionnaires_done,
    check_battery_completeness,
    check_battery_sent,
    check_if_ignoring,
    check_questionnaires,
    filter_inactive_and_not_pending,
    get_most_recent_not_done,
)
from utils.selenium import (
    initialize_selenium,
)
from utils.task_tracker import start_task

logger.remove()
logger.add(
    sys.stdout,
    format="[<dim>{time:YY-MM-DD HH:mm:ss}</dim>] <level>{level: <8}</level> | <level>{message}</level>",
)

logger.add("logs/qreceive.log", format=json_log_format, rotation="500 MB")

app = typer.Typer()

PENDING_EMAIL_PATH = Path("logs/pending_email.json")


def _serialize_email_info(email_info: AdminEmailInfo) -> dict:
    """Turn an AdminEmailInfo (holding live pydantic client objects) into plain JSON.

    Used to persist a run's results to PENDING_EMAIL_PATH between cron invocations,
    since qreceive runs multiple times a day but only emails admins once (see
    _save_pending_email). Each client is tagged with "_type" so
    _deserialize_email_info knows which pydantic model to rebuild it as.
    """

    def serialize_client(
        client: ClientWithQuestionnaires | FailedClientFromDB | ClientFromDB,
    ) -> dict:
        if isinstance(client, FailedClientFromDB):
            return {"_type": "failed", **client.model_dump(mode="json")}
        if isinstance(client, ClientWithQuestionnaires):
            return {"_type": "with_q", **client.model_dump(mode="json")}
        return {"_type": "client", **client.model_dump(mode="json")}

    return {
        "ignoring": [c.model_dump(mode="json") for c in email_info["ignoring"]],
        "completed": [c.model_dump(mode="json") for c in email_info["completed"]],
        "call": [serialize_client(c) for c in email_info["call"]],
        "failed": [
            {"client": serialize_client(item[0]), "reason": item[1]}
            for item in email_info["failed"]
        ],
        "errors": email_info["errors"],
    }


def _deserialize_email_info(data: dict) -> AdminEmailInfo:
    """Inverse of _serialize_email_info — rebuilds pydantic clients from the "_type" tag."""

    def deserialize_call_client(
        d: dict,
    ) -> ClientWithQuestionnaires | FailedClientFromDB:
        d = dict(d)
        t = d.pop("_type")
        if t == "failed":
            return FailedClientFromDB.model_validate(d)
        return ClientWithQuestionnaires.model_validate(d)

    def deserialize_failed_client(
        d: dict,
    ) -> ClientWithQuestionnaires | FailedClientFromDB | ClientFromDB:
        d = dict(d)
        t = d.pop("_type")
        if t == "failed":
            return FailedClientFromDB.model_validate(d)
        if t == "client":
            return ClientFromDB.model_validate(d)
        return ClientWithQuestionnaires.model_validate(d)

    return {
        "ignoring": [
            ClientWithQuestionnaires.model_validate(c) for c in data["ignoring"]
        ],
        "completed": [
            ClientWithQuestionnaires.model_validate(c) for c in data["completed"]
        ],
        "call": [deserialize_call_client(c) for c in data["call"]],
        "failed": [
            (deserialize_failed_client(item["client"]), item["reason"])
            for item in data["failed"]
        ],
        "errors": data["errors"],
    }


def _merge_email_infos(infos: list[AdminEmailInfo]) -> AdminEmailInfo:
    """Combine the accumulated non-1pm runs' results with the 1pm run's before emailing.

    De-dupes by client ID (a client could show up in multiple runs' results) and drops
    anyone who ended up "completed" from the "call" list, since a later run may have
    resolved what an earlier run flagged.
    """
    merged: AdminEmailInfo = {
        "ignoring": [],
        "completed": [],
        "call": [],
        "failed": [],
        "errors": [],
    }
    seen_ignoring: set[int] = set()
    seen_completed: set[int] = set()
    seen_call: set[int] = set()
    seen_failed: set[int] = set()
    seen_errors: set[str] = set()

    for info in infos:
        for client in info["ignoring"]:
            if client.id not in seen_ignoring:
                seen_ignoring.add(client.id)
                merged["ignoring"].append(client)
        for client in info["completed"]:
            if client.id not in seen_completed:
                seen_completed.add(client.id)
                merged["completed"].append(client)
        for client in info["call"]:
            if client.id not in seen_call:
                seen_call.add(client.id)
                merged["call"].append(client)
        for item in info["failed"]:
            if item[0].id not in seen_failed:
                seen_failed.add(item[0].id)
                merged["failed"].append(item)
        for error in info["errors"]:
            if error not in seen_errors:
                seen_errors.add(error)
                merged["errors"].append(error)

    # A client in "completed" has finished their objectives — remove them from "call"
    merged["call"] = [c for c in merged["call"] if c.id not in seen_completed]

    return merged


def _save_pending_email(email_info: AdminEmailInfo) -> None:
    """Append this run's results to the on-disk queue, to be emailed at the next 1pm run."""
    existing = _load_pending_email()
    runs = [*existing, email_info]
    PENDING_EMAIL_PATH.write_text(
        json.dumps({"runs": [_serialize_email_info(r) for r in runs]}, indent=2)
    )
    logger.info(f"Queued email content for 1pm send ({len(runs)} run(s) accumulated)")


def _load_pending_email() -> list[AdminEmailInfo]:
    """Load the queued results from prior non-1pm runs since the last successful email."""
    if not PENDING_EMAIL_PATH.exists():
        return []
    try:
        data = json.loads(PENDING_EMAIL_PATH.read_text())
        return [_deserialize_email_info(r) for r in data.get("runs", [])]
    except Exception as e:
        logger.warning(f"Failed to load pending email queue: {e}")
        return []


def build_failure_message(config: Config, client: FailedClientFromDB) -> str | None:
    """Builds a message to be sent to the client based on their failure."""
    for failure_data in client.failure:
        reason = failure_data["reason"]
        if reason == "portal not opened":
            return f"Hi, this is {config.name} from Driftwood Evaluation Center. We noticed you haven't accessed the patient portal, TherapyAppointment as of yet. I resent the invite through email. We won't be able to move ahead with scheduling the appointment until this is done. Please let us know if you have any questions or need assistance. Thank you."
        if reason == "docs not signed":
            return f'This is {config.name} from Driftwood Evaluation Center. We see that you signed into your portal at portal.therapyappointment.com but you didn\'t complete the Forms under the "Forms" section. Please sign back in, navigate to the Forms section, and complete the forms not marked as "Completed" to move forward with the evaluation process. Thank you!'

    return None


def should_send_reminder(reminded_count: int, last_reminded_distance: int) -> bool:
    """Checks if a reminder should be sent to the client, based on the last reminder distance."""
    reminder_schedule = {
        0: 0,  # Initial message (same day)
        1: 14,  # First follow-up (2 weeks later)
        2: 7,  # Second follow-up (1 week after first follow-up)
    }

    expected_day = reminder_schedule.get(reminded_count)
    if expected_day is not None and last_reminded_distance >= expected_day:
        logger.debug(
            f"Reminder should be sent because client has been reminded {reminded_count} times, and it has been {last_reminded_distance} days since the last reminder"
        )
        return True
    return False


def check_failures(
    config: Config,
    services: Services,
    driver: WebDriver,
    failed_clients: dict[int, FailedClientFromDB],
):
    """Checks the failures of clients and updates them in the database."""
    logger.debug("Checking on failures for clients")
    # Matches the "too young for asd"/"too young for adhd" failure reasons below:
    # ASD questionnaires require the client to be at least 2, ADHD at least 5.
    two_years_ago = date.today() - relativedelta(years=2)
    five_years_ago = date.today() - relativedelta(years=5)

    for client_id, client in failed_clients.items():
        for failure_data in client.failure:
            reason = failure_data["reason"]
            is_resolved = False

            if reason in ["portal not opened", "docs not signed"]:
                go_to_client(driver, services, str(client_id))
                if reason == "portal not opened":
                    is_resolved = check_if_opened_portal(driver)
                elif reason == "docs not signed":
                    is_resolved = check_if_docs_signed(driver)

            elif reason == "too young for asd" and client.dob is not None:
                is_resolved = client.dob < two_years_ago

            elif reason == "too young for adhd" and client.dob is not None:
                is_resolved = client.dob < five_years_ago

            elif reason == "District on receive does not match district on send":
                is_resolved = has_requested_records_date(config, client_id)

            if is_resolved:
                update_failure_in_db(config, client_id, reason, resolved=True)
                logger.info(f"Resolved failure for {client.fullName}")
            else:
                update_failure_in_db(config, client_id, reason)


@app.command()
def main(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Run without sending texts, updating the punch list, or writing reminder state to the DB.",
    ),
    skip_failures: bool = typer.Option(
        False,
        "--skip-failures",
        help="Skip checking on and sending reminders for failures; only process questionnaire completion.",
    ),
    force_send: bool = typer.Option(
        False,
        "--force-send",
        help="Send texts regardless of the current time (bypasses the 1pm-only window).",
    ),
    debug_batteries: bool = typer.Option(
        False,
        "--debug-batteries",
        help="Show detailed battery sent/done analysis for every client and exit. No changes made.",
    ),
    sync_batteries: bool = typer.Option(
        False,
        "--sync-batteries",
        help="Only update the DA/EVAL Qs Sent/Done columns on the punch list. No texts or questionnaire checks.",
    ),
):
    """Main function for qreceive.py."""
    if sync_batteries:
        services, config = load_config()
        rules = get_questionnaire_rules(config)
        eval_dates = get_most_recent_eval_appointment_dates(config)
        clients_raw, _ = get_previous_clients(config, failed=False)
        clients_with_qs = validate_questionnaires(clients_raw)
        logger.info(f"Syncing battery columns for {len(clients_with_qs)} clients")
        sync_updates: list[tuple[str, str, str]] = []
        for client in clients_with_qs.values():
            eval_date = eval_dates.get(client.id)
            da_done, eval_done = check_battery_completeness(
                client, rules, most_recent_eval_date=eval_date
            )
            da_sent, eval_sent = check_battery_sent(
                client, rules, most_recent_eval_date=eval_date
            )
            if da_done is True:
                sync_updates.append((str(client.id), "DA Qs Done", "TRUE"))
            elif da_done is False:
                sync_updates.append((str(client.id), "DA Qs Done", "FALSE"))
            if eval_done is True:
                sync_updates.append((str(client.id), "EVAL Qs Done", "TRUE"))
            elif eval_done is False:
                sync_updates.append((str(client.id), "EVAL Qs Done", "FALSE"))
            if da_sent is True:
                sync_updates.append((str(client.id), "DA Qs Sent", "TRUE"))
            elif da_sent is False:
                sync_updates.append((str(client.id), "DA Qs Sent", "FALSE"))
            if eval_sent is True:
                sync_updates.append((str(client.id), "EVAL Qs Sent", "TRUE"))
            elif eval_sent is False:
                sync_updates.append((str(client.id), "EVAL Qs Sent", "FALSE"))
        batch_update_punch_list(config, sync_updates)
        logger.info(f"Updated {len(sync_updates)} cell(s) on the punch list")
        return

    if debug_batteries:
        services, config = load_config()
        rules = get_questionnaire_rules(config)
        eval_dates = get_most_recent_eval_appointment_dates(config)
        clients_raw, _ = get_previous_clients(config, failed=False)
        clients_with_qs = validate_questionnaires(clients_raw)
        logger.info(
            f"Analyzing battery state for {len(clients_with_qs)} clients (verbose)"
        )
        sync_preview: list[tuple[str, str, str]] = []
        for client in clients_with_qs.values():
            eval_date = eval_dates.get(client.id)
            da_done, eval_done = check_battery_completeness(
                client, rules, verbose=True, most_recent_eval_date=eval_date
            )
            da_sent, eval_sent = check_battery_sent(
                client, rules, verbose=True, most_recent_eval_date=eval_date
            )
            if da_done is True:
                sync_preview.append((str(client.id), "DA Qs Done", "TRUE"))
            elif da_done is False:
                sync_preview.append((str(client.id), "DA Qs Done", "FALSE"))
            if eval_done is True:
                sync_preview.append((str(client.id), "EVAL Qs Done", "TRUE"))
            elif eval_done is False:
                sync_preview.append((str(client.id), "EVAL Qs Done", "FALSE"))
            if da_sent is True:
                sync_preview.append((str(client.id), "DA Qs Sent", "TRUE"))
            elif da_sent is False:
                sync_preview.append((str(client.id), "DA Qs Sent", "FALSE"))
            if eval_sent is True:
                sync_preview.append((str(client.id), "EVAL Qs Sent", "TRUE"))
            elif eval_sent is False:
                sync_preview.append((str(client.id), "EVAL Qs Sent", "FALSE"))
        logger.info(
            f"Punch list sync preview — {len(sync_preview)} cell(s) would change:"
        )
        for cid, col, val in sync_preview:
            logger.info(f"  client {cid}: {col} = {val}")
        return

    # qreceive is cron-run multiple times a day, but texts/admin emails should
    # only go out once daily — the 1pm run is the designated send window.
    # Other runs still check/update status, just without notifying anyone.
    start_hour = datetime.now().hour
    is_send_time = start_hour == 13
    send_texts = (is_send_time or force_send) and not dry_run

    if dry_run:
        logger.warning(
            "DRY RUN - no texts, punch list updates, or reminder DB writes will occur."
        )
    if not is_send_time and not force_send and not dry_run:
        logger.info(
            f"Current hour is {datetime.now().hour} — texts will not be sent outside the 1pm window. Use --force-send to override."
        )
    if force_send:
        logger.warning("--force-send active — sending texts regardless of time.")
    if skip_failures:
        logger.warning("SKIP FAILURES - failure checks and reminders will be skipped.")

    services, config = load_config()

    task_label = (
        "Questionnaire reminders"
        if is_send_time or force_send
        else "Checking questionnaires"
    )
    task = start_task(config, "questionnaire_reminders", task_label)
    if task is None:
        logger.info(
            "Skipping run: a previous questionnaire reminders run is still in progress."
        )
        return

    openphone = OpenPhone(config, services)
    rules = get_questionnaire_rules(config)
    eval_dates = get_most_recent_eval_appointment_dates(config)
    email_info: AdminEmailInfo = {
        "ignoring": [],
        "failed": [],
        "call": [],
        "completed": [],
        "errors": [],
    }

    try:
        clients, failed_clients = get_previous_clients(config, True)
        if clients is None:
            logger.critical("Failed to get previous clients")
            task.fail("Failed to get previous clients")
            return

        clients = validate_questionnaires(clients)
        all_clients_with_qs = dict(clients)  # unfiltered, used for end-of-run sync
        clients = filter_inactive_and_not_pending(clients)

        email_info["completed"], email_info["errors"] = check_questionnaires(
            config, clients, services, dry_run=dry_run
        )
        task.progress(1, 3, detail="checked questionnaire completions")

        driver = None
        if not skip_failures:
            driver = initialize_selenium()
            check_failures(config, services, driver, failed_clients)
        task.progress(2, 3, detail="checked failures")

        clients, failed_clients = get_previous_clients(config, failed=True)
        all_clients_raw = dict(clients)

        messages_sent: list[
            tuple[FailedClientFromDB | ClientWithQuestionnaires, str, str | None]
        ] = []
        numbers_sent = []

        completed_ids = {c.id for c in email_info["completed"]}
        if failed_clients and not skip_failures:
            for client in failed_clients.values():
                if any(
                    failure["reason"] in ["portal not opened", "docs not signed"]
                    for failure in client.failure
                ):
                    if client.language != "English":
                        logger.info(
                            f"{client.fullName} doesn't speak English, skipping"
                        )
                        continue
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

                    if client.autismStop:
                        logger.warning(f"{client.fullName} has autism stop, skipping")
                        continue

                    if client.pause:
                        logger.warning(f"{client.fullName} has been paused, skipping")
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

                    # 3 reminders sent and 3+ days of silence since the last one: hand
                    # off to a human to call instead of texting a 4th time.
                    if (
                        reminded_count == 3
                        and last_reminded_distance >= 3
                        and client.id not in completed_ids
                    ):
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
                                if send_texts:
                                    try:
                                        assert driver is not None
                                        resend_portal_invite(
                                            driver, services, str(client.id)
                                        )
                                    except Exception as e:
                                        logger.error(
                                            f"Failed to resend invite for {client.fullName}: {e}"
                                        )
                                        email_info["failed"].append(
                                            (
                                                client,
                                                "Failed to resend invite (possibly never initially invited?)",
                                            )
                                        )
                                        continue
                                elif dry_run:
                                    logger.info(
                                        f"[DRY RUN] Would resend portal invite for {client.fullName}"
                                    )

                            message = build_failure_message(config, client)
                            # Redundant failsafe to super ensure we don't text people a message that just says "None"
                            if not message:
                                logger.error(
                                    f"Failed to build message for {client.fullName}"
                                )
                                continue

                            if send_texts:
                                try:
                                    attempt_text = openphone.send_text(
                                        message, client.phoneNumber, mark_done=True
                                    )

                                    if attempt_text and "id" in attempt_text:
                                        numbers_sent.append(client.phoneNumber)
                                        messages_sent.append(
                                            (client, attempt_text["id"], reason)
                                        )
                                        try:
                                            log_questionnaire_msg(
                                                config,
                                                client.id,
                                                attempt_text["id"],
                                                is_failure_reminder=True,
                                                failure_reason=reason,
                                            )
                                        except Exception as log_err:
                                            logger.error(
                                                f"Failed to log automated message: {log_err}"
                                            )

                                    else:
                                        logger.error(
                                            f"Failed to send message to {client.fullName}"
                                        )
                                        email_info["failed"].append(
                                            (client, "Failed to send text request")
                                        )
                                except InvalidPhoneNumberError as e:
                                    logger.error(
                                        f"Invalid phone number for {client.fullName}: {e}"
                                    )
                                    email_info["failed"].append(
                                        (
                                            client,
                                            f"Invalid phone number: {client.phoneNumber}",
                                        )
                                    )
                                except NotEnoughCreditsError:
                                    logger.critical(
                                        "Aborting all further message sends due to insufficient credits."
                                    )
                                    email_info["errors"].append(
                                        "OpenPhone API needs more credits to send messages."
                                    )
                                    break
                            elif dry_run:
                                logger.info(
                                    f"[DRY RUN] Would text {client.fullName} ({client.phoneNumber}):\n{message}"
                                )

        if clients:
            clients = validate_questionnaires(clients)
            for client in clients.values():
                done = all_questionnaires_done(client)

                if check_if_ignoring(client):
                    logger.warning(f"{client.fullName} is being ignored.")
                    email_info["ignoring"].append(client)
                    continue

                if any(client.fullName in error for error in email_info["errors"]):
                    logger.warning(f"{client.fullName} has an error, skipping")
                    continue

                if client.autismStop:
                    logger.warning(f"{client.fullName} has autism stop, skipping")
                    continue

                if client.pause:
                    logger.warning(f"{client.fullName} has been paused, skipping")
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
                        and should_send_reminder(
                            most_recent_q["reminded"], last_reminded_distance
                        )
                    ):
                        if (
                            most_recent_q["reminded"] == 2
                            and most_recent_q["sent"] is not None
                        ):
                            has_replied = openphone.has_client_replied(
                                client.phoneNumber, since=most_recent_q["sent"]
                            )
                            if has_replied:
                                logger.info(
                                    f"{client.fullName} has replied since questionnaires were sent — skipping final reminder, setting questionnaires to IGNORING"
                                )
                                for q in client.questionnaires:
                                    if q["status"] in (
                                        "PENDING",
                                        "POSTDA_PENDING",
                                        "POSTEVAL_PENDING",
                                    ):
                                        q["status"] = "IGNORING"
                                if not dry_run:
                                    update_questionnaires_in_db(config, [client])
                                else:
                                    logger.info(
                                        f"[DRY RUN] Would set {client.fullName}'s questionnaires to IGNORING"
                                    )
                                email_info["ignoring"].append(client)
                                continue

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

                        if send_texts:
                            try:
                                attempt_text = openphone.send_text(
                                    message, client.phoneNumber, mark_done=True
                                )

                                if attempt_text and "id" in attempt_text:
                                    numbers_sent.append(client.phoneNumber)
                                    messages_sent.append(
                                        (client, attempt_text["id"], None)
                                    )
                                    try:
                                        log_questionnaire_msg(
                                            config,
                                            client.id,
                                            attempt_text["id"],
                                            is_failure_reminder=False,
                                        )
                                    except Exception as log_err:
                                        logger.error(
                                            f"Failed to log automated message: {log_err}"
                                        )
                                else:
                                    logger.error(
                                        f"Failed to send message to {client.fullName}"
                                    )
                                    email_info["failed"].append(
                                        (client, "Failed to send text request")
                                    )
                            except InvalidPhoneNumberError as e:
                                logger.error(
                                    f"Invalid phone number for {client.fullName}: {e}"
                                )
                                email_info["failed"].append(
                                    (
                                        client,
                                        f"Invalid phone number: {client.phoneNumber}",
                                    )
                                )
                            except NotEnoughCreditsError:
                                logger.critical(
                                    "Aborting all further message sends due to insufficient credits."
                                )
                                email_info["errors"].append(
                                    "OpenPhone API needs more credits to send messages."
                                )
                                break
                        elif dry_run:
                            logger.info(
                                f"[DRY RUN] Would text {client.fullName} ({client.phoneNumber}):\n{message}"
                            )
                elif client in email_info["completed"]:
                    logger.info(
                        f"{client.fullName} completed all questionnaires — punchlist will be synced at end of run"
                    )

        referral_msg = "This is Driftwood Evaluation Center. We have received your referral. We are managing a very large amount of patients and will reach out to you as soon as we can. Thank you!"
        referral_messages_sent: list[tuple[ClientFromDB, str]] = []

        if send_texts or dry_run:
            cutoff_date = date.today() - timedelta(days=1)
            sent_referral_ids = get_sent_referral_client_ids(config)
            new_clients = [
                c
                for c in all_clients_raw.values()
                if c.addedDate is not None
                and c.addedDate >= cutoff_date
                and c.id not in sent_referral_ids
                and not c.autismStop
                and not c.pause
                and len(str(c.id)) != 5  # excludes shell clients (5-digit IDs)
            ]
            logger.info(
                f"Found {len(new_clients)} new client(s) to send referral message"
            )
            for client in new_clients:
                if not client.phoneNumber:
                    logger.warning(
                        f"{client.fullName} is a new client but has no phone number"
                    )
                    email_info["failed"].append(
                        (client, "New referral — no phone number")
                    )
                    continue
                if client.phoneNumber in numbers_sent:
                    logger.warning(
                        f"Already messaged {client.fullName} today, skipping referral msg"
                    )
                    continue
                logger.info(
                    f"Sending referral message to new client {client.fullName} (added {client.addedDate})"
                )
                if send_texts:
                    try:
                        attempt_text = openphone.send_text(
                            referral_msg, client.phoneNumber, mark_done=True
                        )
                        if attempt_text and "id" in attempt_text:
                            numbers_sent.append(client.phoneNumber)
                            referral_messages_sent.append((client, attempt_text["id"]))
                        else:
                            logger.error(
                                f"Failed to send referral msg to {client.fullName}"
                            )
                            email_info["failed"].append(
                                (client, "New referral — failed to send text request")
                            )
                    except InvalidPhoneNumberError as e:
                        logger.error(f"Invalid phone number for {client.fullName}: {e}")
                        email_info["failed"].append(
                            (
                                client,
                                f"New referral — invalid phone number: {client.phoneNumber}",
                            )
                        )
                    except NotEnoughCreditsError:
                        logger.critical(
                            "Aborting referral messages due to insufficient credits."
                        )
                        email_info["errors"].append(
                            "OpenPhone API needs more credits to send messages."
                        )
                        break
                elif dry_run:
                    logger.info(
                        f"[DRY RUN] Would send referral msg to {client.fullName} ({client.phoneNumber}):\n{referral_msg}"
                    )

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
                                or q["status"] == "POSTDA_PENDING"
                                or q["status"] == "POSTEVAL_PENDING"
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

        logger.info(
            f"Starting status check for {len(referral_messages_sent)} referral message(s)."
        )
        for client, message_id in referral_messages_sent:
            try:
                delivered = openphone.check_text_delivered(message_id)
                if delivered:
                    logger.success(
                        f"Delivered referral msg to {client.fullName} ({message_id})"
                    )
                    try:
                        log_referral_msg(config, client.id, message_id)
                    except Exception as log_err:
                        logger.error(
                            f"Failed to log referral msg for {client.fullName}: {log_err}"
                        )
                else:
                    logger.error(
                        f"Failed to deliver referral msg to {client.fullName} ({message_id})"
                    )
                    email_info["failed"].append(
                        (client, "New referral — did not deliver within timeout")
                    )
            except Exception as e:
                logger.error(
                    f"Error checking referral msg status for {client.fullName} ({message_id}): {e}"
                )

        logger.info("Syncing punchlist Qs Done and Qs Sent columns with DB state")
        sync_updates: list[tuple[str, str, str]] = []
        for client in all_clients_with_qs.values():
            eval_date = eval_dates.get(client.id)
            da_done, eval_done = check_battery_completeness(
                client, rules, verbose=dry_run, most_recent_eval_date=eval_date
            )
            da_sent, eval_sent = check_battery_sent(
                client, rules, verbose=dry_run, most_recent_eval_date=eval_date
            )

            if da_done is True:
                sync_updates.append((str(client.id), "DA Qs Done", "TRUE"))
            elif da_done is False:
                sync_updates.append((str(client.id), "DA Qs Done", "FALSE"))

            if eval_done is True:
                sync_updates.append((str(client.id), "EVAL Qs Done", "TRUE"))
            elif eval_done is False:
                sync_updates.append((str(client.id), "EVAL Qs Done", "FALSE"))

            if da_sent is True:
                sync_updates.append((str(client.id), "DA Qs Sent", "TRUE"))
            elif da_sent is False:
                sync_updates.append((str(client.id), "DA Qs Sent", "FALSE"))

            if eval_sent is True:
                sync_updates.append((str(client.id), "EVAL Qs Sent", "TRUE"))
            elif eval_sent is False:
                sync_updates.append((str(client.id), "EVAL Qs Sent", "FALSE"))

        if not dry_run:
            batch_update_punch_list(config, sync_updates)
        else:
            for client_id_str, col, val in sync_updates:
                logger.info(
                    f"[DRY RUN] Would set {col}={val} for client {client_id_str}"
                )

    except Exception as e:
        error_message = f"An unhandled exception occurred during the run: {e}"
        logger.exception("Unhandled exception occurred during the run")
        email_info["errors"].append(error_message)
        task.fail(error_message)
        raise
    else:
        task.complete(detail=f"{len(email_info['completed'])} completed")

    finally:
        if is_send_time or force_send:
            earlier_runs = _load_pending_email()
            all_infos = [*earlier_runs, email_info]
            merged_info = _merge_email_infos(all_infos) if earlier_runs else email_info
            admin_email_text, admin_email_html = build_admin_email(merged_info)
            if admin_email_text != "":
                if not dry_run:
                    try:
                        send_gmail(
                            message_text=admin_email_text,
                            subject=f"Receive Run for {datetime.today().strftime('%a, %b')} {datetime.today().day}",
                            to_addr=",".join(config.qreceive_emails),
                            from_addr=config.automated_email,
                            html=admin_email_html,
                        )
                    except Exception:
                        logger.exception("Failed to send the admin email")
                    PENDING_EMAIL_PATH.unlink(missing_ok=True)
                else:
                    logger.info(
                        f"[DRY RUN] Would send admin email:\n{admin_email_text}"
                    )
        else:
            admin_email_text, admin_email_html = build_admin_email(email_info)
            if not dry_run and admin_email_text != "":
                _save_pending_email(email_info)


if __name__ == "__main__":
    app()
