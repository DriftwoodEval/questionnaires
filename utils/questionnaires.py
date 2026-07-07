import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

from dateutil.relativedelta import relativedelta
from loguru import logger
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from utils.custom_types import (
    ClientWithQuestionnaires,
    Config,
    Questionnaire,
    Services,
)
from utils.database import update_questionnaires_in_db
from utils.platforms.mhs import check_mhs_completed
from utils.platforms.novopsych import check_novopsych_completed
from utils.selenium import (
    initialize_selenium,
    save_screenshot_to_path,
    wait_for_page_load,
    wait_for_url_stability,
)

MAX_WORKERS = 5


def _in_current_session(client: ClientWithQuestionnaires, q: dict) -> bool:
    """Whether a questionnaire dict belongs to the client's current session.

    Clients can go inactive and later reactivate under the same id; on
    reactivation `sessionStartedAt` is stamped and prior questionnaires should
    stop counting toward completion/battery checks. `None` means the client
    has never been reset, so all history counts.
    """
    if not client.sessionStartedAt:
        return True
    q_date = q.get("sent") or q.get("updatedAt")
    if not q_date:
        return True
    if isinstance(q_date, date) and not isinstance(q_date, datetime):
        q_date = datetime.combine(q_date, datetime.min.time())
    return q_date >= client.sessionStartedAt


def all_questionnaires_done(client: ClientWithQuestionnaires) -> bool:
    """Check if all questionnaires for a given client are completed."""
    done_statuses = {"COMPLETED", "EXTERNAL"}
    return all(
        q["status"] in done_statuses
        for q in client.questionnaires
        if isinstance(q, dict)
        and q.get("status") != "ARCHIVED"
        and _in_current_session(client, q)
    )


def filter_inactive_and_not_pending(
    clients: dict[int, ClientWithQuestionnaires],
) -> dict[int, ClientWithQuestionnaires]:
    """Filter out clients that are inactive and don't have pending, rescheduled, or ignoring questionnaires."""
    return {
        client.id: client
        for client in clients.values()
        if client.status is True
        and any(
            q.get("status")
            in [
                "PENDING",
                # "SPANISH",
                "POSTDA_PENDING",
                "POSTEVAL_PENDING",
                "IGNORING",
                "RESCHEDULED",
            ]
            for q in client.questionnaires
            if isinstance(q, dict) and _in_current_session(client, q)
        )
    }


def check_if_ignoring(client: ClientWithQuestionnaires) -> bool:
    """Check if any questionnaire for the given client is being ignored."""
    return any(
        q["status"] == "RESCHEDULED" or q["status"] == "IGNORING"
        for q in client.questionnaires
        if isinstance(q, dict)
    )


def generate_screenshot_filename(status: str, q_type: str, url: str) -> str:
    """Creates a filename for a screenshot based on information about the questionnaire."""
    parsed = urlparse(url)

    # Clean the host (e.g., 'qosa.pearsonassessments.com' -> 'pearsonassessments')
    host_parts = parsed.netloc.split(".")
    domain = host_parts[-2] if len(host_parts) > 1 else host_parts[0]

    path_clean = re.sub(r"[^\w\-]+", "_", parsed.path.strip("/"))
    query_clean = re.sub(r"[^\w\-]+", "_", parsed.query.strip())

    url_identity = "_".join(filter(None, [path_clean, query_clean]))
    if not url_identity:
        url_identity = "unknown"

    safe_type = re.sub(r"[^\w\-]+", "_", q_type)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    return f"{status.upper()}_{safe_type}_{domain}_{url_identity}_{timestamp}.png"


def save_screenshot_deduped(
    driver: WebDriver, screenshots_dir: Path, filename: str
) -> None:
    """Save a screenshot, replacing any prior screenshot with the same status/type/questionnaire identity."""
    # Filename format: {STATUS}_{type}_{domain}_{url_identity}_{YYYYMMDD}_{HHMMSS}.png
    # Strip the trailing _YYYYMMDD_HHMMSS (16 chars) + .png (4 chars) to get the identity prefix.
    stem = filename[:-4]  # strip .png
    prefix = stem[:-16]  # strip _YYYYMMDD_HHMMSS
    for old in screenshots_dir.glob(f"{prefix}_*.png"):
        old.unlink()
        logger.debug(f"Removed old screenshot: {old.name}")
    save_screenshot_to_path(driver, screenshots_dir / filename)


def check_q_done(driver: WebDriver, q_link: str, q_type: str) -> bool:
    """Checks questionnaire completion status and captures evidence."""
    url_patterns = {
        "ASRS (2-5 Years)": "/asrs_web/",
        "ASRS (6-18 Years)": "/asrs_web/",
        "Conners EC": "/CEC/",
        "Conners 4": "/conners4",
        "DP-4": ["respondent.wpspublish.com", "hub-respondent.wpspublish.com"],
    }

    raw_completion_texts = {
        "mhs.com": [
            "Thank you for completing",
            "Gracias por contestar",
            "This link has already been used",
            "We have received your answers",
            "Hemos recibido sus respuestas",
        ],
        "pearsonassessments.com": [
            "Test Completed!",
            "¡Prueba completada!",
        ],
        "wpspublish.com": [
            "This assessment is not available at this time",
            "Esta evaluación no está disponible en este momento",
        ],
    }

    completion_xpaths = {
        host: " | ".join(f"//*[contains(text(), '{text}')]" for text in texts)
        for host, texts in raw_completion_texts.items()
    }

    wait = WebDriverWait(driver, 15)

    final_url = ""
    parsed_url = urlparse(q_link)
    link_host = parsed_url.netloc

    def capture_outcome(status: str):
        filename = generate_screenshot_filename(status, q_type, q_link)
        save_screenshot_deduped(driver, Path("logs/screenshots"), filename)

    try:
        driver.get(q_link)
        final_url = wait_for_url_stability(driver)

        if not wait_for_page_load(driver):
            return False

        if q_type in url_patterns:
            expected = url_patterns[q_type]
            patterns = [expected] if isinstance(expected, str) else expected

            if not any(pattern in final_url for pattern in patterns):
                logger.warning(
                    f"URL mismatch: Expected one of {patterns} in URL for type '{q_type}', but got '{final_url}'"
                )

        for host_key, xpath in completion_xpaths.items():
            if host_key in link_host:
                logger.info(f"Checking {host_key} completion for {q_link}")
                wait.until(ec.presence_of_element_located((By.XPATH, xpath)))
                logger.info(f"Completion found for {q_link}: {xpath}")
                capture_outcome("COMPLETED")
                return True

        logger.warning(f"Unknown or unsupported questionnaire host in link: {q_link}")
        capture_outcome("UNKNOWN_HOST")
        return False

    except (TimeoutException, NoSuchElementException):
        logger.info(
            f"Questionnaire at {q_link} is likely not completed (Timeout waiting for completion message)."
        )
        capture_outcome("INCOMPLETE")
        return False

    except WebDriverException as e:
        if "Read timed out" in str(e):
            raise
        logger.exception(f"WebDriver error checking questionnaire at {q_link}")
        capture_outcome("WEBDRIVER_ERROR")
        return False

    except Exception:
        logger.exception(f"{q_link}")
        capture_outcome("UNKNOWN_ERROR")
        raise


def check_questionnaires(
    config: Config,
    clients: dict[int, ClientWithQuestionnaires],
    services: Services,
    dry_run: bool = False,
) -> tuple[
    list[ClientWithQuestionnaires],
    list[str],
]:
    """Check if all questionnaires for the given clients are completed using parallel execution."""
    if not clients:
        return [], []

    # Get the initially completed clients to track progress
    initially_completed_ids = {
        client_id
        for client_id, client in clients.items()
        if all_questionnaires_done(client)
    }

    tasks = []
    for client in clients.values():
        if client.id in initially_completed_ids:
            logger.info(f"{client.fullName} has already completed their questionnaires")
            continue

        for questionnaire in client.questionnaires:
            if questionnaire["status"] == "COMPLETED":
                logger.info(
                    f"{client.fullName}'s {questionnaire['questionnaireType']} is already done"
                )
                continue
            if questionnaire["status"] == "ARCHIVED":
                logger.info(
                    f"{client.fullName}'s {questionnaire['questionnaireType']} is archived"
                )
                continue
            if not questionnaire["link"]:
                logger.warning(
                    f"No link found for {client.fullName}'s {questionnaire['questionnaireType']}"
                )
                continue
            tasks.append((client, questionnaire))

    if not tasks:
        return [], []

    updated_clients_set = set()
    error_clients: list[str] = []

    def _check_single_q(task):
        client, questionnaire = task
        logger.info(
            f"Checking {client.fullName}'s {questionnaire['questionnaireType']}"
        )
        for attempt in range(3):
            try:
                q_driver = initialize_selenium()
                try:
                    if questionnaire["questionnaireType"] == "CAT-Q":
                        is_done = check_novopsych_completed(
                            q_driver,
                            services,
                            client.firstName,
                            client.lastName,
                        )
                        if is_done:
                            filename = generate_screenshot_filename(
                                "COMPLETED_NOVOPSYCH",
                                questionnaire["questionnaireType"],
                                questionnaire["link"],
                            )
                            save_screenshot_deduped(
                                q_driver, Path("logs/screenshots"), filename
                            )
                    else:
                        is_done = check_q_done(
                            q_driver,
                            questionnaire["link"],
                            questionnaire["questionnaireType"],
                        )

                        if (
                            not is_done
                            and questionnaire["link"]
                            and "mhs.com" in questionnaire["link"]
                        ):
                            is_done = check_mhs_completed(
                                q_driver,
                                services,
                                client.id,
                                questionnaire["questionnaireType"],
                            )
                            if is_done:
                                filename = generate_screenshot_filename(
                                    "COMPLETED_MHS_PORTAL",
                                    questionnaire["questionnaireType"],
                                    questionnaire["link"],
                                )
                                save_screenshot_deduped(
                                    q_driver, Path("logs/screenshots"), filename
                                )

                    if is_done:
                        questionnaire["status"] = "COMPLETED"
                        logger.info(
                            f"{client.fullName}'s {questionnaire['questionnaireType']} is COMPLETED"
                        )
                        return client.id, True
                    logger.warning(
                        f"{client.fullName}'s {questionnaire['questionnaireType']} is {questionnaire['status']}"
                    )
                    return client.id, False
                finally:
                    q_driver.quit()
            except WebDriverException as e:
                if "Read timed out" in str(e) and attempt < 2:
                    logger.warning(
                        f"Timeout checking {client.fullName}'s {questionnaire['questionnaireType']}, retrying (attempt {attempt + 2}/3)"
                    )
                    continue
                logger.error(
                    f"Error checking questionnaires for {client.fullName}: {e}"
                )
                return client.id, e
            except Exception as e:
                logger.error(
                    f"Error checking questionnaires for {client.fullName}: {e}"
                )
                return client.id, e
        raise RuntimeError("unreachable")

    workers = 1 if dry_run else MAX_WORKERS
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(_check_single_q, tasks))

    for client_id, result in results:
        if isinstance(result, bool):
            if result:
                updated_clients_set.add(client_id)
        else:
            error_clients.append(f"{clients[client_id].fullName}: {result}")

    updated_clients = [clients[cid] for cid in updated_clients_set]
    if updated_clients:
        update_questionnaires_in_db(config, updated_clients)

    # Return clients that became completed in this run
    completed_clients = []
    for client in updated_clients:
        if all_questionnaires_done(client):
            logger.success(f"{client.fullName} has completed all questionnaires")
            completed_clients.append(client)

    return completed_clients, error_clients


def get_most_recent_not_done(
    client: ClientWithQuestionnaires,
) -> Questionnaire | None:
    """Get the most recent questionnaire that is still PENDING, POSTDA_PENDING, or POSTEVAL_PENDING from the given client by taking max of q["sent"]."""
    pending_and_sent = (
        q
        for q in client.questionnaires
        if (
            q["status"] == "PENDING"
            or q["status"] == "POSTDA_PENDING"
            or q["status"] == "POSTEVAL_PENDING"
            # or q["status"] == "SPANISH"
        )
        and q["sent"] is not None
        and _in_current_session(client, q)
    )

    return max(pending_and_sent, key=lambda q: cast(date, q["sent"]), default=None)


def _resolve_wanted_diagnoses(asd_adhd: str | None) -> set[str]:
    """Convert a client's asdAdhd field to a set of diagnosis strings for rule matching."""
    if not asd_adhd:
        return {"ASD", "ADHD"}
    normalized = "ASD+ADHD" if asd_adhd == "Both" else asd_adhd
    diagnoses: set[str] = set()
    if "ASD" in normalized:
        diagnoses.add("ASD")
    if "ADHD" in normalized:
        diagnoses.add("ADHD")
    return diagnoses or {"ASD", "ADHD"}


def check_battery_sent(
    client: ClientWithQuestionnaires,
    rules: list[dict],
    verbose: bool = False,
) -> tuple[bool | None, bool | None]:
    """Check if DA and EVAL questionnaire batteries have been sent for a client.

    Returns (da_sent, eval_sent) where each is:
      True  — all required types exist and all have status != JUST_ADDED and != ARCHIVED
      False — any required type is missing, JUST_ADDED, or ARCHIVED
      None  — no applicable rules for this battery (don't update the column)
    """
    age_in_years = relativedelta(date.today(), client.dob).years

    age_filtered = [r for r in rules if r["minAge"] <= age_in_years <= r["maxAge"]]

    wanted_diagnoses = _resolve_wanted_diagnoses(client.asdAdhd)

    applicable = [
        r
        for r in age_filtered
        if (r["daeval"] == "DAEVAL" and r.get("diagnosis") is None)
        or (r["daeval"] != "DAEVAL" and r.get("diagnosis") in wanted_diagnoses)
    ]

    da_only_types: set[str] = set()
    eval_only_types: set[str] = set()
    daeval_types: set[str] = set()
    for rule in applicable:
        qs: list[str] = rule.get("questionnaires") or []
        if rule["daeval"] == "DA":
            da_only_types.update(qs)
        elif rule["daeval"] == "EVAL":
            eval_only_types.update(qs)
        elif rule["daeval"] == "DAEVAL":
            daeval_types.update(qs)

    unsent_statuses = {"JUST_ADDED", "ARCHIVED"}
    active_sent_types = {
        q["questionnaireType"]
        for q in client.questionnaires
        if q.get("status") not in unsent_statuses and _in_current_session(client, q)
    }

    da_ok = bool(da_only_types) and da_only_types.issubset(active_sent_types)
    eval_ok = bool(eval_only_types) and eval_only_types.issubset(active_sent_types)
    daeval_ok = bool(daeval_types) and daeval_types.issubset(active_sent_types)

    # DA: satisfied by DA rules alone, or by DAEVAL rules alone
    da_sent: bool | None = None
    if da_ok or daeval_ok:
        da_sent = True
    elif da_only_types:
        da_sent = False

    # EVAL: satisfied by EVAL rules alone, or by DAEVAL rules alone
    eval_sent: bool | None = None
    if eval_ok or daeval_ok:
        eval_sent = True
    elif eval_only_types:
        eval_sent = False

    if verbose:
        all_q_statuses = {
            q["questionnaireType"]: q["status"] for q in client.questionnaires
        }
        logger.debug(
            f"[battery-sent] {client.fullName} (ID:{client.id}) "
            f"asdAdhd={client.asdAdhd!r} age={age_in_years} wanted={wanted_diagnoses}"
        )
        logger.debug(
            f"  applicable rules ({len(applicable)}): "
            + ", ".join(
                f"[{r['daeval']}:{r['diagnosis']}]={r['questionnaires']}"
                for r in applicable
            )
        )
        logger.debug(
            f"  da_only={da_only_types} eval_only={eval_only_types} daeval={daeval_types}"
        )
        logger.debug(
            f"  da_ok={da_ok} eval_ok={eval_ok} daeval_ok={daeval_ok} | active={active_sent_types}"
        )
        logger.debug(f"  all_statuses={all_q_statuses}")
        if da_sent is False:
            logger.warning(f"  MISSING from DA: {da_only_types - active_sent_types}")
        if eval_sent is False:
            logger.warning(
                f"  MISSING from EVAL: {eval_only_types - active_sent_types}"
            )
        logger.info(f"  => da_sent={da_sent}  eval_sent={eval_sent}")

    return da_sent, eval_sent


def check_battery_completeness(
    client: ClientWithQuestionnaires,
    rules: list[dict],
    verbose: bool = False,
) -> tuple[bool | None, bool | None]:
    """Check if DA and EVAL questionnaire batteries are complete for a client.

    Returns (da_done, eval_done) where each is:
      True  — all required types for this battery are COMPLETED or EXTERNAL
      False — at least one required type is not done
      None  — no applicable rules for this battery (don't update the column)
    """
    age_in_years = relativedelta(date.today(), client.dob).years

    age_filtered = [r for r in rules if r["minAge"] <= age_in_years <= r["maxAge"]]

    wanted_diagnoses = _resolve_wanted_diagnoses(client.asdAdhd)

    applicable = [
        r
        for r in age_filtered
        if (r["daeval"] == "DAEVAL" and r.get("diagnosis") is None)
        or (r["daeval"] != "DAEVAL" and r.get("diagnosis") in wanted_diagnoses)
    ]

    da_only_types: set[str] = set()
    eval_only_types: set[str] = set()
    daeval_types: set[str] = set()
    for rule in applicable:
        qs: list[str] = rule.get("questionnaires") or []
        if rule["daeval"] == "DA":
            da_only_types.update(qs)
        elif rule["daeval"] == "EVAL":
            eval_only_types.update(qs)
        elif rule["daeval"] == "DAEVAL":
            daeval_types.update(qs)

    done_statuses = {"COMPLETED", "EXTERNAL"}
    completed_types = {
        q["questionnaireType"]
        for q in client.questionnaires
        if q.get("status") != "ARCHIVED"
        and q.get("status") in done_statuses
        and _in_current_session(client, q)
    }

    da_ok = bool(da_only_types) and da_only_types.issubset(completed_types)
    eval_ok = bool(eval_only_types) and eval_only_types.issubset(completed_types)
    daeval_ok = bool(daeval_types) and daeval_types.issubset(completed_types)

    da_done: bool | None = None
    if da_ok or daeval_ok:
        da_done = True
    elif da_only_types:
        da_done = False

    eval_done: bool | None = None
    if eval_ok or daeval_ok:
        eval_done = True
    elif eval_only_types:
        eval_done = False

    if verbose:
        logger.debug(
            f"[battery-done] {client.fullName} (ID:{client.id}) "
            f"asdAdhd={client.asdAdhd!r} age={age_in_years} wanted={wanted_diagnoses}"
        )
        logger.debug(
            f"  da_only={da_only_types} eval_only={eval_only_types} daeval={daeval_types}"
        )
        logger.debug(
            f"  da_ok={da_ok} eval_ok={eval_ok} daeval_ok={daeval_ok} | completed={completed_types}"
        )
        if da_done is False:
            logger.warning(f"  NOT DONE in DA: {da_only_types - completed_types}")
        if eval_done is False:
            logger.warning(f"  NOT DONE in EVAL: {eval_only_types - completed_types}")
        logger.info(f"  => da_done={da_done}  eval_done={eval_done}")

    return da_done, eval_done
