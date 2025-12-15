import hashlib
from datetime import date
from typing import Dict, List, Literal, Optional
from urllib.parse import urlparse

import pymysql.cursors
from loguru import logger

from utils.custom_types import (
    Appointment,
    ClientFromDB,
    ClientWithQuestionnaires,
    Config,
    FailedClientFromDB,
)


def get_db(config: Config):
    """Connect to the database and return a connection object."""
    db_url = urlparse(config.database_url)
    connection = pymysql.connect(
        host=db_url.hostname,
        port=db_url.port or 3306,
        user=db_url.username,
        password=db_url.password or "",
        database=db_url.path[1:],
        cursorclass=pymysql.cursors.DictCursor,
    )
    return connection


def get_previous_clients(
    config: Config, failed: bool = False
) -> tuple[dict[int, ClientFromDB], dict[int, FailedClientFromDB]]:
    """Load previous clients from the database, excluding inactive clients."""
    logger.info(
        f"Loading previous clients from DB{' and failed clients' if failed else ''}"
    )
    failed_prev_clients = {}
    if failed:
        failed_prev_clients = get_failures_from_db(config)

    # Load clients from the database
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = "SELECT * FROM emr_client WHERE status IS NOT FALSE"
            cursor.execute(sql)
            clients = cursor.fetchall()

            sql = "SELECT * FROM emr_questionnaire"
            cursor.execute(sql)
            questionnaires = cursor.fetchall()
            for client in clients:
                # Add the questionnaires to each client
                client["questionnaires"] = [
                    questionnaire
                    for questionnaire in questionnaires
                    if questionnaire["clientId"] == client["id"]
                ]

    # Create a dictionary of clients with their IDs as keys
    prev_clients = {}
    if clients:
        for client_data in clients:
            try:
                pydantic_client = ClientFromDB(**client_data)
                prev_clients[pydantic_client.id] = pydantic_client
            except Exception:
                logger.exception(
                    f"Failed to create ClientFromDB for ID {client_data.get('id', 'Unknown')}"
                )

    return prev_clients, failed_prev_clients


def get_failures_from_db(config: Config) -> dict[int, FailedClientFromDB]:
    """Get the failed clients from the database."""
    db_connection = get_db(config)

    client_failures: Dict[int, List[Dict]] = {}
    client_notes: Dict[int, Dict] = {}

    with db_connection:
        with db_connection.cursor() as cursor:
            sql = "SELECT * FROM emr_failure"
            cursor.execute(sql)
            for failure in cursor.fetchall():
                client_id = failure["clientId"]
                if client_id not in client_failures:
                    client_failures[client_id] = []
                client_failures[client_id].append(failure)

            sql = "SELECT * FROM emr_note"
            cursor.execute(sql)
            for note in cursor.fetchall():
                client_notes[note["clientId"]] = note

            sql = "SELECT * FROM emr_client WHERE status IS NOT FALSE"
            cursor.execute(sql)
            clients_data = cursor.fetchall()

    failed_clients: Dict[int, FailedClientFromDB] = {}
    for client_data in clients_data:
        client_id = client_data["id"]
        failures = client_failures.get(client_id, [])
        note = client_notes.get(client_id)

        eligible_failures = [f for f in failures if f.get("reminded", 0) < 100]

        if eligible_failures:
            client_final_data = {
                **client_data,
                "failure": eligible_failures,
                "note": note,
            }

            try:
                pydantic_client = FailedClientFromDB.model_validate(client_final_data)
                failed_clients[client_id] = pydantic_client
            except Exception:
                logger.exception(
                    f"Failed to create FailedClientFromDB for ID {client_id}"
                )

    return failed_clients


def get_evaluator_npi(config: Config, evaluator_email) -> Optional[str]:
    """Get the NPI of an evaluator from the database.

    Args:
        config: The configuration.
        evaluator_email: The email address of the evaluator.

    Returns:
        The NPI of the evaluator, or None if not found.
    """
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = "SELECT npi FROM emr_evaluator WHERE email = %s"
            cursor.execute(sql, (evaluator_email))
            npi = cursor.fetchone()
            return npi["npi"] if npi else None


def get_all_evaluators_info(config: Config) -> dict[int, dict]:
    """Gets a map of NPI (int) to a dictionary containing all evaluator info."""
    evaluators_info = {}
    db_connection = get_db(config)

    try:
        with db_connection.cursor() as cursor:
            cursor.execute("SELECT * FROM emr_evaluator")
            results = cursor.fetchall()

            for row in results:
                npi = row.get("npi")
                if npi is not None:
                    evaluators_info[npi] = row

    except Exception:
        logger.exception(f"Failed to get all evaluators info")

    return evaluators_info


def get_appointments(
    config: Config, start_date: date, end_date: date
) -> Optional[list[Appointment]]:
    """Fetch appointments within the given date range and associated client names."""
    try:
        connection = get_db(config)
        with connection:
            with connection.cursor() as cursor:
                sql = """
                    SELECT
                        a.*,
                        c.fullName as clientName
                    FROM
                        emr_appointment a
                    LEFT JOIN emr_client c ON a.clientId = c.id
                    WHERE
                        a.startTime >= %s AND a.endTime <= %s + INTERVAL 1 DAY
                """
                cursor.execute(sql, (start_date, end_date))
                results = cursor.fetchall()

                appointments = []
                for row in results:
                    appointment = Appointment(**row)
                    appointments.append(appointment)

                return appointments
    except Exception:
        logger.exception("Failed to fetch appointments and associated client names.")
        return


def insert_basic_client(
    config: Config,
    client_id: str,
    dob,
    first_name: str,
    last_name: str,
    asd_adhd: str,
    gender: str,
    phone_number,
):
    """Insert a client into the database, using only the data from sending a questionnaire.

    Args:
        config (Config): The configuration object.
        client_id (str): The client ID.
        dob: The date of birth of the client.
        first_name (str): The first name of the client.
        last_name (str): The last name of the client.
        asd_adhd (str): The type of condition the client has (ASD, ADHD, or ASD+ADHD).
        gender (str): The gender of the client.
        phone_number: The phone number of the client.

    Returns:
        None
    """
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = """
                INSERT INTO `emr_client` (id, hash, dob, firstName, lastName, fullName, asdAdhd, gender, phoneNumber)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE id=id, asdAdhd=VALUES(asdAdhd)
            """

            values = (
                int(client_id),
                hashlib.sha256(str(client_id).encode("utf-8")).hexdigest(),
                dob,
                first_name,
                last_name,
                f"{first_name} {last_name}",
                "Both" if asd_adhd == "ASD+ADHD" else asd_adhd,
                gender,
                phone_number,
            )

            cursor.execute(sql, values)

        db_connection.commit()


def put_questionnaire_in_db(
    config: Config,
    client_id: str,
    link: str,
    qtype: str,
    sent_date: str,
    status: Literal[
        "PENDING",
        "COMPLETED",
        "IGNORING",
        "POSTEVAL_PENDING",
        "SPANISH",
        "LANGUAGE",
        "TEACHER",
        "EXTERNAL",
        "ARCHIVED",
        "JUST_ADDED",
    ],
):
    """Insert a questionnaire into the database."""
    db_connection = get_db(config)

    with db_connection:
        with db_connection.cursor() as cursor:
            sql = """
                INSERT INTO emr_questionnaire (
                    clientId, link, questionnaireType, sent, status
                ) VALUES (%s, %s, %s, %s, %s)
            """

            values = (int(client_id), link, qtype, sent_date, status)

            cursor.execute(sql, values)
        db_connection.commit()


def update_questionnaire_in_db(
    config: Config,
    client_id: str,
    qtype: str,
    sent_date: str,
    status: Literal[
        "PENDING",
        "COMPLETED",
        "IGNORING",
        "POSTEVAL_PENDING",
        "SPANISH",
        "LANGUAGE",
        "TEACHER",
        "EXTERNAL",
        "ARCHIVED",
        "JUST_ADDED",
    ],
):
    """Update a questionnaire in the database."""
    db_connection = get_db(config)

    with db_connection:
        with db_connection.cursor() as cursor:
            sql = """
                UPDATE `emr_questionnaire`
                SET status=%s, updated_at = NOW()
                WHERE clientId=%s AND sent=%s AND questionnaireType=%s
            """

            values = (status, int(client_id), sent_date, qtype)
            cursor.execute(sql, values)
        db_connection.commit()


def update_questionnaires_in_db(
    config: Config, clients: list[ClientWithQuestionnaires]
):
    """Update questionnaires in the database, setting status, reminded count, and last reminded date.

    Args:
        config (Config): The configuration object.
        clients (list[ClientWithQuestionnaires]): A list of ClientWithQuestionnaires objects.
    """
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            for client in clients:
                for questionnaire in client.questionnaires:
                    sql = """
                        UPDATE `emr_questionnaire`
                        SET status=%s, reminded=%s, lastReminded=%s, updated_at = NOW()
                        WHERE clientId=%s AND sent=%s AND questionnaireType=%s
                    """

                    values = (
                        questionnaire["status"],
                        questionnaire["reminded"],
                        questionnaire["lastReminded"],
                        client.id,
                        questionnaire["sent"],
                        questionnaire["questionnaireType"],
                    )

                    cursor.execute(sql, values)
                    db_connection.commit()


def add_failure_to_db(
    config: Config,
    client_id: int,
    error: str,
    failed_date: date,
    da_eval: Optional[Literal["DA", "EVAL", "DAEVAL", "Records"]] = None,
):
    """Adds the information given to the DB."""
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = """INSERT INTO emr_failure (clientId, daEval, reason, failedDate)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            daEval=VALUES(daEval),
            updated_at = NOW();
            """
            values = (client_id, da_eval, error, failed_date)
            cursor.execute(sql, values)
            db_connection.commit()


def update_failure_in_db(
    config: Config,
    client_id: int,
    reason: str,
    da_eval: Optional[Literal["DA", "EVAL", "DAEVAL", "Records"]] = None,
    resolved: Optional[bool] = None,
    failed_date: Optional[date] = None,
    reminded: Optional[int] = None,
    last_reminded: Optional[date] = None,
):
    """Updates the failure in the DB."""
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = "UPDATE emr_failure SET "
            values = ()

            updates = []
            if da_eval is not None:
                updates.append("daEval=%s")
                values += (da_eval,)

            if failed_date is not None:
                updates.append("failedDate=%s")
                values += (failed_date,)

            if resolved is True:
                updates.append("reminded=reminded + 100")
            elif reminded is not None:
                updates.append("reminded=%s")
                values += (reminded,)

            if last_reminded is not None:
                updates.append("lastReminded=%s")
                values += (last_reminded,)

            if not updates:
                updates.append("updated_at = NOW()")

            sql += ", ".join(updates)
            sql += " WHERE clientId=%s AND reason=%s"
            values += (client_id, reason)

            cursor.execute(sql, values)
            db_connection.commit()
