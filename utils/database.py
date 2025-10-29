import hashlib
from datetime import date
from typing import Literal, Optional
from urllib.parse import urlparse

import pymysql.cursors
from loguru import logger

from utils.types import (
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
    """Load previous clients from the database and a YAML file.

    Args:
        config (Config): The configuration object.
        failed (bool, optional): Whether to load failed clients. Defaults to False.
    """
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
            sql = "SELECT * FROM emr_client"
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
            except Exception as e:
                logger.error(
                    f"Failed to create ClientFromDB for ID {client_data.get('id', 'Unknown')}: {e}"
                )

    return prev_clients, failed_prev_clients


def get_failures_from_db(config: Config) -> dict[int, FailedClientFromDB]:
    """Get the failed clients from the database."""
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = "SELECT * FROM emr_failure"
            cursor.execute(sql)
            failures = cursor.fetchall()

            sql = "SELECT * FROM emr_client"
            cursor.execute(sql)
            clients = cursor.fetchall()

            sql = "SELECT * FROM emr_note"
            cursor.execute(sql)
            notes = cursor.fetchall()

            for failure in failures:
                for client in clients:
                    if failure["clientId"] == client["id"]:
                        client["failure"] = failure
            for note in notes:
                for client in clients:
                    if note["clientId"] == client["id"]:
                        client["note"] = note

    failed_clients = {}
    for client_data in clients:
        if "failure" in client_data and client_data["failure"]["reminded"] < 100:
            try:
                pydantic_client = FailedClientFromDB(**client_data)
                failed_clients[pydantic_client.id] = pydantic_client
            except Exception as e:
                logger.error(
                    f"Failed to create FailedClientFromDB for ID {client_data.get('id', 'Unknown')}: {e}"
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
        "PENDING", "COMPLETED", "IGNORING", "LANGUAGE", "TEACHER", "EXTERNAL"
    ],
):
    """Insert a questionnaire into the database.

    Args:
        config (Config): The configuration object.
        client_id (str): The client ID.
        link (str): The link of the questionnaire.
        qtype (str): The qtype of the questionnaire.
        sent_date: The date the questionnaire was sent.
        status (Literal["COMPLETED", "PENDING", "RESCHEDULED"]): The status of the questionnaire.

    Returns:
        None
    """
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
    status: Literal["PENDING", "COMPLETED", "IGNORING", "LANGUAGE", "TEACHER"],
):
    """Update a questionnaire in the database."""
    db_connection = get_db(config)

    with db_connection:
        with db_connection.cursor() as cursor:
            sql = """
                UPDATE `emr_questionnaire`
                SET status=%s
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
                        SET status=%s, reminded=%s, lastReminded=%s
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
    da_eval: Optional[Literal["DA", "EVAL", "DAEVAL"]] = None,
):
    """Adds the information given to the DB."""
    db_connection = get_db(config)
    with db_connection:
        with db_connection.cursor() as cursor:
            sql = "INSERT IGNORE INTO emr_failure (clientId, daEval, reason, failedDate) VALUES (%s, %s, %s, %s)"
            values = (client_id, da_eval, error, failed_date)
            cursor.execute(sql, values)
            db_connection.commit()


def update_failure_in_db(
    config: Config,
    client_id: int,
    reason: str,
    da_eval: Optional[Literal["DA", "EVAL", "DAEVAL"]] = None,
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

            sql += ", ".join(updates)
            sql += " WHERE clientId=%s AND reason=%s"
            values += (client_id, reason)

            cursor.execute(sql, values)
            db_connection.commit()
