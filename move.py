import hashlib
import re
from datetime import datetime
from typing import Tuple

import mysql.connector

import shared_utils as utils

utils.log.basicConfig(
    level=utils.log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[utils.log.FileHandler("move.log"), utils.log.StreamHandler()],
    force=True,
)

services, config = utils.load_config()


def get_evaluator_npi(evaluator_email) -> str:
    db_connection, cursor = utils.get_db(config)
    sql = "SELECT npi FROM emr_evaluator WHERE email = %s"
    cursor.execute(sql, (evaluator_email,))
    npi = cursor.fetchone()
    db_connection.close()
    return npi[0] if npi else None  # type: ignore


def insert_basic_client(
    client_id: str,
    asana_id: str,
    dob,
    first_name,
    last_name,
    asd_adhd,
    interpreter,
    gender,
    phone_number,
):
    db_connection, cursor = utils.get_db(config)
    sql = """
        INSERT INTO `emr_client` (id, hash, asanaId, dob, firstName, lastName, fullName, asdAdhd, interpreter, gender, phoneNumber)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE id=id
    """

    values = (
        int(client_id),
        hashlib.sha256(str(client_id).encode("utf-8")).hexdigest(),
        asana_id if asana_id else None,
        dob,
        first_name,
        last_name,
        f"{first_name} {last_name}",
        asd_adhd,
        interpreter,
        gender,
        phone_number,
    )

    try:
        cursor.execute(sql, values)
        cursor.nextset()
        db_connection.commit()
    except mysql.connector.errors.IntegrityError as e:
        utils.log.error(e)

    db_connection.close()


def put_appointment_in_db(client_id, evaluator_email, date, appointment_type):
    db_connection, cursor = utils.get_db(config)

    # Check if the client already has an appointment of the given type
    check_sql = """
        SELECT COUNT(*) FROM emr_appointment
        WHERE clientId = %s AND type = %s
    """
    cursor.execute(check_sql, (int(client_id), appointment_type))
    count = cursor.fetchone()

    if count and count[0] > 0:  # type: ignore
        utils.log.info(
            f"Client {client_id} already has an appointment of type {appointment_type}."
        )
        db_connection.close()
        return None

    # Insert the new appointment
    sql = """
        INSERT INTO emr_appointment (
            clientId, evaluatorNpi, date, type
        ) VALUES (%s, %s, %s, %s)
    """

    values = (
        int(client_id),
        get_evaluator_npi(evaluator_email),
        date,
        appointment_type,
    )

    try:
        cursor.execute(sql, values)
        appointment_id = cursor.lastrowid
        cursor.nextset()
        db_connection.commit()
    except mysql.connector.errors.IntegrityError as e:
        utils.log.error(e)
        appointment_id = None

    db_connection.close()
    return appointment_id


def put_questionnaire_in_db(appointment_id, link, type, sent_date, completed):
    db_connection, cursor = utils.get_db(config)

    sql = """
        INSERT INTO emr_questionnaire (
            appointmentId, link, questionnaireType, sent, completed
        ) VALUES (%s, %s, %s, %s, %s)
    """

    values = (int(appointment_id), link, type, sent_date, completed)

    try:
        cursor.execute(sql, values)
        cursor.nextset()
        db_connection.commit()
    except mysql.connector.errors.IntegrityError as e:
        utils.log.error(e)

    db_connection.close()


previous_clients = utils.get_previous_clients()

if previous_clients:
    for client_id, client_data in previous_clients.items():
        insert_basic_client(
            re.sub(r"\D", "", client_id),
            client_data["asana"],
            client_data["birthdate"],
            client_data["firstname"],
            client_data["lastname"],
            client_data["check"],
            not client_data["english"],
            client_data["gender"],
            re.sub(r"\D", "", client_data["phone_number"]),
        )
        appointment_id = put_appointment_in_db(
            re.sub(r"\D", "", client_id),
            client_data["evaluator_email"],
            datetime.strptime(client_data["date"], "%Y/%m/%d").date(),
            client_data["daeval"],
        )
        if appointment_id:
            for questionnaire in client_data["questionnaires"]:
                put_questionnaire_in_db(
                    appointment_id,
                    questionnaire["link"],
                    questionnaire["type"],
                    client_data["sent_date"],
                    questionnaire["done"],
                )
