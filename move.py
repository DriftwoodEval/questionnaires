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


previous_clients = utils.get_previous_clients()

if previous_clients:
    for client_id, client_data in previous_clients.items():
        utils.insert_basic_client(
            config,
            re.sub(r"C0+", "", client_id),
            client_data["asana"],
            client_data["birthdate"],
            client_data["firstname"],
            client_data["lastname"],
            client_data["check"],
            not client_data["english"],
            client_data["gender"],
            re.sub(r"\D", "", client_data["phone_number"]),
        )
        for questionnaire in client_data["questionnaires"]:
            utils.put_questionnaire_in_db(
                config,
                re.sub(r"C0+", "", client_id),
                questionnaire["link"],
                questionnaire["type"],
                client_data["sent_date"],
                questionnaire["done"],
            )
