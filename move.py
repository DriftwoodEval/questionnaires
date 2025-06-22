import re
from datetime import datetime

import yaml

import shared_utils as utils

services, config = utils.load_config()


previous_clients = {}
with open("./previous_clients.yml", "r") as file:
    previous_clients = yaml.safe_load(file) or {}

previous_clients = {
    client_id: utils.ClientFromDB(
        id=int(re.sub(r"\D", "", client_id)),
        asanaId=client_data.get("asana", "") if client_data.get("asana") else "",
        dob=datetime.strptime(client_data["birthdate"], "%Y/%m/%d").date(),
        firstName=client_data["firstname"],
        lastName=client_data["lastname"],
        fullName=f"{client_data['firstname']} {client_data['lastname']}",
        asdAdhd=client_data["check"],
        gender=client_data["gender"],
        phoneNumber=re.sub(r"\D", "", client_data["phone_number"]),
        questionnaires=[
            utils.Questionnaire(
                questionnaireType=questionnaire["type"],
                link=questionnaire["link"],
                sent=datetime.strptime(client_data["sent_date"], "%Y/%m/%d").date(),
                status="RESCHEDULED"
                if client_data["date"] == "Reschedule"
                else ("COMPLETED" if questionnaire["done"] else "PENDING"),
                reminded=0,
            )
            for questionnaire in client_data["questionnaires"]
        ],
    )
    for client_id, client_data in previous_clients.items()
}

if previous_clients:
    for client_id, client_data in previous_clients.items():
        utils.insert_basic_client(
            config,
            re.sub(r"C0+", "", client_id),
            client_data.asanaId or "",
            client_data.dob,
            client_data.firstName,
            client_data.lastName,
            client_data.asdAdhd or "",
            client_data.gender or "",
            client_data.phoneNumber,
        )
        if client_data.questionnaires:
            for questionnaire in client_data.questionnaires:
                utils.put_questionnaire_in_db(
                    config,
                    re.sub(r"C0+", "", client_id),
                    questionnaire["link"],
                    questionnaire["questionnaireType"],
                    questionnaire["sent"],
                    questionnaire["status"],
                )
