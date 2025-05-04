import logging
from datetime import date, datetime
from time import sleep

import requests

import shared_utils as utils

utils.log.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("qreceive.log"), logging.StreamHandler()],
    force=True,
)

services, config = utils.load_config()


def get_text_info(message_id):
    sleep(0.2)
    url = f"https://api.openphone.com/v1/messages/{message_id}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": services["openphone"]["key"],
    }
    response = requests.get(url, headers=headers)
    response_data = response.json().get("data")
    return response_data


def send_text_and_ensure(
    message,
    to_number,
    from_number=services["openphone"]["main_number"],
    user_blame=services["openphone"]["users"][config["name"].lower()]["id"],
):
    attempt_text = utils.send_text(
        config, services, message, to_number, from_number, user_blame
    )
    if not attempt_text:
        utils.log.warning(f"Possibly failed to send message {message} to {to_number}")
        return False
    message_id = attempt_text["id"]
    for i in range(3):
        sleep_time = 2**i
        sleep(sleep_time)
        message_info = get_text_info(message_id)
        message_status = message_info["status"]
        utils.log.info(f"Message status on attempt {i + 1}: {message_status}")
        if message_status == "delivered":
            return True
    else:
        utils.log.warning(f"Failed to send message {message} to {to_number}")
        return False


def check_appointment_distance(appointment: date):
    today = date.today()
    delta = appointment - today
    return delta.days


def build_message(config, client, distance):
    link_count = len(client.get("questionnaires", []))
    if distance < 0:
        if distance == -1:
            distance_sentence = "(yesterday)"
        else:
            distance_sentence = f"({abs(distance)} days ago)"
    else:
        if distance == 1:
            distance_sentence = "(TOMORROW)"
        else:
            distance_sentence = f"(in {distance} days)"

    if not client.get("reminded"):
        message = f"Hello, this is {config['name']} from Driftwood Evaluation Center. Please be on the lookout for an email from the patient portal Therapy Appointment as there {'is a questionnaire' if link_count == 1 else 'are questionnaires'} in your messages for your appointment on {utils.format_appointment(client)} {distance_sentence}. Please let me know if you have any questions. Thank you for your time."
    else:
        message = f"Hello, this is {config['name']} with Driftwood Evaluation Center. It appears your questionnaire{'' if link_count == 1 else 's'} for your appointment on {utils.format_appointment(client)} {distance_sentence} {'is' if link_count == 1 else 'are'} still incomplete. You can find {'it' if link_count == 1 else 'them'} in your messages in the patient portal at https://portal.therapyappointment.com. Please complete {'it' if link_count == 1 else 'them'} as soon as possible as we will be unable to effectively evaluate if {'it is' if link_count == 1 else 'they are'} incomplete."
    return message


def main():
    projects_api = utils.init_asana(services)
    driver, actions = utils.initialize_selenium()
    utils.check_questionnaires(driver, config, services)
    clients = utils.get_previous_clients()
    if clients:
        numbers_sent = []
        for id in clients:
            client = clients[id]
            utils.mark_links_in_asana(projects_api, client, services, config)

            distance = check_appointment_distance(
                datetime.strptime(client["date"], "%Y/%m/%d").date()
            )
            done = utils.all_questionnaires_done(client)
            utils.log.info(
                f"{client['firstname']} {client['lastname']} is {distance} days away and {'done' if done else 'not done'}"
            )
            if not done:
                already_messaged_today = client["phone_number"] in numbers_sent
                if already_messaged_today:
                    utils.log.info(
                        f"Already messaged {client['firstname']} {client['lastname']} at {client['phone_number']} today"
                    )
                if distance >= 5 and distance % 3 == 2 and not already_messaged_today:
                    utils.log.info(
                        f"Sending reminder TO {client['firstname']} {client['lastname']}"
                    )
                    message = build_message(config, client, distance)
                    message_sent = send_text_and_ensure(message, client["phone_number"])
                    if message_sent:
                        client["reminded"] = client.get("reminded", 0) + 1
                        numbers_sent.append(client["phone_number"])
                        utils.sent_reminder_asana(config, projects_api, client)
                    else:
                        utils.send_text(
                            config,
                            services,
                            f"Message failed to deliver to {client['firstname']} {client['lastname']}.",
                            services["openphone"]["users"][config["name"].lower()][
                                "phone"
                            ],
                        )
                elif 0 <= distance < 5:
                    utils.log.info(
                        f"Sending reminder ABOUT {client['firstname']} {client['lastname']}"
                    )
                    utils.send_text(
                        config,
                        services,
                        f"{client['firstname']} {client['lastname']} has an appointment on {(utils.format_appointment(client))} (in {distance} days) and hasn't done everything, please call them.",
                        services["openphone"]["users"][config["name"].lower()]["phone"],
                    )
                elif distance < 0 and distance % 3 == 2 and not already_messaged_today:
                    utils.log.info(
                        f"Sending reminder TO overdue {client['firstname']} {client['lastname']}"
                    )
                    message = build_message(config, client, distance)
                    message_sent = send_text_and_ensure(message, client["phone_number"])
                    if message_sent:
                        client["reminded"] = client.get("reminded", 0) + 1
                        numbers_sent.append(client["phone_number"])
                        utils.sent_reminder_asana(config, projects_api, client)
                    else:
                        utils.send_text(
                            config,
                            services,
                            f"Message failed to deliver to {client['firstname']} {client['lastname']}.",
                            services["openphone"]["users"][config["name"].lower()][
                                "phone"
                            ],
                        )

            utils.update_yaml(clients, "./put/clients.yml")


main()

# TODO: Generate reports
