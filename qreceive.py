import logging
from datetime import date, datetime
from time import sleep

import requests
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

import shared_utils as utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("qreceive.log"), logging.StreamHandler()],
)

services, config = utils.load_config()


def send_text(
    message,
    to_number,
    from_number=services["openphone"]["main_number"],
    user_blame=services["openphone"]["users"][config["name"].lower()]["id"],
):
    to_number = "+1" + "".join(filter(str.isdigit, to_number))
    url = "https://api.openphone.com/v1/messages"
    headers = {
        "Content-Type": "application/json",
        "Authorization": services["openphone"]["key"],
    }
    data = {
        "content": message,
        "from": from_number,
        "to": [to_number],
        "userId": user_blame,
    }
    response = requests.post(url, headers=headers, json=data)
    response_data = response.json().get("data")
    return response_data


def get_text_info(message_id):
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
    logging.info(f"Attempting to send message '{message}' to {to_number}")
    attempt_text = send_text(message, to_number, from_number, user_blame)
    if not attempt_text:
        logging.warning(f"Possibly failed to send message {message} to {to_number}")
        return False
    message_id = attempt_text["id"]
    for i in range(3):
        sleep_time = 2**i
        sleep(sleep_time)
        message_info = get_text_info(message_id)
        message_status = message_info["status"]
        logging.info(f"Message status on attempt {i + 1}: {message_status}")
        if message_status == "delivered":
            return True
    else:
        logging.warning(f"Failed to send message {message} to {to_number}")
        return False


def check_q_done(driver, q_link):
    driver.implicitly_wait(10)
    url = q_link
    driver.get(url)

    complete = False

    if "mhs.com" in url:
        try:
            driver.find_element(
                By.XPATH, "//*[contains(text(), 'Thank you for completing')]"
            )
            complete = True
        except NoSuchElementException:
            complete = False
    elif "pearsonassessments.com" in url:
        try:
            driver.find_element(By.XPATH, "//*[contains(text(), 'Test Completed!')]")
            complete = True
        except NoSuchElementException:
            complete = False
    elif "wpspublish" in url:
        try:
            driver.find_element(
                By.XPATH,
                "//*[contains(text(), 'This assessment is not available at this time')]",
            )
            complete = True
        except NoSuchElementException:
            complete = False

    return complete


def check_questionnaires(driver):
    clients = utils.get_previous_clients()
    if clients:
        for id in clients:
            client = clients[id]
            for questionnaire in client["questionnaires"]:
                questionnaire["done"] = check_q_done(driver, questionnaire["link"])
        utils.update_yaml(clients, "./put/clients.yml")


def format_appointment(client):
    appointment = client["date"]
    return datetime.strptime(appointment, "%Y/%m/%d").strftime("%A, %B %d")


def check_appointment_distance(appointment: date):
    today = date.today()
    delta = appointment - today
    return delta.days


def all_questionnaires_done(client):
    return all(q["done"] for q in client["questionnaires"])


def build_message(config, client):
    link_count = len(client.get("questionnaires", []))
    if not client.get("reminded"):
        message = f"Hello, this is {config['name']} from Driftwood Evaluation Center. Please be on the lookout for an email from the patient portal Therapy Appointment as there {'is a questionnaire' if link_count == 1 else 'are questionnaires'} for you to complete in your messages. Please let me know if you have any questions. Thank you for your time."
    else:
        message = f"Hello, this is {config['name']} with Driftwood Evaluation Center. It appears your questionnaire{'' if link_count == 1 else 's'} for your appointment on {format_appointment(client)} {'is' if link_count == 1 else 'are'} still incomplete. Please complete {'it' if link_count == 1 else 'them'} as soon as possible as we will be unable to effectively evaluate if {'it is' if link_count == 1 else 'they are'} incomplete."
    return message


def main():
    projects_api = utils.init_asana(services)
    driver, actions = utils.initialize_selenium()
    check_questionnaires(driver)
    clients = utils.get_previous_clients()
    if clients:
        for id in clients:
            client = clients[id]
            for questionnaire in client["questionnaires"]:
                if questionnaire["done"]:
                    utils.mark_link_done(
                        projects_api,
                        services,
                        config,
                        client["asana"],
                        questionnaire["link"],
                    )

            distance = check_appointment_distance(
                datetime.strptime(client["date"], "%Y/%m/%d").date()
            )
            done = all_questionnaires_done(client)
            if distance % 3 == 2 and not done:
                if distance >= 5:
                    message = build_message(config, client)
                    # If this is the first reminder
                    if not client.get("reminded"):
                        message_sent = send_text_and_ensure(
                            message, client["phone_number"]
                        )
                        if message_sent:
                            client["reminded"] = True
                        else:
                            send_text(
                                f"Message failed to deliver to {client['firstname']} {client['lastname']}.",
                                services["openphone"]["users"][config["name"].lower()][
                                    "phone"
                                ],
                            )
                    else:
                        message_sent = send_text_and_ensure(
                            message, client["phone_number"]
                        )
                        if not message_sent:
                            send_text(
                                f"Message failed to deliver to {client['firstname']} {client['lastname']}.",
                                services["openphone"]["users"][config["name"].lower()][
                                    "phone"
                                ],
                            )
                else:
                    send_text(
                        f"{client['firstname']} {client['lastname']} has an appointment on {(format_appointment(client))} (in {distance} days) and hasn't done everything, please call them.",
                        services["openphone"]["users"][config["name"].lower()]["phone"],
                    )
            if done:
                send_text(
                    f"{client['firstname']} {client['lastname']} has finished their questionnares for an appointment on {format_appointment(client)}. Please generate.",
                    services["openphone"]["users"][config["name"].lower()]["phone"],
                )
            utils.update_yaml(clients, "./put/clients.yml")


main()

# TODO: Generate reports
