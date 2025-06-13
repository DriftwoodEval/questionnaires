from datetime import date, datetime

import requests
from backoff import expo, on_exception, on_predicate
from loguru import logger
from ratelimit import RateLimitException, limits

import shared_utils as utils

logger.add("logs/qreceive.log", rotation="500 MB")

services, config = utils.load_config()


def log_backoff(details):
    logger.debug(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


def log_giveup(details):
    logger.error(
        "Gave up after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


class LimitedRequest:
    @on_exception(
        expo,
        RateLimitException,
        max_tries=8,
        on_backoff=log_backoff,
        on_giveup=log_giveup,
    )
    @limits(calls=10, period=1)
    def get(self, url: str, params=None, headers=None, **kwargs) -> requests.Response:
        return requests.get(url, params, headers=headers, **kwargs)

    @on_exception(
        expo,
        RateLimitException,
        max_tries=8,
        on_backoff=log_backoff,
        on_giveup=log_giveup,
    )
    @limits(calls=10, period=1)
    def post(self, url: str, data=None, headers=None, **kwargs) -> requests.Response:
        return requests.post(url, data, headers=headers, **kwargs)


class OpenPhone:
    def __init__(self, config, services):
        self.config = config
        self.services = services
        self.main_number = services["openphone"]["main_number"]
        self.default_user = services["openphone"]["users"][config["name"].lower()]["id"]
        self.limited_request = LimitedRequest()

    @on_exception(
        expo,
        (ConnectionError, requests.HTTPError),
        max_tries=8,
        on_backoff=log_backoff,
        on_giveup=log_giveup,
    )
    def get_text_info(self, message_id: str) -> dict:
        url = f"https://api.openphone.com/v1/messages/{message_id}"
        headers = {
            "Content-Type": "application/json",
            "Authorization": services["openphone"]["key"],
        }
        response = self.limited_request.get(url, headers=headers)

        if response is None:
            raise ConnectionError("Failed to retrieve response from OpenPhone API")

        if response.status_code != 200:
            raise requests.HTTPError("API response: {}".format(response.status_code))

        response_data = response.json().get("data")
        return response_data

    @on_predicate(
        expo,
        max_tries=3,
        on_backoff=log_backoff,
        on_giveup=log_giveup,
    )
    def check_text_delivered(self, message_id: str) -> bool:
        message_info = self.get_text_info(message_id)
        message_status = message_info["status"]
        return message_status == "delivered"

    def send_text(
        self,
        message: str,
        to_number: str,
        from_number: str | None = None,
        user_blame: str | None = None,
    ) -> dict | None:
        if from_number is None:
            from_number = self.main_number
        if user_blame is None:
            user_blame = self.default_user

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
        try:
            logger.info(f"Attempting to send message '{message}' to {to_number}")
            response = self.limited_request.post(url, headers=headers, json=data)

            if response is None:
                raise ConnectionError("Failed to retrieve response from OpenPhone API")

            if response.status_code != 200:
                raise requests.HTTPError(
                    "API response: {}".format(response.status_code)
                )

            response_data = response.json().get("data")
            return response_data
        except Exception as e:
            logger.exception(f"Failed to get message info: {e}")
            return None

    def send_text_and_ensure(
        self,
        message: str,
        to_number: str,
        from_number: str | None = None,
        user_blame: str | None = None,
    ) -> bool:
        attempt_text = self.send_text(message, to_number, from_number, user_blame)
        if not attempt_text:
            logger.error(f"Possibly failed to send message {message} to {to_number}")
            return False
        message_id = attempt_text["id"]
        delivered = self.check_text_delivered(message_id)
        if delivered is True:
            logger.success(f"Successfully sent message {message} to {to_number}")
            return True
        else:
            logger.error(f"Failed to send message {message} to {to_number}")
            return False


def build_message(config: dict, client: dict, distance: int) -> str:
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
    openphone = OpenPhone(config, services)
    driver, actions = utils.initialize_selenium()
    email_info = {
        "reschedule": [],
        "failed": [],
        "call": {},
        "completed": {},
    }
    email_info["completed"] = utils.check_questionnaires(driver, config, services)
    clients = utils.get_previous_clients()
    if clients:
        numbers_sent = []
        for id in clients:
            client = clients[id]
            utils.mark_links_in_asana(projects_api, client, services, config)

            done = utils.all_questionnaires_done(client)

            if client["date"] == "Reschedule" and not done:
                logger.warning(
                    f"Client {client['firstname']} {client['lastname']} wants to/has rescheduled"
                )
                email_info["reschedule"].append(
                    f"{client['firstname']} {client['lastname']}"
                )
            elif client["date"] == "Reschedule" and done:
                continue

            distance = utils.check_appointment_distance(
                datetime.strptime(client["date"], "%Y/%m/%d").date()
            )
            logger.info(
                f"{client['firstname']} {client['lastname']} is {distance} days away and {'done' if done else 'not done'}"
            )
            if not done:
                already_messaged_today = client[
                    "phone_number"
                ] in numbers_sent and client[
                    "phone_number"
                ] != utils.format_phone_number(
                    services["openphone"]["users"][config["name"].lower()]["phone"]
                )
                if already_messaged_today:
                    logger.info(
                        f"Already messaged {client['firstname']} {client['lastname']} at {client['phone_number']} today"
                    )
                if distance >= 5 and distance % 3 == 2 and not already_messaged_today:
                    logger.info(
                        f"Sending reminder TO {client['firstname']} {client['lastname']}"
                    )
                    message = build_message(config, client, distance)
                    message_sent = openphone.send_text_and_ensure(
                        message, client["phone_number"]
                    )
                    if message_sent:
                        client["reminded"] = client.get("reminded", 0) + 1
                        numbers_sent.append(client["phone_number"])
                        utils.sent_reminder_asana(config, projects_api, client)
                    else:
                        email_info["failed"].append(
                            f"{client['firstname']} {client['lastname']}"
                        )
                elif 0 <= distance < 5:
                    logger.info(
                        f"Sending reminder ABOUT {client['firstname']} {client['lastname']}"
                    )
                    if str(distance) not in email_info["call"]:
                        email_info["call"][str(distance)] = []
                    email_info["call"][str(distance)].append(
                        f"{client['firstname']} {client['lastname']}"
                    )
                elif distance < 0 and distance % 3 == 2 and not already_messaged_today:
                    logger.info(
                        f"Sending reminder TO overdue {client['firstname']} {client['lastname']}"
                    )
                    message = build_message(config, client, distance)
                    message_sent = openphone.send_text_and_ensure(
                        message, client["phone_number"]
                    )
                    if message_sent:
                        client["reminded"] = client.get("reminded", 0) + 1
                        numbers_sent.append(client["phone_number"])
                        utils.sent_reminder_asana(config, projects_api, client)
                    else:
                        email_info["failed"].append(
                            f"{client['firstname']} {client['lastname']}"
                        )
            utils.update_yaml(clients, "./put/clients.yml")
        admin_email_text, admin_email_html = utils.build_admin_email(email_info)
        if admin_email_text != "":
            utils.send_gmail(
                admin_email_text,
                f"Receive Run for {datetime.today().strftime('%a, %b %-d')}",
                config["qreceive_emails"],
                config["automated_email"],
                html=admin_email_html,
            )


main()
