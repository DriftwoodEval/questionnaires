from datetime import date, datetime, timedelta

import requests
from backoff import constant, expo, on_exception, on_predicate
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
        max_tries=5,
        on_backoff=log_backoff,
        on_giveup=log_giveup,
    )
    @limits(calls=10, period=1)
    def get(self, url: str, params=None, headers=None, **kwargs) -> requests.Response:
        return requests.get(url, params, headers=headers, **kwargs)

    @on_exception(
        expo,
        RateLimitException,
        max_tries=5,
        on_backoff=log_backoff,
        on_giveup=log_giveup,
    )
    @limits(calls=10, period=1)
    def post(self, url: str, data=None, headers=None, **kwargs) -> requests.Response:
        return requests.post(url, data, headers=headers, **kwargs)


class OpenPhone:
    def __init__(self, config: utils.Config, services: utils.Services):
        self.config = config
        self.services = services
        self.main_number = services["openphone"]["main_number"]
        self.default_user = services["openphone"]["users"][config.name.lower()]["id"]
        self.limited_request = LimitedRequest()

    @on_exception(
        expo,
        (ConnectionError, requests.HTTPError),
        factor=2,
        base=2,
        max_tries=5,
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

        if response.status_code >= 400:
            raise requests.HTTPError("API response: {}".format(response.status_code))

        response_data = response.json().get("data")
        return response_data

    @on_predicate(
        expo,
        factor=2,
        base=2,
        max_tries=5,
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

            if response.status_code >= 400:
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


def build_message(
    config: utils.Config,
    client: utils.ClientWithQuestionnaires,
    most_recent_q: utils.Questionnaire,
    distance: int,
) -> str | None:
    link_count = len([q for q in client.questionnaires if q["status"] == "PENDING"])
    if distance == 0:
        distance_phrase = "today"
    elif distance == -1:
        distance_phrase = f"on {most_recent_q['sent'].strftime('%m/%d')} (yesterday)"
    else:
        distance_phrase = (
            f"on {most_recent_q['sent'].strftime('%m/%d')} ({abs(distance)} days ago)"
        )

    message = None
    if most_recent_q["reminded"] == 0:
        message = f"Hello, this is {config.name} from Driftwood Evaluation Center. We are ready to schedule your appointment! In order for us to schedule your appointment, we need you to complete your {'questionnaire' if link_count == 1 else 'questionnaires'}. You can find {'it' if link_count == 1 else 'them'} in the messages tab in our patient portal: https://portal.therapyappointment.com Please reply to this text with any questions. Thank you for your help."
    elif most_recent_q["reminded"] == 1:
        message = f"Hello, this is {config.name} with Driftwood Evaluation Center. We are waiting for you to complete the questionnaire{'' if link_count == 1 else 's'} sent to you {distance_phrase}. We are unable to schedule your appointment until {'it is' if link_count == 1 else 'they are'} completed in {'its' if link_count == 1 else 'their'} entirety. You can find {'it' if link_count == 1 else 'them'} in the messages tab in our patient portal: https://portal.therapyappointment.com Please reply to this text with any questions. Thank you for your help."
    elif most_recent_q["reminded"] == 2:
        message = f"This is Driftwood Evaluation Center. We haven't heard from you. If your questionnaire{' is' if link_count == 1 else 's are'} not completed by {(datetime.now() + timedelta(days=3)).strftime('%m/%d')} (3 days from now), we will close out your referral. Reply to this text with any concerns. You can find the questionnaire{'' if link_count == 1 else 's'} in the messages tab in our patient portal: https://portal.therapyappointment.com"

    return message


def should_send_reminder(
    most_recent_q: utils.Questionnaire, last_reminded_distance: int
) -> bool:
    reminded_count = most_recent_q["reminded"]

    reminder_schedule = {
        0: 0,  # Initial message (same day)
        1: 7,  # First follow-up (1 week later)
        2: 5,  # Second follow-up (5 days after first follow-up)
    }

    expected_day = reminder_schedule.get(reminded_count)
    if expected_day is not None and last_reminded_distance >= expected_day:
        logger.debug(
            f"Reminder should be sent because client has been reminded {reminded_count} times, and it has been {last_reminded_distance} days since the last reminder"
        )
        return True
    else:
        return False


def main():
    projects_api = utils.init_asana(services)
    openphone = OpenPhone(config, services)
    driver, actions = utils.initialize_selenium()
    email_info: utils.AdminEmailInfo = {
        "reschedule": [],
        "failed": [],
        "call": [],
        "completed": [],
    }
    clients, _ = utils.get_previous_clients(config)
    if clients is None:
        logger.critical("Failed to get previous clients")
        return

    clients = utils.validate_questionnaires(clients)
    email_info["completed"] = (
        utils.check_questionnaires(driver, config, services, clients)
    )

    clients, _ = utils.get_previous_clients(config)

    # TODO: Remove when Asana is no longer used
    utils.log_asana(services, projects_api)

    if clients:
        clients = utils.validate_questionnaires(clients)
        numbers_sent = []
        for _, client in clients.items():
            # TODO: Change when Asana is no longer used
            if client in email_info["completed"]:
                utils.mark_links_in_asana(projects_api, client)

            done = utils.all_questionnaires_done(client)

            if utils.check_if_rescheduled(client):
                logger.warning(f"Client {client.fullName} wants to/has rescheduled")
                # TODO: Change how we hold onto email data? Revisit with Maddy
                email_info["reschedule"].append(client)
                continue

            if not done:
                most_recent_q = utils.get_most_recent_not_done(client)
                distance = utils.check_distance(most_recent_q["sent"])
                last_reminded = most_recent_q.get("lastReminded")
                if last_reminded is not None:
                    # TODO: This is a lot like check_distance, maybe try for reusability?
                    last_reminded_distance = (date.today() - last_reminded).days
                else:
                    last_reminded_distance = 0

                logger.info(
                    f"{client.fullName} had questionnaire sent on {most_recent_q['sent']} and isn't done"
                )

                if not client.phoneNumber:
                    logger.warning(f"Client {client.fullName} has no phone number")
                    # TODO: Include reasons for failures in email
                    email_info["failed"].append(client)
                    continue

                already_messaged_today = (
                    client.phoneNumber in numbers_sent
                    and utils.format_phone_number(client.phoneNumber)
                    != utils.format_phone_number(
                        services["openphone"]["users"][config.name.lower()]["phone"]
                    )
                )

                if already_messaged_today:
                    logger.warning(
                        f"Already messaged {client.fullName} at {client.phoneNumber} today"
                    )

                # TODO: For some reason this appears to work but doesn't register as the same variable as last_reminded distance above
                if most_recent_q["reminded"] == 3 and last_reminded_distance >= 3:
                    email_info["call"].append(client)
                    for q in client.questionnaires:
                        if q["status"] == "PENDING":
                            q["reminded"] += 1
                            q["lastReminded"] = date.today()

                elif (
                    most_recent_q["reminded"] < 3
                    and not already_messaged_today
                    and client.phoneNumber
                ):
                    if should_send_reminder(most_recent_q, last_reminded_distance):
                        logger.info(f"Sending reminder TO {client.fullName}")
                        message = build_message(config, client, most_recent_q, distance)
                        # Redundant failsafe to super ensure we don't text people a message that just says "None"
                        if not message:
                            logger.error(
                                f"Failed to build message for {client.fullName}"
                            )
                            continue
                        message_sent = openphone.send_text_and_ensure(
                            message, client.phoneNumber
                        )
                        if message_sent:
                            numbers_sent.append(client.phoneNumber)

                            for q in client.questionnaires:
                                if q["status"] == "PENDING":
                                    q["reminded"] += 1
                                    q["lastReminded"] = date.today()
                        else:
                            logger.error(f"Failed to send message to {client.fullName}")
                            # TODO: Track the reason for failure
                            email_info["failed"].append(client)
            elif client in email_info["completed"]:
                if len(client.questionnaires) > 2:
                    utils.update_punch_by_column(config, str(client.id), "DA", "done")
                    utils.update_punch_by_column(config, str(client.id), "EVAL", "done")
                else:
                    utils.update_punch_by_column(config, str(client.id), "DA", "done")
            # URGENT TODO: update every time, in loop
            utils.update_questionnaires_in_db(config, [client])

        # TODO: Can we send an incomplete email, even with exception?
        admin_email_text, admin_email_html = utils.build_admin_email(email_info)
        if admin_email_text != "":
            utils.send_gmail(
                admin_email_text,
                f"Receive Run for {datetime.today().strftime('%a, %b')} {datetime.today().day}",
                ",".join(config.qreceive_emails),
                config.automated_email,
                html=admin_email_html,
            )
    else:
        logger.info("No clients to check")

if __name__ == "__main__":
    main()
