from datetime import datetime, timedelta

from loguru import logger

from utils.custom_types import ClientWithQuestionnaires, Config, Questionnaire


def format_ta_message(questionnaires: list[dict]) -> str:
    """Formats the message to be sent in TA."""
    logger.debug("Formatting TA message")
    message = ""
    for q_id, questionnaire in enumerate(questionnaires, start=1):
        notes = ""
        if "Self" in questionnaire["type"]:
            notes = " - For client being tested"
        message += f"{q_id}) {questionnaire['link']}{notes}\n"
    logger.success("Formatted TA message")
    return message


def build_q_message(
    config: Config,
    client: ClientWithQuestionnaires,
    most_recent_q: Questionnaire,
    distance: int,
) -> str | None:
    """Builds the message to be sent to the client based on their most recent questionnaire."""
    if not most_recent_q["sent"]:
        logger.warning(
            f"{client.fullName}'s {most_recent_q['questionnaireType']} has no sent date, cannot build message"
        )
        return None

    link_count = len(
        [
            q
            for q in client.questionnaires
            if q["status"]
            in [
                "PENDING",
                #  "SPANISH"
                "POSTDA_PENDING",
                "POSTEVAL_PENDING",
            ]
        ]
    )
    # is_spanish = any(q["status"] == "SPANISH" for q in client.questionnaires)  # noqa: ERA001 maybe someday
    is_spanish = False
    is_postda = any(q["status"] == "POSTDA_PENDING" for q in client.questionnaires)
    is_posteval = any(q["status"] == "POSTEVAL_PENDING" for q in client.questionnaires)
    portal_link = "https://portal.therapyappointment.com"

    if distance == 0:
        distance_phrase_en = "today"
        distance_phrase_es = "hoy"
    elif distance == -1:
        date_str = most_recent_q["sent"].strftime("%m/%d")
        distance_phrase_en = f"on {date_str} (yesterday)"
        distance_phrase_es = f"el {date_str} (ayer)"
    else:
        date_str = most_recent_q["sent"].strftime("%m/%d")
        days_ago = abs(distance)
        distance_phrase_en = f"on {date_str} ({days_ago} days ago)"
        distance_phrase_es = f"el {date_str} (hace {days_ago} días)"

    q_s_en = "questionnaire" if link_count == 1 else "questionnaires"
    it_them_en = "it" if link_count == 1 else "them"
    it_they_en = "it" if link_count == 1 else "they"
    is_are_en = "is" if link_count == 1 else "are"
    its_their_en = "its" if link_count == 1 else "their"

    q_s_es = "cuestionario" if link_count == 1 else "cuestionarios"
    lo_los_es = "lo" if link_count == 1 else "los"
    esta_estan_es = "está" if link_count == 1 else "están"
    su_sus_es = "su" if link_count == 1 else "sus"
    sent_s_es = "" if link_count == 1 else "s"
    complete_s_es = "" if link_count == 1 else "s"
    sent_it_them_es = "Lo enviamos" if link_count == 1 else "Los enviamos"

    messages_en = {
        0: (
            f"Hello, this is {config.name} from Driftwood Evaluation Center. "
            f"{'We are moving towards scheduling an appointment. The next step is ' if not is_posteval else ('In order to finalize our review, ' if is_postda else 'In order to provide you with a comprehensive report, ')}"
            f"we need you to complete your {q_s_en}. You can find {it_them_en} in the messages tab "
            f"in our patient portal: {portal_link} Please reply to this text with any questions. "
            f"Thank you for your help."
        ),
        1: (
            f"Hello, this is {config.name} with Driftwood Evaluation Center. "
            f"We are waiting for you to complete the {q_s_en} sent to you {distance_phrase_en}. "
            f"{'We are unable to schedule your appointment' if not is_posteval else ('We are unable to finalize our review' if is_postda else 'We are unable to provide you with a comprehensive report')} until {it_they_en} {is_are_en} completed "
            f"in {its_their_en} entirety. You can find {it_them_en} in the messages tab in our "
            f"patient portal: {portal_link} Please reply to this text with any questions. "
            f"Thank you for your help."
        ),
        2: (
            f"This is Driftwood Evaluation Center. If your {q_s_en} {is_are_en} not completed by "
            f"{(datetime.now() + timedelta(days=3)).strftime('%m/%d')} (3 days from now), "
            f"we will {'close out your referral' if not is_posteval else ('be unable to move forward' if is_postda else 'provide you with an incomplete report')}. Reply to this text with any concerns. You can find the "
            f"{q_s_en} in the messages tab in our patient portal: {portal_link}"
        ),
    }

    messages_es = {
        0: (
            f"Hola, es {config.name} de Driftwood Evaluation Center. ¡Estamos listos para "
            f"programar su cita! Para poder programar su cita, necesitamos que complete {su_sus_es} "
            f"{q_s_es}. {sent_it_them_es} a su correo electrónico desde una dirección DriftwoodEval.com. "
            f"Por favor, responda a este mensaje con cualquier pregunta. Gracias."
        ),
        1: (
            f"Hola, es {config.name} de Driftwood Evaluation Center. Estamos esperando que "
            f"complete {su_sus_es} {q_s_es} enviado{sent_s_es} {distance_phrase_es}. "
            f"No podemos programar su cita hasta que {lo_los_es} {esta_estan_es} "
            f"completo{complete_s_es} en {su_sus_es} totalidad. {sent_it_them_es} a su correo electrónico "
            f"desde una dirección DriftwoodEval.com. Por favor, responda a este mensaje con "
            f"cualquier pregunta. Gracias."
        ),
        2: (
            f"Es Driftwood Evaluation Center. Si {su_sus_es} {q_s_es} no {esta_estan_es} "
            f"completo{complete_s_es} antes de "
            f"{(datetime.now() + timedelta(days=3)).strftime('%m/%d')} (en 3 días), "
            f"cerraremos su remisión. Responda a este mensaje con cualquier inquietud. "
            f"{sent_it_them_es} a su correo electrónico desde una dirección DriftwoodEval.com."
        ),
    }

    reminded_count = most_recent_q["reminded"]

    if is_spanish:
        message = messages_es.get(reminded_count)
    else:
        message = messages_en.get(reminded_count)

    return message
