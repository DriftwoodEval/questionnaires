from datetime import datetime, timedelta

from loguru import logger

from utils.custom_types import ClientWithQuestionnaires, Config, Questionnaire

PORTAL_LINK = "https://portal.therapyappointment.com"


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


# Default global reminder messages, seeded into emr_questionnaire_reminder_template
# on migration and ported verbatim (in placeholder form) from the wording that used
# to be hardcoded here. Keyed by (reminderIndex, variant), matching the DB table.
DEFAULT_REMINDER_TEMPLATES: dict[tuple[int, str], str] = {
    (0, "DEFAULT"): (
        "Hello, this is $STAFF_NAME from Driftwood Evaluation Center. "
        "We are moving towards scheduling an appointment. The next step is "
        "we need you to complete your $QUESTIONNAIRE_WORD. You can find $IT_THEM "
        "in the messages tab in our patient portal: $PORTAL_LINK Please reply to "
        "this text with any questions. Thank you for your help."
    ),
    (0, "POSTDA"): (
        "Hello, this is $STAFF_NAME from Driftwood Evaluation Center. "
        "In order to finalize our review, we need you to complete your "
        "$QUESTIONNAIRE_WORD. You can find $IT_THEM in the messages tab in our "
        "patient portal: $PORTAL_LINK Please reply to this text with any "
        "questions. Thank you for your help."
    ),
    (0, "POSTEVAL"): (
        "Hello, this is $STAFF_NAME from Driftwood Evaluation Center. "
        "In order to provide you with a comprehensive report, we need you to "
        "complete your $QUESTIONNAIRE_WORD. You can find $IT_THEM in the "
        "messages tab in our patient portal: $PORTAL_LINK Please reply to this "
        "text with any questions. Thank you for your help."
    ),
    (1, "DEFAULT"): (
        "Hello, this is $STAFF_NAME with Driftwood Evaluation Center. "
        "We are waiting for you to complete the $QUESTIONNAIRE_WORD sent to you "
        "$DISTANCE_PHRASE. We are unable to schedule your appointment until "
        "$IT_THEY $IS_ARE completed in $ITS_THEIR entirety. You can find "
        "$IT_THEM in the messages tab in our patient portal: $PORTAL_LINK "
        "Please reply to this text with any questions. Thank you for your help."
    ),
    (1, "POSTDA"): (
        "Hello, this is $STAFF_NAME with Driftwood Evaluation Center. "
        "We are waiting for you to complete the $QUESTIONNAIRE_WORD sent to you "
        "$DISTANCE_PHRASE. We are unable to finalize our review until $IT_THEY "
        "$IS_ARE completed in $ITS_THEIR entirety. You can find $IT_THEM in the "
        "messages tab in our patient portal: $PORTAL_LINK Please reply to this "
        "text with any questions. Thank you for your help."
    ),
    (1, "POSTEVAL"): (
        "Hello, this is $STAFF_NAME with Driftwood Evaluation Center. "
        "We are waiting for you to complete the $QUESTIONNAIRE_WORD sent to you "
        "$DISTANCE_PHRASE. We are unable to provide you with a comprehensive "
        "report until $IT_THEY $IS_ARE completed in $ITS_THEIR entirety. You "
        "can find $IT_THEM in the messages tab in our patient portal: "
        "$PORTAL_LINK Please reply to this text with any questions. Thank you "
        "for your help."
    ),
    (2, "DEFAULT"): (
        "This is Driftwood Evaluation Center. If your $QUESTIONNAIRE_WORD "
        "$IS_ARE not completed by $DEADLINE_DATE ($ESCALATION_DAYS days from "
        "now), we will close out your referral. Reply to this text with any "
        "concerns. You can find the $QUESTIONNAIRE_WORD in the messages tab in "
        "our patient portal: $PORTAL_LINK"
    ),
    (2, "POSTDA"): (
        "This is Driftwood Evaluation Center. If your $QUESTIONNAIRE_WORD "
        "$IS_ARE not completed by $DEADLINE_DATE ($ESCALATION_DAYS days from "
        "now), we will be unable to move forward. Reply to this text with any "
        "concerns. You can find the $QUESTIONNAIRE_WORD in the messages tab in "
        "our patient portal: $PORTAL_LINK"
    ),
    (2, "POSTEVAL"): (
        "This is Driftwood Evaluation Center. If your $QUESTIONNAIRE_WORD "
        "$IS_ARE not completed by $DEADLINE_DATE ($ESCALATION_DAYS days from "
        "now), we will provide you with an incomplete report. Reply to this "
        "text with any concerns. You can find the $QUESTIONNAIRE_WORD in the "
        "messages tab in our patient portal: $PORTAL_LINK"
    ),
}


def render_reminder_message(
    templates: dict[tuple[int, str], str],
    settings: dict,
    config: Config,
    client: ClientWithQuestionnaires,
    *,
    most_recent_q: Questionnaire,
    distance: int,
    override: str | None = None,
) -> str | None:
    """Renders the reminder message for a client's most recent pending questionnaire.

    Picks the template for (reminded count, postda/posteval variant) unless an
    `override` text is given for this client's batch/stage, then substitutes
    $PLACEHOLDER tokens (in either the default template or the override) with
    values computed for this client/batch.
    """
    if not most_recent_q["sent"]:
        logger.warning(
            f"{client.fullName}'s {most_recent_q['questionnaireType']} has no sent date, cannot build message"
        )
        return None

    link_count = len(
        [
            q
            for q in client.questionnaires
            if q["status"] in ["PENDING", "POSTDA_PENDING", "POSTEVAL_PENDING"]
        ]
    )
    completed_count = len(
        [q for q in client.questionnaires if q["status"] == "COMPLETED"]
    )
    is_postda = any(q["status"] == "POSTDA_PENDING" for q in client.questionnaires)
    is_posteval = any(q["status"] == "POSTEVAL_PENDING" for q in client.questionnaires)

    if is_posteval and is_postda:
        variant = "POSTDA"
    elif is_posteval:
        variant = "POSTEVAL"
    else:
        variant = "DEFAULT"

    reminded_count = most_recent_q["reminded"]
    template = (
        override if override is not None else templates.get((reminded_count, variant))
    )
    if template is None:
        return None

    if distance == 0:
        distance_phrase = "today"
    elif distance == -1:
        date_str = most_recent_q["sent"].strftime("%m/%d")
        distance_phrase = f"on {date_str} (yesterday)"
    else:
        date_str = most_recent_q["sent"].strftime("%m/%d")
        days_ago = abs(distance)
        distance_phrase = f"on {date_str} ({days_ago} days ago)"

    escalation_days = settings["escalationSilenceDays"]
    deadline_date = (datetime.now() + timedelta(days=escalation_days)).strftime("%m/%d")

    substitutions = {
        "$CLIENT_FIRST_NAME": client.firstName,
        "$STAFF_NAME": config.name,
        "$QUESTIONNAIRE_WORD": "questionnaire" if link_count == 1 else "questionnaires",
        "$IT_THEM": "it" if link_count == 1 else "them",
        "$IT_THEY": "it" if link_count == 1 else "they",
        "$IS_ARE": "is" if link_count == 1 else "are",
        "$ITS_THEIR": "its" if link_count == 1 else "their",
        "$DISTANCE_PHRASE": distance_phrase,
        "$DEADLINE_DATE": deadline_date,
        "$ESCALATION_DAYS": str(escalation_days),
        "$PORTAL_LINK": PORTAL_LINK,
        "$COMPLETED_COUNT": str(completed_count),
        "$REMAINING_COUNT": str(link_count),
    }

    message = template
    for placeholder, value in substitutions.items():
        message = message.replace(placeholder, value)
    return message
