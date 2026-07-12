from datetime import date, datetime

import pytest

from utils.custom_types import (
    ClientWithQuestionnaires,
    Questionnaire,
    QuestionnaireStatus,
)


def make_questionnaire(
    q_type: str = "ASRS (6-18 Years)",
    status: QuestionnaireStatus = "PENDING",
    sent: date | None = None,
    link: str | None = "https://example.com/q",
    client_id: int = 1,
    reminded: int = 0,
    last_reminded: date | None = None,
) -> Questionnaire:
    return {
        "clientId": client_id,
        "questionnaireType": q_type,
        "link": link,
        "sent": sent,
        "status": status,
        "reminded": reminded,
        "lastReminded": last_reminded,
    }


def make_client(
    client_id: int = 1,
    dob: date = date(2015, 1, 1),
    status: bool = True,
    asd_adhd: str | None = None,
    session_started_at: datetime | None = None,
    questionnaires: list[Questionnaire] | None = None,
) -> ClientWithQuestionnaires:
    return ClientWithQuestionnaires.model_validate(
        {
            "id": client_id,
            "dob": dob,
            "firstName": "Test",
            "lastName": "Client",
            "fullName": "Test Client",
            "gender": "M",
            "asdAdhd": asd_adhd,
            "status": status,
            "sessionStartedAt": session_started_at,
            "autismStop": False,
            "pause": False,
            "babyNetERNeeded": False,
            "babyNetERDownloaded": False,
            "language": "English",
            "questionnaires": questionnaires
            if questionnaires is not None
            else [make_questionnaire(client_id=client_id)],
        }
    )


@pytest.fixture
def questionnaire_factory():
    return make_questionnaire


@pytest.fixture
def client_factory():
    return make_client
