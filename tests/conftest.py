from datetime import date, datetime

import pytest

from utils.custom_types import (
    ClientWithQuestionnaires,
    Config,
    PieceworkConfig,
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


def make_config(**overrides) -> Config:
    fields = {
        "initials": "TC",
        "name": "Test Clinician",
        "email": "clinician@example.com",
        "automated_email": "automated@example.com",
        "qreceive_emails": ["admin@example.com"],
        "punch_list_id": "punch-list-id",
        "punch_list_range": "Sheet1!A1:Z",
        "failed_sheet_id": "failed-sheet-id",
        "payroll_folder_id": "payroll-folder-id",
        "database_url": "mysql://user:pass@localhost/db",
        "excluded_ta": [],
        "records_folder_id": "records-folder-id",
        "sent_records_folder_id": "sent-records-folder-id",
        "records_emails": {},
        "piecework": PieceworkConfig(costs={}, name_map={}, payroll_emails={}),
    }
    fields.update(overrides)
    return Config.model_validate(fields)


@pytest.fixture
def questionnaire_factory():
    return make_questionnaire


@pytest.fixture
def client_factory():
    return make_client


@pytest.fixture
def config_factory():
    return make_config
