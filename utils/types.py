from datetime import date
from typing import Annotated, Literal, Optional, TypedDict, Union

from pydantic import (
    BaseModel,
    EmailStr,
    StringConstraints,
    field_validator,
)


class Service(TypedDict):
    """A TypedDict containing service credentials."""

    username: str
    password: str


class ServiceWithAdmin(Service):
    """A TypedDict containing service credentials and an admin user."""

    admin_username: str
    admin_password: str


class OpenPhoneUser(TypedDict):
    """A TypedDict containing OpenPhone user information."""

    id: str
    phone: str


class OpenPhoneService(TypedDict):
    """A TypedDict containing OpenPhone API credentials and settings."""

    key: str
    main_number: str
    users: dict[str, OpenPhoneUser]


class Services(TypedDict):
    """A TypedDict containing all the service configurations and credentials."""

    mhs: Service
    openphone: OpenPhoneService
    qglobal: Service
    therapyappointment: ServiceWithAdmin
    wps: Service


class Config(BaseModel):
    """A Pydantic model representing the configuration of the application."""

    initials: Annotated[
        str,
        StringConstraints(strip_whitespace=True, to_upper=True, max_length=4),
    ]
    name: str
    email: EmailStr
    automated_email: EmailStr
    qreceive_emails: list[EmailStr]
    cc_emails: list[EmailStr]
    excluded_calendars: list[EmailStr]
    punch_list_id: str
    punch_list_range: Annotated[
        str,
        StringConstraints(pattern=r"^.+![A-z]+\d*(:[A-z]+\d*)?$"),
    ]
    failed_sheet_id: str
    database_url: str
    excluded_ta: list[str]
    records_folder_id: str
    sent_records_folder_id: str
    records_emails: dict[str, str]


class Questionnaire(TypedDict):
    """A TypedDict containing information about a questionnaire."""

    questionnaireType: str
    link: str
    sent: date
    status: Literal["COMPLETED", "PENDING", "RESCHEDULED"]
    reminded: int
    lastReminded: Optional[date]


class Failure(TypedDict):
    """A TypedDict containing information about a failure."""

    failedDate: date
    reason: str
    reminded: int
    lastReminded: Optional[date]


class FailedClient(TypedDict):
    """A TypedDict containing information about a failed client."""

    firstName: str
    lastName: str
    fullName: str
    asdAdhd: str
    daEval: str
    failedDate: str
    error: str
    questionnaires_needed: Optional[list[str] | str]
    questionnaire_links_generated: Optional[list[dict[str, bool | str]]]


class _ClientBase(BaseModel):
    id: int
    dob: Optional[date] = None
    firstName: str
    lastName: str
    preferredName: Optional[str] = None
    fullName: str
    phoneNumber: Optional[str] = None
    gender: Optional[str] = None
    asdAdhd: Optional[str] = None


class ClientFromDB(_ClientBase):
    """A Pydantic model representing a client from the database."""

    questionnaires: Optional[list[Questionnaire]] = None


class ClientWithQuestionnaires(_ClientBase):
    """A Pydantic model representing a client with questionnaires."""

    questionnaires: list[Questionnaire]

    @field_validator("questionnaires")
    def validate_questionnaires(cls, v: list[Questionnaire]) -> list[Questionnaire]:
        """Validate that the client has questionnaires."""
        if not v:
            raise ValueError("Client has no questionnaires")
        return v


class FailedClientFromDB(ClientFromDB):
    """A Pydantic model representing a failed client from the database."""

    failure: Failure
    note: Optional[dict] = None

    @field_validator("failure")
    def validate_failure(cls, v: Failure) -> Failure:
        """Validate that the client has a failure."""
        if not v:
            raise ValueError("Client has no failure")
        return v


class AdminEmailInfo(TypedDict):
    """A TypedDict containing lists of clients grouped by status, for emailing."""

    ignoring: list[ClientWithQuestionnaires]
    failed: list[tuple[Union[ClientWithQuestionnaires, FailedClientFromDB], str]]
    call: list[Union[ClientWithQuestionnaires, FailedClientFromDB]]
    completed: list[ClientWithQuestionnaires]
    errors: list[str]


def validate_questionnaires(
    clients: dict[int, ClientFromDB],
) -> dict[int, ClientWithQuestionnaires]:
    """Validate clients from the database and convert them to ClientWithQuestionnaires.

    Returns:
        A dictionary of validated clients, where the keys are the client IDs and the values
        are ClientWithQuestionnaires objects.
    """
    validated = {}
    for client_id, client in clients.items():
        try:
            validated[client_id] = ClientWithQuestionnaires.model_validate(client)
        except ValueError:
            continue  # Skip invalid clients
    return validated
