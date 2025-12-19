from datetime import date, datetime
from typing import Annotated, List, Literal, Optional, TypedDict, Union

from pydantic import (
    BaseModel,
    EmailStr,
    Field,
    StringConstraints,
    field_validator,
)


class LocalConfigOverrides(BaseModel, extra="forbid"):
    """Optional configuration overrides."""

    database_url: Optional[str] = None


class LocalSettings(BaseModel):
    """Model for reading the local_config.yml file."""

    api_url: str = Field(description="The full URL for fetching the remote config.")
    config_overrides: LocalConfigOverrides = Field(default_factory=LocalConfigOverrides)


class Service(BaseModel):
    """A BaseModel containing service credentials."""

    username: str
    password: str


class ServiceWithAdmin(Service):
    """A BaseModel containing service credentials and an admin user."""

    admin_username: str
    admin_password: str


class OpenPhoneUser(BaseModel):
    """A BaseModel containing OpenPhone user information."""

    id: str
    phone: str


class OpenPhoneService(BaseModel):
    """A BaseModel containing OpenPhone API credentials and settings."""

    key: str
    main_number: Annotated[str, StringConstraints(pattern=r"^\+?1\d{10}$")]
    users: dict[str, OpenPhoneUser]


class Services(BaseModel):
    """A BaseModel containing all the service configurations and credentials."""

    openphone: OpenPhoneService
    therapyappointment: ServiceWithAdmin
    mhs: Service
    qglobal: Service
    wps: Service


class PieceworkCosts(BaseModel):
    """Cost configuration for different work types."""

    DA: Optional[float] = None
    EVAL: Optional[float] = None
    DAEVAL: Optional[float] = None
    REPORT: Optional[float] = None


class PieceworkConfig(BaseModel):
    """Piecework configuration including default and evaluator-specific costs."""

    costs: dict[str, PieceworkCosts]
    name_map: dict[str, str]

    def get_unit_cost(self, evaluator_name: str, appointment_type: str) -> float:
        """Get the unit cost for a specific evaluator and appointment type.

        Falls back to default costs if evaluator-specific costs are not found.
        """
        default_costs = self.costs["default"]

        if evaluator_name in self.costs:
            evaluator_costs = self.costs[evaluator_name]
            if hasattr(evaluator_costs, appointment_type):
                cost = getattr(evaluator_costs, appointment_type)
                if cost is None:
                    if hasattr(default_costs, appointment_type):
                        return getattr(default_costs, appointment_type)
                return cost

        if hasattr(default_costs, appointment_type):
            cost = getattr(default_costs, appointment_type)
            if cost is None:
                return 0.00
            return cost

        return 0.00

    def get_full_name(self, initials: str) -> str:
        """Get full name from initials.

        Args:
            initials: The initials to look up (case-insensitive)

        Returns:
            Full name if found, otherwise returns the original initials
        """
        initials_lower = initials.lower()
        return self.name_map.get(initials_lower, initials)


class RecordsContact(BaseModel):
    """A Pydantic containing records contact information."""

    email: EmailStr
    fax: bool = False


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
    punch_list_id: str
    punch_list_range: Annotated[
        str,
        StringConstraints(pattern=r"^.+![A-z]+\d*(:[A-z]+\d*)?$"),
    ]
    failed_sheet_id: str
    payroll_folder_id: str
    database_url: str
    excluded_ta: list[str]
    records_folder_id: str
    sent_records_folder_id: str
    records_emails: dict[str, RecordsContact]
    piecework: PieceworkConfig


class FullConfig(BaseModel):
    """A Pydantic model representing the full configuration of the application."""

    services: Services
    config: Config


class Questionnaire(TypedDict):
    """A TypedDict containing information about a questionnaire."""

    clientId: int
    questionnaireType: str
    link: Optional[str]
    sent: Optional[date]
    status: Literal[
        "PENDING",
        "COMPLETED",
        "IGNORING",
        "POSTEVAL_PENDING",
        "SPANISH",
        "LANGUAGE",
        "TEACHER",
        "EXTERNAL",
        "ARCHIVED",
        "JUST_ADDED",
    ]
    reminded: int
    lastReminded: Optional[date]


class Failure(TypedDict):
    """A TypedDict containing information about a failure."""

    failedDate: date
    reason: str
    daEval: Optional[Literal["DA", "EVAL", "DAEVAL", "Records"]]
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
    dob: date
    firstName: str
    lastName: str
    preferredName: Optional[str] = None
    fullName: str
    phoneNumber: Optional[str] = None
    gender: Optional[str] = None
    asdAdhd: Optional[str] = None
    status: bool


class ClientFromDB(_ClientBase):
    """A Pydantic model representing a client from the database."""

    questionnaires: Optional[list[Questionnaire]] = None
    autismStop: bool
    ifsp: bool
    ifspDownloaded: bool


class ClientWithQuestionnaires(ClientFromDB):
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

    failure: List[Failure]
    note: Optional[dict] = None

    @field_validator("failure")
    def validate_failure(cls, v: List[Failure]) -> List[Failure]:
        """Validate that the client has at least one failure."""
        if not v:
            raise ValueError("Client has no failures")
        return v


class AdminEmailInfo(TypedDict):
    """A TypedDict containing lists of clients grouped by status, for emailing."""

    ignoring: list[ClientWithQuestionnaires]
    failed: list[tuple[Union[ClientWithQuestionnaires, FailedClientFromDB], str]]
    call: list[Union[ClientWithQuestionnaires, FailedClientFromDB]]
    completed: list[ClientWithQuestionnaires]
    errors: list[str]
    ifsp_download_needed: list[ClientFromDB]


class Appointment(TypedDict):
    """A TypedDict containing information about an appointment from the database."""

    id: str
    evaluatorNpi: int
    clientName: str
    startTime: datetime
    endTime: datetime
    daEval: str
    asdAdhd: str
    cancelled: bool
    placeholder: bool
    locationKey: str
    calendarEventId: str


def validate_questionnaires(
    clients: dict[int, ClientFromDB],
) -> dict[int, ClientWithQuestionnaires]:
    """Convert clients from the database to ClientWithQuestionnaires.

    Returns:
        A dictionary of validated clients, where the keys are the client IDs and the values
        are ClientWithQuestionnaires objects.
    """
    validated = {}
    for client_id, client in clients.items():
        client_dict = client.model_dump()
        try:
            validated[client_id] = ClientWithQuestionnaires.model_validate(client_dict)
        except ValueError:
            continue  # Skip invalid clients
    return validated
