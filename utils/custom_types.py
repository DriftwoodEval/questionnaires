from datetime import date, datetime
from typing import Annotated, Literal, TypedDict

from pydantic import (
    BaseModel,
    EmailStr,
    Field,
    StringConstraints,
    TypeAdapter,
    field_validator,
)


class LocalConfigOverrides(BaseModel, extra="forbid"):
    """Optional configuration overrides."""

    database_url: str | None = None


class LocalSettings(BaseModel):
    """Model for reading the local_config.yml file."""

    api_url: str = Field(description="The full URL for fetching the remote config.")
    log_host: str = Field(description="Host for sending logs to a remote log server.")
    api_secret: str = Field(description="API secret for authentication")
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
    novopsych: Service


class PieceworkCosts(BaseModel):
    """Cost configuration for different work types."""

    DA: float | None = None
    ADHDDA: float | None = None
    EVAL: float | None = None
    DAEVAL: float | None = None
    REPORT: float | None = None


class PieceworkConfig(BaseModel):
    """Piecework configuration including default and evaluator-specific costs."""

    costs: dict[str, PieceworkCosts]
    name_map: dict[str, str]
    payroll_emails: dict[str, EmailStr]

    def get_unit_cost(self, evaluator_name: str, appointment_type: str) -> float:
        """Get the unit cost for a specific evaluator and appointment type.

        Falls back to default costs if evaluator-specific costs are not found.
        """
        default_costs = self.costs["default"]
        default_cost = getattr(default_costs, appointment_type, None)

        if evaluator_name in self.costs:
            evaluator_costs = self.costs[evaluator_name]
            evaluator_cost = getattr(evaluator_costs, appointment_type, None)
            if evaluator_cost is not None:
                return evaluator_cost
            if default_cost is not None:
                return default_cost
            return 0.00

        if default_cost is not None:
            return default_cost

        return 0.00

    def get_full_name(self, initials: str) -> str:
        """Get full name from initials.

        Args:
            initials: The initials to look up (case-insensitive)

        Returns:
            Full name if found, otherwise returns the original initials
        """
        initials_lower = initials.lower()
        for k, v in self.name_map.items():
            if k.lower() == initials_lower:
                return v
        return ""


class RecordsContact(BaseModel):
    """A Pydantic containing records contact information."""

    email: str
    fax: bool = False
    aliases: list[str]

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        """Validate that the email is a single valid email or a comma-separated list of valid emails."""
        adapter = TypeAdapter(EmailStr)
        for email in v.split(","):
            adapter.validate_python(email.strip())
        return v


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


QuestionnaireStatus = Literal[
    "PENDING",
    "COMPLETED",
    "IGNORING",
    "POSTDA_PENDING",
    "POSTEVAL_PENDING",
    "SPANISH",
    "LANGUAGE",
    "TEACHER",
    "EXTERNAL",
    "ARCHIVED",
    "JUST_ADDED",
]


class Questionnaire(TypedDict):
    """A TypedDict containing information about a questionnaire."""

    clientId: int
    questionnaireType: str
    link: str | None
    sent: date | None
    status: QuestionnaireStatus
    reminded: int
    lastReminded: date | None


class Failure(TypedDict):
    """A TypedDict containing information about a failure."""

    failedDate: date
    reason: str
    daEval: Literal["DA", "EVAL", "DAEVAL", "Records"] | None
    reminded: int
    lastReminded: date | None


class FailedClient(TypedDict):
    """A TypedDict containing information about a failed client."""

    firstName: str
    lastName: str
    fullName: str
    asdAdhd: str
    daEval: str
    failedDate: str
    error: str
    questionnaires_needed: list[str] | str | None
    questionnaire_links_generated: list[dict[str, bool | str]] | None


class _ClientBase(BaseModel):
    id: int
    dob: date
    firstName: str  # noqa: N815
    lastName: str  # noqa: N815
    preferredName: str | None = None  # noqa: N815
    fullName: str  # noqa: N815
    phoneNumber: str | None = None  # noqa: N815
    gender: str | None = None
    asdAdhd: str | None = None  # noqa: N815
    status: bool
    sessionStartedAt: datetime | None = None  # noqa: N815


class _SharedClientFromDB(_ClientBase):
    """Common fields for clients from the database. Not intended to be used directly."""

    autismStop: bool  # noqa: N815
    pause: bool
    babyNetERNeeded: bool  # noqa: N815
    babyNetERDownloaded: bool  # noqa: N815
    language: str
    addedDate: date | None = None  # noqa: N815
    latitude: float | None = None
    longitude: float | None = None
    address: str | None = None
    schoolDistrict: str | None = None  # noqa: N815
    recordsNeeded: str | None = None  # noqa: N815
    pendingRequestMessage: str | None = None  # noqa: N815


class ClientFromDB(_SharedClientFromDB):
    """A Pydantic model representing a client from the database."""

    questionnaires: list[Questionnaire] | None = None


class ClientWithQuestionnaires(_SharedClientFromDB):
    """A Pydantic model representing a client with questionnaires."""

    questionnaires: list[Questionnaire]

    @field_validator("questionnaires")
    @classmethod
    def validate_questionnaires(cls, v: list[Questionnaire]) -> list[Questionnaire]:
        if not v:
            raise ValueError("Client has no questionnaires")
        return v


class FailedClientFromDB(ClientFromDB):
    """A Pydantic model representing a failed client from the database."""

    failure: list[Failure]
    note: dict | None = None

    @field_validator("failure")
    @classmethod
    def validate_failure(cls, v: list[Failure]) -> list[Failure]:
        if not v:
            raise ValueError("Client has no failures")
        return v


class AdminEmailInfo(TypedDict):
    """A TypedDict containing lists of clients grouped by status, for emailing."""

    ignoring: list[ClientWithQuestionnaires]
    failed: list[
        tuple[ClientWithQuestionnaires | FailedClientFromDB | ClientFromDB, str]
    ]
    call: list[ClientWithQuestionnaires | FailedClientFromDB]
    completed: list[ClientWithQuestionnaires]
    errors: list[str]


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
