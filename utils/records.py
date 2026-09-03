import re

from utils.custom_types import RecordsContact

# Common misspellings seen on client forms, mapped to the canonical district name.
MISSPELLINGS = {
    "berkley": "berkeley",
}

# States TherapyAppointment reports before a client has finished onboarding.
# These are routine and expected, not failures worth surfacing.
EXPECTED_INCOMPLETE_STATES = ("portal not opened", "docs not signed")


class RecordsRequestError(Exception):
    """An expected consent-form/validation problem whose message is safe to surface.

    Lets classify_failure() fall back to "Unhandled error" only for genuinely
    unexpected crashes, not for known human-readable failures.
    """


def classify_failure(exc: Exception, client_name: str) -> tuple[str, bool]:
    """Turn a records-request exception into a reportable message and a sheet flag.

    Returns (error message, whether the failure belongs on the tracking sheet).
    A RecordsRequestError, or a TherapyAppointment state the client hasn't
    finished yet, is an expected outcome, not an unhandled crash.
    """
    reason = str(exc)

    if reason in EXPECTED_INCOMPLETE_STATES:
        return reason, False

    if isinstance(exc, RecordsRequestError):
        return reason, True

    return f"Unhandled error for {client_name}", True


def normalize_district(name: str | None) -> str:
    if not name:
        return ""

    pattern = r"\b(county school district|school district|county)\b"

    clean = re.sub(rf"(?i){pattern}", "", name)

    normalized = " ".join(clean.split()).lower()

    return MISSPELLINGS.get(normalized, normalized)


def resolve_school_contact(
    name: str, school_contacts: dict[str, RecordsContact]
) -> tuple[str, RecordsContact] | tuple[None, None]:
    """Helper to find a contact by name or alias."""
    name = name.lower().strip()
    if name in school_contacts:
        return name, school_contacts[name]
    for canonical_name, contact in school_contacts.items():
        if name in [a.lower().strip() for a in contact.aliases]:
            return canonical_name, contact
    return None, None
