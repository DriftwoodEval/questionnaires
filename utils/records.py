import re

from utils.custom_types import RecordsContact


def normalize_district(name: str | None) -> str:
    if not name:
        return ""

    pattern = r"\b(county school district|school district|county)\b"

    clean = re.sub(rf"(?i){pattern}", "", name)

    return " ".join(clean.split()).lower()


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
