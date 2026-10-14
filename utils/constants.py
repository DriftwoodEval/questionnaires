from typing import Final

# Kept in sync with TEST_NAMES in ../winnonah (python/utils/constants.py and
# src/lib/constants.ts).
TEST_NAMES: Final = [
    "Testman Testson",
    "Testman Testson Jr.",
    "Johnny Smonny",
    "Johnny Smonathan",
    "Test Mctest",
    "Test Test",
    "Testing Test",
    "Johnny Test",
    "Barbara Steele",
    "Karen Aston",
    "Test Testerson",
]
TEST_NAMES_LOWER: Final = {n.lower() for n in TEST_NAMES}

# Clients this age or older don't need records requested from their school,
# so records-request.py never picks them up, and an unsigned school
# consent-to-release form doesn't count against them in check_if_docs_signed.
SCHOOL_RECORDS_ADULT_AGE: Final = 22
