from typing import Final

# Kept in sync with BUSINESS_TIMEZONE in ../winnonah (python/utils/constants.py
# and src/lib/constants.ts). Prefer config.business_timezone (from
# load_config()) where a Config is already in scope; this is the fallback for
# leaf functions that aren't threaded a Config.
BUSINESS_TIMEZONE: Final = "America/New_York"

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
