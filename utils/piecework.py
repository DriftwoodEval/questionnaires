import re
from typing import Any

import pandas as pd


def extract_writer_initials(assigned_to: Any) -> str:
    """Extract only letters from the assigned to column."""
    if pd.isna(assigned_to) or not assigned_to:
        return ""
    return re.sub(r"[^a-zA-Z]", "", str(assigned_to))
