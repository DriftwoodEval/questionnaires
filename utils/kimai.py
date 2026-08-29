from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
from loguru import logger

from utils.custom_types import KimaiService

PAGE_SIZE = 100
TIMEOUT = 30


def _session(kimai: KimaiService) -> requests.Session:
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {kimai.token}"})
    return session


def _fetch_lookup(
    session: requests.Session, base: str, resource: str, label_keys: list[str]
) -> dict[int, str]:
    """Fetch a Kimai collection and map each entity id to the first populated
    label key (alias, name, etc.)."""
    try:
        response = session.get(f"{base}/api/{resource}", timeout=TIMEOUT)
        response.raise_for_status()
    except requests.RequestException:
        logger.warning(f"Could not fetch Kimai {resource} for the export")
        return {}

    lookup: dict[int, str] = {}
    for item in response.json():
        label = next(
            (str(item[key]) for key in label_keys if item.get(key)),
            str(item.get("id")),
        )
        lookup[item["id"]] = label
    return lookup


def _fetch_timesheets(
    session: requests.Session, base: str, start_date: date, end_date: date
) -> list[dict] | None:
    params = {
        "begin": f"{start_date.isoformat()}T00:00:00",
        "end": f"{end_date.isoformat()}T23:59:59",
        # Without this Kimai only returns the token owner's own entries.
        "user": "all",
        "size": PAGE_SIZE,
    }

    records: list[dict] = []
    page = 1
    while True:
        try:
            response = session.get(
                f"{base}/api/timesheets",
                params={**params, "page": page},
                timeout=TIMEOUT,
            )
        except requests.RequestException:
            logger.exception("Failed to fetch Kimai timesheets")
            return None

        if not response.ok:
            logger.error(
                f"Kimai timesheets request failed: {response.status_code} "
                f"{response.url} -> {response.text[:500]}"
            )
            return None

        batch = response.json()
        logger.debug(
            f"Kimai timesheets page {page}: {len(batch)} entries "
            f"(X-Total-Count={response.headers.get('X-Total-Count')})"
        )
        records.extend(batch)
        if len(batch) < PAGE_SIZE:
            return records
        page += 1


def _to_rows(records: list[dict], lookups: dict[str, dict]) -> list[dict]:
    users = lookups["users"]
    activities = lookups["activities"]
    customers = lookups["customers"]
    project_customers = lookups["project_customers"]

    rows = []
    for record in records:
        begin = record.get("begin")
        start = datetime.fromisoformat(begin) if begin else None
        project_id = record.get("project")
        customer_id = project_customers.get(project_id) if project_id else None
        duration = record.get("duration") or 0
        rows.append(
            {
                "Date": start.strftime("%Y-%m-%d") if start else "",
                "Start": start.strftime("%H:%M") if start else "",
                "User": users.get(record.get("user"), str(record.get("user", ""))),
                "Customer": customers.get(customer_id, ""),
                "Activity": activities.get(
                    record.get("activity"), str(record.get("activity", ""))
                ),
                "Description": record.get("description") or "",
                "Hours": round(duration / 3600, 2),
                "Rate": record.get("rate") or 0,
                "Tags": ", ".join(record.get("tags") or []),
            }
        )
    return rows


def export_timesheets(
    kimai: KimaiService, start_date: date, end_date: date, output_dir: Path
) -> Path | None:
    """Fetch every Kimai timesheet entry in the date range, resolve the user,
    activity and customer names, and write them to an Excel file.

    Returns the file path, or None if the fetch failed or there were no entries.
    """
    session = _session(kimai)
    base = kimai.url.rstrip("/")

    records = _fetch_timesheets(session, base, start_date, end_date)
    if records is None:
        return None
    if not records:
        logger.info("No Kimai timesheet entries found for the selected range")
        return None

    users = _fetch_lookup(session, base, "users", ["alias", "username"])
    activities = _fetch_lookup(session, base, "activities", ["name"])
    customers = _fetch_lookup(session, base, "customers", ["name"])

    project_customers: dict[int, int] = {}
    try:
        response = session.get(f"{base}/api/projects", timeout=TIMEOUT)
        response.raise_for_status()
        for item in response.json():
            if item.get("customer"):
                project_customers[item["id"]] = item["customer"]
    except requests.RequestException:
        logger.warning("Could not fetch Kimai projects for the export")

    rows = _to_rows(
        records,
        {
            "users": users,
            "activities": activities,
            "customers": customers,
            "project_customers": project_customers,
        },
    )
    df = pd.DataFrame(rows).sort_values(["User", "Date", "Start"])
    summary = _hours_by_person(df)

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = (
        output_dir
        / f"kimai_{start_date.strftime('%y-%m-%d')}_{end_date.strftime('%y-%m-%d')}.xlsx"
    )
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        summary.to_excel(writer, index=False, sheet_name="Hours by Person")
        df.to_excel(writer, index=False, sheet_name="Timesheets")
    logger.success(f"Wrote Kimai export: {filename} ({len(df)} entries)")
    return filename


def _hours_by_person(df: pd.DataFrame) -> pd.DataFrame:
    """One row per person with the total hours worked."""
    return (
        df.groupby(["User"], as_index=False)["Hours"]
        .sum()
        .round({"Hours": 2})
        .rename(columns={"User": "Person"})
        .sort_values(["Person"])
    )
