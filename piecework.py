import json
import re
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import inquirer
import pandas as pd
from loguru import logger

from utils.database import get_all_evaluators_info, get_appointments
from utils.google import get_punch_list
from utils.misc import load_config
from utils.types import Appointment, Config

TRACKING_FILE = Path("piecework_output") / "reports_tracking.json"


def load_tracked_reports() -> dict[str, str]:
    """Load the set of previously tracked client IDs from disk."""
    if TRACKING_FILE.exists():
        try:
            with open(TRACKING_FILE, "r") as f:
                data = json.load(f)
                return data.get("client_ids", [])
        except Exception:
            logger.exception("Failed to load previously tracked client IDs")
            return {}
    return {}


def save_tracked_reports(client_ids: dict[str, str]):
    """Save the set of tracked client IDs to disk."""
    TRACKING_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(TRACKING_FILE, "w") as f:
            json.dump({"client_ids": client_ids}, f, indent=2)
        logger.info(f"Saved {len(client_ids)} entries to {TRACKING_FILE}")
    except Exception:
        logger.exception("Failed to save tracking file")


def extract_writer_initials(assigned_to: str) -> str:
    """Extract only letters from the assigned to column."""
    if pd.isna(assigned_to) or not assigned_to:
        return ""
    return re.sub(r"[^a-zA-Z]", "", assigned_to)


def get_report_clients(config: Config) -> Optional[pd.DataFrame]:
    """Find clients who have reports done, and who either: haven't been ran before, or were ran on the same day."""
    punch_list = get_punch_list(config)

    if punch_list is None:
        logger.critical("Punch list is empty")
        return None

    report_done = punch_list[
        (punch_list["Billed?"] == "TRUE")
        & (punch_list["AJP Review Done/Hold for payroll"] != "TRUE")
    ].copy()

    if report_done.empty:
        logger.info("No clients found that have reports done")
        return None

    tracked_reports = load_tracked_reports()
    today_str = date.today().strftime("%Y-%m-%d")

    logger.info(f"Loaded report history: {len(tracked_reports)}")

    new_reports = report_done[
        report_done["Client ID"].apply(
            lambda cid: cid not in tracked_reports
            or tracked_reports.get(cid) == today_str
        )
    ].copy()

    if new_reports.empty:
        logger.info("No new reports found")
        return None

    for client_id in new_reports["Client ID"]:
        if client_id not in tracked_reports:
            tracked_reports[client_id] = today_str

    save_tracked_reports(tracked_reports)

    result = new_reports[
        [
            "Client Name",
            "Client ID",
            "Assigned to OR added to report writing folder",
        ]
    ].rename(columns={"Assigned to OR added to report writing folder": "Assigned To"})

    result["Initials"] = result["Assigned To"].apply(extract_writer_initials)
    result["Writer Name"] = result["Initials"].apply(
        lambda initials: config.piecework.get_full_name(initials) if initials else ""
    )

    result = result.drop(columns=["Initials", "Assigned To"])

    logger.info(f"Found {len(result)} new reports")

    return result


def get_date_range() -> Optional[tuple[date, date]]:
    """Prompt the user to select a date range (last week or week before)."""
    today = date.today()
    days_since_last_sunday = (today.weekday() + 1) % 7
    most_recent_sunday = today - timedelta(days=days_since_last_sunday)
    last_full_week_sunday = most_recent_sunday - timedelta(days=7)
    last_full_week_saturday = last_full_week_sunday + timedelta(days=6)

    week_before_sunday = last_full_week_sunday - timedelta(days=7)
    week_before_saturday = week_before_sunday + timedelta(days=6)

    questions = [
        inquirer.List(
            "date_range",
            message="Select the date range for the report",
            choices=[
                (
                    f"Last week ({last_full_week_sunday.strftime('%m-%d')} to {last_full_week_saturday.strftime('%m-%d')})",
                    (last_full_week_sunday, last_full_week_saturday),
                ),
                (
                    f"Week before last ({week_before_sunday.strftime('%m-%d')} to {week_before_saturday.strftime('%m-%d')})",
                    (week_before_sunday, week_before_saturday),
                ),
            ],
        ),
    ]
    answers = inquirer.prompt(questions)
    if answers:
        date_range = answers["date_range"]
        logger.info(f"Selected range: {date_range[0]} to {date_range[1]}")
        return date_range
    else:
        logger.info("No date range selected. Exiting.")
        return None


def get_work_counts(
    appointments: list[Appointment],
    evaluators: dict[int, dict],
    report_clients: Optional[pd.DataFrame] = None,
) -> dict[str, dict[str, int]]:
    """Aggregates appointment counts by evaluator by type.

    Returns a dict mapping evaluator name to appointment type counts.
    """
    counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    worker_names: dict[int, str] = {}

    for appointment in appointments:
        if appointment.get("cancelled"):
            continue

        npi = appointment.get("evaluatorNpi")
        da_eval = appointment.get("daEval")

        if not npi or not da_eval:
            logger.warning(
                f"Skipping appointment with missing NPI or daEval: {appointment.get('id', 'N/A')}"
            )
            continue

        evaluator_info = evaluators.get(npi)
        evaluator_name = (
            evaluator_info.get("providerName")
            if evaluator_info and evaluator_info.get("providerName")
            else f"Unknown Evaluator (NPI: {npi})"
        )

        worker_names[npi] = str(evaluator_name)
        counts[npi][str(da_eval)] += 1

    if report_clients is not None and not report_clients.empty:
        for _, row in report_clients.iterrows():
            writer_name = row.get("Writer Name", "")
            if writer_name:
                if writer_name not in worker_names.values():
                    worker_names[writer_name] = writer_name
                    counts[writer_name]["REPORT"] = (
                        counts[writer_name].get("REPORT", 0) + 1
                    )
                else:
                    for npi, name in worker_names.items():
                        if name == writer_name:
                            counts[npi]["REPORT"] = counts[npi].get("REPORT", 0) + 1

    return {
        worker_names.get(npi, f"Unknown Evaluator (NPI: {npi})"): dict(da_eval_counts)
        for npi, da_eval_counts in counts.items()
    }


def prepare_summary_data(
    work_counts: dict[str, dict[str, int]], config: Config
) -> list[dict]:
    """Prepares the aggregated appointment counts for the Summary Counts DataFrame.

    The evaluator name appears on its own row, followed by rows with type/count data.
    """
    summary_rows = []

    for worker_name, app_counts in work_counts.items():
        # Add name row with no type/count
        summary_rows.append(
            {
                "NAME": worker_name,
                "TYPE": "",
                "COUNT": "",
                "UNIT": "",
                "COST": "",
                "TOTAL PAY": "",
            }
        )

        # Add type/count rows with no name
        sorted_app_types = sorted(app_counts.keys())
        evaluator_total = 0.00

        for app_type in sorted_app_types:
            count = app_counts[app_type]
            unit_cost = config.piecework.get_unit_cost(worker_name, app_type)
            total_cost = count * unit_cost
            evaluator_total += total_cost
            summary_rows.append(
                {
                    "NAME": "",
                    "TYPE": app_type,
                    "COUNT": count,
                    "UNIT": f"${unit_cost:.2f}",
                    "COST": f"${total_cost:.2f}",
                    "TOTAL PAY": "",
                }
            )

        summary_rows.append(
            {
                "NAME": "",
                "TYPE": "",
                "COUNT": "",
                "UNIT": "",
                "COST": "",
                "TOTAL PAY": f"${evaluator_total:.2f}",
            }
        )

        summary_rows.append(
            {
                "NAME": "",
                "TYPE": "",
                "COUNT": "",
                "UNIT": "",
                "COST": "",
                "TOTAL PAY": "",
            }
        )

    return summary_rows


def prepare_detail_data(
    appointments: list[Appointment],
    evaluators: dict[int, dict],
    config: Config,
    report_clients: Optional[pd.DataFrame],
) -> list[dict]:
    """Prepares the flat list of non-cancelled appointment details for the Details DataFrame."""
    detail_rows = []
    for appointment in appointments:
        if appointment.get("cancelled"):
            continue

        npi = appointment.get("evaluatorNpi")
        da_eval = appointment.get("daEval")

        if npi and da_eval:
            evaluator_info = evaluators.get(npi)
            evaluator_name = (
                evaluator_info.get("providerName")
                if evaluator_info and evaluator_info.get("providerName")
                else f"Unknown Evaluator (NPI: {npi})"
            )

            detail_rows.append(
                {
                    "Worker": evaluator_name,
                    "Client": appointment.get("clientName", "N/A"),
                    "Start Time": appointment.get("startTime").strftime(
                        "%Y-%m-%d %I:%M %p"
                    ),
                    "Type": str(da_eval),
                }
            )

    if report_clients is not None and not report_clients.empty:
        for _, row in report_clients.iterrows():
            writer_name = row.get("Writer Name", "")
            client_name = row.get("Client Name", "")
            if writer_name:
                detail_rows.append(
                    {
                        "Worker": writer_name,
                        "Client": client_name,
                        "Start Time": "N/A",
                        "Type": "REPORT",
                    }
                )

    sorted_detail_rows = sorted(
        detail_rows, key=lambda k: (k["Worker"], k["Start Time"])
    )

    return sorted_detail_rows


def generate_excel_report(
    summary_data: list[dict],
    detail_data: list[dict],
    start_date: date,
    end_date: date,
):
    """Generates a single Excel file with two sheets: Summary and Detail."""
    piecework_output_folder = Path("piecework_output")
    piecework_output_folder.mkdir(parents=True, exist_ok=True)

    filename = (
        piecework_output_folder
        / f"piecework_{start_date.strftime('%y-%m-%d')}_{end_date.strftime('%y-%m-%d')}.xlsx"
    )

    logger.info(f"Preparing to write Excel report to {filename}...")

    df_summary = pd.DataFrame(summary_data)
    df_detail = pd.DataFrame(detail_data)

    try:
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            df_summary.to_excel(writer, sheet_name="Summary Counts", index=False)

            df_detail.to_excel(writer, sheet_name="Details", index=False)

        logger.success(f"Successfully generated Excel report file: {filename}")

    except Exception:
        logger.exception(f"An error occurred while writing the Excel file {filename}.")


def main():
    """Main function to run piecework."""
    logger.info("Starting...")

    _, config = load_config()
    date_range = get_date_range()

    if not date_range:
        return

    start_date, end_date = date_range

    evaluators = get_all_evaluators_info(config)
    if not evaluators:
        logger.error("Could not load evaluator information. Aborting.")
        return

    appointments = get_appointments(config, start_date, end_date)
    if not appointments:
        logger.info("No appointments found for the selected range.")
        appointments = []

    try:
        report_clients = get_report_clients(config)
    except Exception:
        logger.exception("Failed to fetch report clients.")
        report_clients = None

    if not appointments and (report_clients is None or report_clients.empty):
        logger.info("No appointments or report writing entries found.")
        return

    appointment_counts = get_work_counts(appointments, evaluators, report_clients)
    summary_data = prepare_summary_data(appointment_counts, config)
    detail_data = prepare_detail_data(appointments, evaluators, config, report_clients)

    if not summary_data and not detail_data:
        logger.info("No valid appointments found to include in the report.")
        return

    generate_excel_report(
        summary_data,
        detail_data,
        start_date,
        end_date,
    )


if __name__ == "__main__":
    main()
