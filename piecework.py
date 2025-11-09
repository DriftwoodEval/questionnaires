from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import inquirer
import pandas as pd
from loguru import logger

from utils.database import get_all_evaluators_info, get_appointments
from utils.misc import load_config
from utils.types import Appointment, Config


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


def get_evaluator_appointment_counts(
    appointments: list[Appointment], evaluators: dict[int, dict]
) -> dict[str, dict[str, int]]:
    """Aggregates appointment counts by evaluator by type.

    Returns a dict mapping evaluator name to appointment type counts.
    """
    counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    evaluator_names: dict[int, str] = {}

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

        evaluator_names[npi] = str(evaluator_name)
        counts[npi][str(da_eval)] += 1

    return {
        evaluator_names.get(npi, f"Unknown Evaluator (NPI: {npi})"): dict(
            da_eval_counts
        )
        for npi, da_eval_counts in counts.items()
    }


def prepare_summary_data(
    appointment_counts: dict[str, dict[str, int]], config: Config
) -> list[dict]:
    """Prepares the aggregated appointment counts for the Summary Counts DataFrame.

    The evaluator name appears on its own row, followed by rows with type/count data.
    """
    summary_rows = []

    for evaluator_name, app_counts in appointment_counts.items():
        # Add name row with no type/count
        summary_rows.append(
            {
                "NAME": evaluator_name,
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
            unit_cost = config.piecework.get_unit_cost(evaluator_name, app_type)
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
    appointments: list[Appointment], evaluators: dict[int, dict]
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
                    "Evaluator": evaluator_name,
                    "Client": appointment.get("clientName", "N/A"),
                    "Start Time": appointment.get("startTime").strftime(
                        "%Y-%m-%d %I:%M %p"
                    ),
                    "Type": str(da_eval),
                }
            )
    sorted_detail_rows = sorted(
        detail_rows, key=lambda k: (k["Evaluator"], k["Start Time"])
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
        / f"piecework_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.xlsx"
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
        return

    appointment_counts = get_evaluator_appointment_counts(appointments, evaluators)
    summary_data = prepare_summary_data(appointment_counts, config)
    detail_data = prepare_detail_data(appointments, evaluators)

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
