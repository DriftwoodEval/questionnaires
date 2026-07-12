"""Tests for the pure aggregation/formatting functions in the piecework.py script.

Importing piecework.py triggers a real NetworkSink socket connection at module
scope (unrelated to the functions under test), so we stub out NetworkSink's
connection step for the duration of the import only. No business logic below
is mocked.
"""

from datetime import datetime

import pandas as pd
import pytest

from utils.custom_types import PieceworkConfig, PieceworkCosts


def _noop_init(self, log_host, port, app_name):
    self.ip = log_host
    self.port = port
    self.app_name = app_name
    self.sock = None


@pytest.fixture(scope="module")
def piecework_module():
    from utils.misc import NetworkSink  # noqa: PLC0415

    real_init = NetworkSink.__init__
    NetworkSink.__init__ = _noop_init
    try:
        import piecework  # noqa: PLC0415
    finally:
        NetworkSink.__init__ = real_init
    return piecework


def make_appointment(
    evaluator_npi: int | None = 111,
    da_eval: str | None = "EVAL",
    asd_adhd=None,
    cancelled=False,
    client_name="Client A",
    start_time=datetime(2024, 1, 1, 10, 0),
):
    return {
        "id": "1",
        "evaluatorNpi": evaluator_npi,
        "clientName": client_name,
        "startTime": start_time,
        "daEval": da_eval,
        "asdAdhd": asd_adhd,
        "cancelled": cancelled,
        "placeholder": False,
        "locationKey": "main",
        "calendarEventId": "cal-1",
    }


EVALUATORS = {111: {"providerName": "Dr. A"}, 222: {"providerName": "Dr. B"}}


class TestGetWorkCounts:
    @pytest.mark.parametrize(
        ("appointments", "report_clients", "expected"),
        [
            (
                [
                    make_appointment(evaluator_npi=111, da_eval="EVAL"),
                    make_appointment(evaluator_npi=111, da_eval="EVAL"),
                    make_appointment(evaluator_npi=222, da_eval="DA"),
                ],
                None,
                {"Dr. A": {"EVAL": 2}, "Dr. B": {"DA": 1}},
            ),
            (
                [make_appointment(evaluator_npi=111, da_eval="EVAL", cancelled=True)],
                None,
                {},
            ),
            (
                [make_appointment(evaluator_npi=111, da_eval="DA", asd_adhd="ADHD")],
                None,
                {"Dr. A": {"ADHDDA": 1}},
            ),
            (
                [
                    make_appointment(evaluator_npi=None, da_eval="EVAL"),
                    make_appointment(evaluator_npi=111, da_eval=None),
                ],
                None,
                {},
            ),
            (
                [make_appointment(evaluator_npi=999, da_eval="EVAL")],
                None,
                {"Unknown Evaluator (NPI: 999)": {"EVAL": 1}},
            ),
            (
                [make_appointment(evaluator_npi=111, da_eval="EVAL")],
                pd.DataFrame([{"Writer Name": "Dr. A"}]),
                {"Dr. A": {"EVAL": 1, "REPORT": 1}},
            ),
            (
                [],
                pd.DataFrame([{"Writer Name": "Freelancer"}]),
                {"Freelancer": {"REPORT": 1}},
            ),
        ],
        ids=[
            "counts_by_evaluator_and_type",
            "cancelled_appointments_excluded",
            "da_with_adhd_diagnosis_counted_separately",
            "missing_npi_or_daeval_skipped",
            "unknown_evaluator_gets_placeholder_name",
            "report_clients_add_report_counts_to_existing_evaluator",
            "report_clients_with_unknown_writer_creates_new_entry",
        ],
    )
    def test_get_work_counts(
        self, piecework_module, appointments, report_clients, expected
    ):
        counts = piecework_module.get_work_counts(
            appointments, EVALUATORS, report_clients
        )
        assert counts == expected


class TestPrepareSummaryData:
    def test_builds_name_type_and_total_rows(self, piecework_module, config_factory):
        config = config_factory(
            piecework=PieceworkConfig(
                costs={"default": PieceworkCosts(DA=10.0, EVAL=20.0)},
                name_map={},
                payroll_emails={},
            )
        )
        work_counts = {"Dr. A": {"DA": 2, "EVAL": 1}}
        rows = piecework_module.prepare_summary_data(work_counts, config)

        assert rows[0] == {
            "NAME": "Dr. A",
            "TYPE": "",
            "COUNT": "",
            "UNIT": "",
            "COST": "",
            "TOTAL PAY": "",
        }
        assert rows[1]["TYPE"] == "DA"
        assert rows[1]["COUNT"] == 2
        assert rows[1]["UNIT"] == 10.0
        assert rows[1]["COST"] == 20.0
        assert rows[2]["TYPE"] == "EVAL"
        assert rows[2]["COST"] == 20.0
        assert rows[3]["TOTAL PAY"] == 40.0
        assert rows[4] == {
            "NAME": "",
            "TYPE": "",
            "COUNT": "",
            "UNIT": "",
            "COST": "",
            "TOTAL PAY": "",
        }

    def test_workers_sorted_alphabetically(self, piecework_module, config_factory):
        config = config_factory(
            piecework=PieceworkConfig(
                costs={"default": PieceworkCosts(DA=10.0)},
                name_map={},
                payroll_emails={},
            )
        )
        work_counts = {"Zed": {"DA": 1}, "Alice": {"DA": 1}}
        rows = piecework_module.prepare_summary_data(work_counts, config)
        name_rows = [r["NAME"] for r in rows if r["NAME"]]
        assert name_rows == ["Alice", "Zed"]


class TestPrepareDetailData:
    def test_builds_rows_per_worker(self, piecework_module):
        appointments = [
            make_appointment(
                evaluator_npi=111,
                da_eval="EVAL",
                client_name="Client A",
                start_time=datetime(2024, 1, 2, 9, 0),
            ),
        ]
        details = piecework_module.prepare_detail_data(appointments, EVALUATORS, None)
        assert details["Dr. A"] == [
            {
                "WORKER": "Dr. A",
                "CLIENT": "Client A",
                "START TIME": "2024-01-02 09:00 AM",
                "TYPE": "EVAL",
            }
        ]
        assert details["__COMBINED_DETAIL_DATA__"] == details["Dr. A"]

    def test_cancelled_appointments_excluded(self, piecework_module):
        appointments = [make_appointment(cancelled=True)]
        details = piecework_module.prepare_detail_data(appointments, EVALUATORS, None)
        assert details["__COMBINED_DETAIL_DATA__"] == []

    def test_report_clients_add_report_rows(self, piecework_module):
        report_clients = pd.DataFrame(
            [{"Writer Name": "Dr. A", "Client Name": "Client B"}]
        )
        details = piecework_module.prepare_detail_data([], EVALUATORS, report_clients)
        assert details["Dr. A"] == [
            {
                "WORKER": "Dr. A",
                "CLIENT": "Client B",
                "START TIME": "N/A",
                "TYPE": "REPORT",
            }
        ]

    def test_details_sorted_by_start_time_within_worker(self, piecework_module):
        appointments = [
            make_appointment(client_name="Later", start_time=datetime(2024, 1, 5)),
            make_appointment(client_name="Earlier", start_time=datetime(2024, 1, 1)),
        ]
        details = piecework_module.prepare_detail_data(appointments, EVALUATORS, None)
        clients_in_order = [row["CLIENT"] for row in details["Dr. A"]]
        assert clients_in_order == ["Earlier", "Later"]
