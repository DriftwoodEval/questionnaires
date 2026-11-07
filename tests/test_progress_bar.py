import pytest

from utils.progress_bar import PinnedProgressBar, render_bar


def styles(total, completed, failed, width):
    return [
        str(span.style) for span in render_bar(total, completed, failed, width).spans
    ]


@pytest.mark.parametrize(
    ("total", "completed", "failed", "width", "expected"),
    [
        # One cell per client.
        (4, 2, set(), 4, ["green", "green", "bright_black", "bright_black"]),
        (4, 3, {2}, 4, ["green", "red", "green", "bright_black"]),
        # Wide bar: each client spans several cells, so a failure is several cells wide.
        (2, 1, {1}, 4, ["red", "red", "bright_black", "bright_black"]),
        # Narrow bar: a cell covering several clients is red if any of them failed.
        (4, 4, {3}, 2, ["green", "red"]),
    ],
)
def test_render_bar(total, completed, failed, width, expected):
    assert styles(total, completed, failed, width) == expected


def test_skipped_counts_notice_clients_that_did_not_fail():
    bar = PinnedProgressBar(5)
    for client, noticed, failed in [
        (1, True, False),
        (2, True, True),
        (3, False, False),
    ]:
        bar.completed = client
        if noticed:
            bar.mark_notice()
        if failed:
            bar.mark_failed()
    assert bar.skipped == 1


def test_questionnaire_bar_resets_per_client_and_marks_failed_slot():
    bar = PinnedProgressBar(3)
    bar.start_questionnaires(3, done=1)
    bar.questionnaire_attempted()
    bar.questionnaire_attempted(failed=True)
    assert (bar.q_attempted, bar.q_failed) == (3, {3})
    bar.advance_to(2)
    assert bar.q_total is None
