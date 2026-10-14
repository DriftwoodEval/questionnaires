"""Records this project's job runs in emr_task.

The tracking logic is shared with winnonah and lives in
`driftwood_common.task_tracker`. This wrapper supplies the database connection
built from the project's `Config`.
"""

from __future__ import annotations

from contextlib import AbstractContextManager

from driftwood_common.task_tracker import NoOpTaskHandle, TaskHandle
from driftwood_common.task_tracker import track_task as _track_task

from utils.custom_types import Config
from utils.database import get_db


def track_task(
    config: Config,
    task_type: str,
    label: str,
    exclusive: bool = True,
    *,
    dev_mode: bool = False,
) -> AbstractContextManager[TaskHandle | NoOpTaskHandle | None]:
    """Record a job run, optionally guarding against overlapping runs.

    With `dev_mode`, nothing is written and a no-op handle is yielded. With
    `exclusive` (the default), yields None if another run of this task type is
    still in progress, in which case the caller should return without doing
    any work.
    """
    return _track_task(
        lambda: get_db(config), task_type, label, exclusive, dev_mode=dev_mode
    )
