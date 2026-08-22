"""
Tracks background job runs in emr_task (shared with the winnonah app's
MySQL database) so the frontend can show a live "tasks in progress"
indicator, and guards each job type against overlapping runs (e.g. a cron
firing again before a slow Selenium check finishes) using a MySQL named
lock.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from loguru import logger

from utils.custom_types import Config
from utils.database import get_db

# How long a non-exclusive task run may take before we assume the process
# that owns it died without closing its row (kill -9, OOM, host crash).
STALE_TASK_HOURS = 4


def _clear_orphaned_rows(
    connection, task_type: str, older_than_hours: int | None = None
) -> None:
    """Mark leftover 'running' rows for this task type as failed.

    For exclusive tasks this runs right after acquiring the named lock,
    which guarantees no other process is genuinely running this task type
    (MySQL releases the lock when its owning connection dies), so any
    'running' row left over is provably orphaned. For non-exclusive tasks
    there's no lock to prove that, so we only sweep rows old enough that a
    real run would have finished by now.
    """
    query = """
        UPDATE emr_task
        SET status = 'failed', completedAt = NOW(),
            error = 'Orphaned: previous process died without closing this task'
        WHERE type = %s AND status = 'running'
    """
    params: tuple = (task_type,)
    if older_than_hours is not None:
        query += " AND startedAt < NOW() - INTERVAL %s HOUR"
        params = (task_type, older_than_hours)

    with connection.cursor() as cursor:
        cursor.execute(query, params)
    connection.commit()


class TaskHandle:
    def __init__(self, connection, task_id: int) -> None:
        self._connection = connection
        self.task_id = task_id

    def progress(
        self, current: int, total: int | None = None, detail: str | None = None
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE emr_task
                SET progressCurrent = %s, progressTotal = %s, detail = COALESCE(%s, detail)
                WHERE id = %s
                """,
                (current, total, detail, self.task_id),
            )
        self._connection.commit()


@contextmanager
def track_task(
    config: Config, task_type: str, label: str, exclusive: bool = True
) -> Iterator[TaskHandle | None]:
    """Records a job run as a row in emr_task.

    If exclusive is True (the default), also holds a MySQL named lock for
    the task type so a second cron-triggered run of the same job can't
    start while one is still in progress. Yields None (and does not create
    a row) if another run of this task type already holds the lock, in
    which case the caller should return without doing any work.

    If exclusive is False, no lock is taken and overlapping runs are
    allowed to proceed concurrently, each getting its own row.

    Otherwise yields a TaskHandle for reporting progress; the row is
    marked completed or failed automatically.
    """
    connection = get_db(config)
    lock_name = f"task:{task_type}"

    if exclusive:
        with connection.cursor() as cursor:
            cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (lock_name,))
            row = cursor.fetchone()
            acquired = row is not None and row["acquired"] == 1

        if not acquired:
            logger.info(
                f"Skipping {task_type} run: a previous run is still in progress."
            )
            connection.close()
            yield None
            return

        _clear_orphaned_rows(connection, task_type)
    else:
        _clear_orphaned_rows(connection, task_type, older_than_hours=STALE_TASK_HOURS)

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO emr_task (type, status, label, startedAt)
                VALUES (%s, 'running', %s, NOW())
                """,
                (task_type, label),
            )
            task_id = cursor.lastrowid
        connection.commit()

        try:
            yield TaskHandle(connection, task_id)
        except Exception as e:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE emr_task
                    SET status = 'failed', completedAt = NOW(), error = %s
                    WHERE id = %s
                    """,
                    (str(e)[:2000], task_id),
                )
            connection.commit()
            raise
        else:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE emr_task
                    SET status = 'completed', completedAt = NOW()
                    WHERE id = %s
                    """,
                    (task_id,),
                )
            connection.commit()
    finally:
        if exclusive:
            with connection.cursor() as cursor:
                cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        connection.close()
