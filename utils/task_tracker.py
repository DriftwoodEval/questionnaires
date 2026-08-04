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
                SET progress_current = %s, progress_total = %s, detail = COALESCE(%s, detail)
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

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO emr_task (type, status, label, started_at)
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
                    SET status = 'failed', completed_at = NOW(), error = %s
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
                    SET status = 'completed', completed_at = NOW()
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
