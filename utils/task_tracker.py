"""
Tracks background job runs in emr_task (shared with the winnonah app's
MySQL database) so the frontend can show a live "tasks in progress"
indicator, and guards each job type against overlapping runs using a MySQL
named lock.
"""

from __future__ import annotations

from loguru import logger

from utils.custom_types import Config
from utils.database import get_db


class TaskHandle:
    def __init__(self, connection, lock_name: str, task_id: int) -> None:
        self._connection = connection
        self._lock_name = lock_name
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

    def complete(self, detail: str | None = None) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE emr_task
                SET status = 'completed', completed_at = NOW(), detail = COALESCE(%s, detail)
                WHERE id = %s
                """,
                (detail, self.task_id),
            )
        self._connection.commit()
        self._release()

    def fail(self, error: str) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE emr_task
                SET status = 'failed', completed_at = NOW(), error = %s
                WHERE id = %s
                """,
                (error[:2000], self.task_id),
            )
        self._connection.commit()
        self._release()

    def _release(self) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute("SELECT RELEASE_LOCK(%s)", (self._lock_name,))
        self._connection.close()


def start_task(config: Config, task_type: str, label: str) -> TaskHandle | None:
    """Records a job run as a row in emr_task and holds a MySQL named lock
    for the task type. Returns None if another run of this task type
    already holds the lock, in which case the caller should skip the run.
    The caller must call complete() or fail() on the returned handle when
    the job finishes.
    """
    connection = get_db(config)
    lock_name = f"task:{task_type}"

    with connection.cursor() as cursor:
        cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (lock_name,))
        row = cursor.fetchone()
        acquired = row is not None and row["acquired"] == 1

    if not acquired:
        logger.info(f"Skipping {task_type} run: a previous run is still in progress.")
        connection.close()
        return None

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

    return TaskHandle(connection, lock_name, task_id)
