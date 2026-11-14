import asyncio
import logging
from datetime import timedelta

from sqlalchemy import func, select

from app.models import Session, Task, TaskStatus
from app.sqs import retry_delay
from app.tenants import finish_task

logger = logging.getLogger(__name__)

# above 300s, a task message can be retried for 5 receives of 60s before it reaches tasks-dlq
TASK_TIMEOUT_S = 360
SWEEP_INTERVAL_S = 30


async def fail_stuck_tasks(timeout_s):
    """One pass. Returns how many tasks were failed."""
    async with Session.begin() as session:
        tasks = list(
            await session.scalars(
                select(Task)
                .where(
                    Task.status.in_((TaskStatus.ACCEPTED, TaskStatus.IN_PROGRESS)),
                    Task.published_at.is_not(None),
                    Task.updated_at < func.now() - timedelta(seconds=timeout_s),
                )
                .order_by(Task.updated_at)
                .limit(100)
                .with_for_update(skip_locked=True)
            )
        )
        for task in tasks:
            logger.warning("task %s has been quiet for over %ds", task.id, timeout_s)
            error = f"no result from worker after {timeout_s}s"
            await finish_task(session, task, TaskStatus.FAILED, error)
    return len(tasks)


async def run_sweeper():
    attempt = 0
    while True:
        try:
            await fail_stuck_tasks(TASK_TIMEOUT_S)
            attempt = 0
            await asyncio.sleep(SWEEP_INTERVAL_S)
        except Exception:
            delay = retry_delay(attempt)
            logger.exception("sweep failed, retrying in %.1fs", delay)
            await asyncio.sleep(delay)
            attempt += 1
