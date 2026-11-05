import asyncio
import logging
import os
import random
from datetime import UTC, datetime

from app.models import TaskStatus
from app.schemas import TaskMessage, TaskUpdate
from app.sqs import queue_url, sqs_client

FAIL_RATE = float(os.environ.get("WORKER_FAIL_RATE", "0"))
DUPLICATE_RATE = float(os.environ.get("WORKER_DUPLICATE_RATE", "0"))
REORDER = os.environ.get("WORKER_REORDER", "false").lower() == "true"
MIN_DELAY_MS = int(os.environ.get("WORKER_MIN_DELAY_MS", "500"))
MAX_DELAY_MS = int(os.environ.get("WORKER_MAX_DELAY_MS", "2000"))

logger = logging.getLogger("worker")

def make_update(task_id, status, error=None):
    # same id on every attempt, so a redelivered task is deduped on the key alone
    return TaskUpdate(
        update_id=f"{task_id}:{status}",
        task_id=task_id,
        status=status,
        occurred_at=datetime.now(UTC),
        error=error,
    )

async def run_task(sqs, updates_url, task):
    async def report(update):
        body = update.model_dump_json()
        await sqs.send_message(QueueUrl=updates_url, MessageBody=body)
        if random.random() < DUPLICATE_RATE:
            await sqs.send_message(QueueUrl=updates_url, MessageBody=body)

    started = make_update(task.id, TaskStatus.IN_PROGRESS)
    if random.random() < FAIL_RATE:
        outcome = make_update(task.id, TaskStatus.FAILED, "simulated failure")
    else:
        outcome = make_update(task.id, TaskStatus.DONE)

    delay = random.uniform(MIN_DELAY_MS, MAX_DELAY_MS) / 1000
    if REORDER:
        await asyncio.sleep(delay)
        await report(outcome)
        await report(started)
    else:
        await report(started)
        await asyncio.sleep(delay)
        await report(outcome)

async def handle(sqs, tasks_url, updates_url, message):
    try:
        task = TaskMessage.model_validate_json(message["Body"])
        await run_task(sqs, updates_url, task)
        # delete only after the updates are out, a crash before this means redelivery
        await sqs.delete_message(QueueUrl=tasks_url, ReceiptHandle=message["ReceiptHandle"])
        logger.info("%s task %s for %s finished", task.type, task.id, task.tenant.slug)
    except Exception:
        logger.exception("message %s failed, it will come back", message["MessageId"])

async def main():
    async with sqs_client() as sqs:
        tasks_url = await queue_url(sqs, "tasks")
        updates_url = await queue_url(sqs, "task-updates")
        logger.info("worker started")
        while True:
            try:
                resp = await sqs.receive_message(
                    QueueUrl=tasks_url, MaxNumberOfMessages=10, WaitTimeSeconds=5
                )
            except Exception:
                logger.exception("receiving tasks failed, retrying")
                await asyncio.sleep(1)
                continue
            # tasks of different tenants are independent, run the batch concurrently
            messages = resp.get("Messages", [])
            await asyncio.gather(*(handle(sqs, tasks_url, updates_url, m) for m in messages))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.run(main())