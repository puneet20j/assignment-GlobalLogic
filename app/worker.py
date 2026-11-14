"""Worker simulator: runs the tasks from `tasks` and reports progress on `task-updates`.

Every setting is a CLI flag whose default comes from an env var, so scenarios can be
picked either way:

    python -m app.worker --fail-rate=1 --reorder
    WORKER_FAIL_RATE=1 WORKER_REORDER=true python -m app.worker
"""

import argparse
import asyncio
import logging
import os
import random
from datetime import UTC, datetime

from app.models import TaskStatus
from app.schemas import TaskMessage, TaskUpdate
from app.sqs import queue_url, retry_delay, sqs_client

logger = logging.getLogger("worker")


def rate(value):
    number = float(value)
    if not 0 <= number <= 1:
        raise argparse.ArgumentTypeError(f"{value} is not between 0 and 1")
    return number


def delay_ms(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError(f"{value} is negative")
    return number


def parse_args(argv=None):
    env = os.environ.get
    parser = argparse.ArgumentParser(
        prog="python -m app.worker",
        description="Simulates provisioning. Each flag defaults to the env var in brackets.",
    )
    parser.add_argument(
        "--fail-rate",
        type=rate,
        default=env("WORKER_FAIL_RATE", "0"),
        help="probability (0-1) that a task ends failed [WORKER_FAIL_RATE, default 0]",
    )
    parser.add_argument(
        "--duplicate-rate",
        type=rate,
        default=env("WORKER_DUPLICATE_RATE", "0"),
        help="probability (0-1) that each update is sent twice [WORKER_DUPLICATE_RATE, default 0]",
    )
    parser.add_argument(
        "--reorder",
        action=argparse.BooleanOptionalAction,
        default=env("WORKER_REORDER", "false").lower() == "true",
        help="send the terminal update before in_progress [WORKER_REORDER, default false]",
    )
    parser.add_argument(
        "--min-delay-ms",
        type=delay_ms,
        default=env("WORKER_MIN_DELAY_MS", "500"),
        help="minimum simulated work time [WORKER_MIN_DELAY_MS, default 500]",
    )
    parser.add_argument(
        "--max-delay-ms",
        type=delay_ms,
        default=env("WORKER_MAX_DELAY_MS", "2000"),
        help="maximum simulated work time [WORKER_MAX_DELAY_MS, default 2000]",
    )
    args = parser.parse_args(argv)
    if args.min_delay_ms > args.max_delay_ms:
        parser.error("--min-delay-ms must not be greater than --max-delay-ms")
    return args


def make_update(task_id, status, error=None):
    # same id on every attempt, so a redelivered task is deduped on the key alone
    return TaskUpdate(
        update_id=f"{task_id}:{status}",
        task_id=task_id,
        status=status,
        occurred_at=datetime.now(UTC),
        error=error,
    )


async def run_task(sqs, updates_url, task, cfg):
    async def report(update):
        body = update.model_dump_json()
        await sqs.send_message(QueueUrl=updates_url, MessageBody=body)
        if random.random() < cfg.duplicate_rate:
            await sqs.send_message(QueueUrl=updates_url, MessageBody=body)

    started = make_update(task.id, TaskStatus.IN_PROGRESS)
    if random.random() < cfg.fail_rate:
        outcome = make_update(task.id, TaskStatus.FAILED, "simulated failure")
    else:
        outcome = make_update(task.id, TaskStatus.DONE)

    delay = random.uniform(cfg.min_delay_ms, cfg.max_delay_ms) / 1000
    if cfg.reorder:
        await asyncio.sleep(delay)
        await report(outcome)
        await report(started)
    else:
        await report(started)
        await asyncio.sleep(delay)
        await report(outcome)


async def handle(sqs, tasks_url, updates_url, message, cfg):
    try:
        task = TaskMessage.model_validate_json(message["Body"])
        await run_task(sqs, updates_url, task, cfg)
        # delete only after the updates are out, a crash before this means redelivery
        await sqs.delete_message(QueueUrl=tasks_url, ReceiptHandle=message["ReceiptHandle"])
        logger.info("%s task %s for %s finished", task.type, task.id, task.tenant.slug)
    except Exception:
        logger.exception("message %s failed, it will come back", message["MessageId"])


async def main(cfg):
    async with sqs_client() as sqs:
        tasks_url = await queue_url(sqs, "tasks")
        updates_url = await queue_url(sqs, "task-updates")
        logger.info(
            "worker started: fail_rate=%s duplicate_rate=%s reorder=%s delay=%s-%sms",
            cfg.fail_rate,
            cfg.duplicate_rate,
            cfg.reorder,
            cfg.min_delay_ms,
            cfg.max_delay_ms,
        )
        attempt = 0
        while True:
            try:
                resp = await sqs.receive_message(
                    QueueUrl=tasks_url, MaxNumberOfMessages=10, WaitTimeSeconds=5
                )
            except Exception:
                delay = retry_delay(attempt)
                logger.exception("receiving tasks failed, retrying in %.1fs", delay)
                await asyncio.sleep(delay)
                attempt += 1
                continue
            attempt = 0
            # tasks of different tenants are independent, run the batch concurrently
            messages = resp.get("Messages", [])
            await asyncio.gather(*(handle(sqs, tasks_url, updates_url, m, cfg) for m in messages))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    asyncio.run(main(parse_args()))
