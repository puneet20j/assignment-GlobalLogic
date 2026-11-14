"""Move DLQ message back to queue after outage
docker compose exec app python redrive.py task-updates
docker compose exec app python redrive.py tasks
"""

import asyncio
import sys

from app.sqs import queue_url, sqs_client


async def redrive(sqs, source_url, target_url, limit):
    """Send first, then delete: a crash in between can only duplicate a message."""
    moved = 0
    while moved < limit:
        resp = await sqs.receive_message(
            QueueUrl=source_url, MaxNumberOfMessages=min(10, limit - moved), WaitTimeSeconds=1
        )
        messages = resp.get("Messages", [])
        if not messages:
            break
        for message in messages:
            await sqs.send_message(QueueUrl=target_url, MessageBody=message["Body"])
            await sqs.delete_message(QueueUrl=source_url, ReceiptHandle=message["ReceiptHandle"])
            moved += 1
    return moved


async def main(queue):
    async with sqs_client() as sqs:
        source = await queue_url(sqs, f"{queue}-dlq")
        target = await queue_url(sqs, queue)
        attributes = await sqs.get_queue_attributes(
            QueueUrl=source, AttributeNames=["ApproximateNumberOfMessages"]
        )
        waiting = int(attributes["Attributes"]["ApproximateNumberOfMessages"])
        moved = await redrive(sqs, source, target, waiting)
        print(f"moved {moved} of {waiting} messages from {queue}-dlq to {queue}")


if __name__ == "__main__":
    if sys.argv[1:] not in (["tasks"], ["task-updates"]):
        sys.exit("usage: python redrive.py tasks|task-updates")
    asyncio.run(main(sys.argv[1]))
