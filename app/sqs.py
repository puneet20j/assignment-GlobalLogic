import asyncio
import logging
import os
import random

import aioboto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


def retry_delay(attempt):
    """Exponential backoff with jitter"""
    return min(30, 2**attempt) * random.uniform(0.5, 1)


def sqs_client():
    return aioboto3.Session().client(
        "sqs",
        endpoint_url=os.environ.get("SQS_ENDPOINT_URL"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


async def queue_url(sqs, name):
    while True:
        try:
            return (await sqs.get_queue_url(QueueName=name))["QueueUrl"]
        except (BotoCoreError, ClientError) as exc:
            logger.warning("queue %s not ready: %s", name, exc)
            await asyncio.sleep(1)
