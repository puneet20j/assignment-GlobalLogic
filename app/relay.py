import asyncio
import logging

from botocore.exceptions import ClientError
from sqlalchemy import func, select

from app.models import (
    Session,
    Task,
    TaskStatus,
    Tenant,
)
from app.schemas import (
    TaskMessage,
    TenantRef,
)
from app.sqs import retry_delay
from app.tenants import finish_task

logger = logging.getLogger(__name__)


async def publish_one(sqs, queue_url):
    """Sends the oldest unpublished task in its own transaction"""
    async with Session.begin() as session:
        result = await session.execute(
            select(Task, Tenant)
            .join(Tenant)
            .where(Task.published_at.is_(None), Task.status == TaskStatus.ACCEPTED)
            .order_by(Task.created_at)
            .limit(1)
            .with_for_update(of=Task, skip_locked=True)
        )
        row = result.first()
        if row is None:
            return False

        task, tenant = row
        message = TaskMessage(
            id=task.id,
            type=task.type,
            tenant_id=task.tenant_id,
            status=task.status,
            created_at=task.created_at,
            tenant=TenantRef(slug=tenant.slug, name=tenant.name),
        )
        try:
            await sqs.send_message(QueueUrl=queue_url, MessageBody=message.model_dump_json())
        except ClientError as exc:
            if "InvalidMessageContents" not in str(exc):
                raise
            logger.error("task %s can't be published: %s", task.id, exc)
            await finish_task(session, task, TaskStatus.FAILED, "could not be published")
            return True
        task.published_at = func.now()
    return True


async def run_relay(sqs, queue_url):
    attempt = 0
    while True:
        try:
            if not await publish_one(sqs, queue_url):
                await asyncio.sleep(0.3)
            attempt = 0
        except Exception:
            delay = retry_delay(attempt)
            logger.exception("relay failed, retrying in %.1fs", delay)
            await asyncio.sleep(delay)
            attempt += 1
