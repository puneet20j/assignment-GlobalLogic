import asyncio
import logging

from sqlalchemy import func, select

from app.models import Session, Task, Tenant
from app.schemas import TaskMessage, TenantRef

logger = logging.getLogger(__name__)

async def publish_one(sqs, queue_url):
    """Sends the oldest unpublished task in its own transaction"""
    async with Session.begin() as session:
        result = await session.execute(
            select(Task, Tenant)
            .join(Tenant)
            .where(Task.published_at.is_(None))
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
        await sqs.send_message(QueueUrl=queue_url, MessageBody=message.model_dump_json())
        task.published_at = func.now()
    return True

async def run_relay(sqs, queue_url):
    while True:
        try:
            if not await publish_one(sqs, queue_url):
                await asyncio.sleep(0.3)
        except Exception:
            logger.exception("relay failed, retrying")
            await asyncio.sleep(1)