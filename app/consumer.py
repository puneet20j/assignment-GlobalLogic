import asyncio
import logging

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from app.models import ProcessedUpdate, Session, Task, TaskStatus, TaskType, Tenant, TenantStatus
from app.schemas import TaskUpdate

logger = logging.getLogger(__name__)


RANK = {
    TaskStatus.ACCEPTED: 0,
    TaskStatus.IN_PROGRESS: 1,
    TaskStatus.DONE: 2,
    TaskStatus.FAILED: 2,
}

STATUS_WHILE_OPEN = {
    TaskType.DEPLOY: TenantStatus.PROVISIONING,
    TaskType.UPDATE: TenantStatus.UPDATING,
    TaskType.DESTROY: TenantStatus.DESTROYING,
}

STATUS_ON_DONE = {
    TaskType.DEPLOY: TenantStatus.ACTIVE,
    TaskType.UPDATE: TenantStatus.ACTIVE,
    TaskType.DESTROY: TenantStatus.DESTROYED,
}

async def apply_update(msg):
    async with Session.begin() as session:
        task = await session.scalar(select(Task).where(Task.id == msg.task_id).with_for_update())
        if task is None:
            raise ValueError(f"unknown task {msg.task_id}")

        recorded = await session.scalar(
            insert(ProcessedUpdate)
            .values(update_id=msg.update_id, task_id=task.id, status=msg.status)
            .on_conflict_do_nothing(index_elements=[ProcessedUpdate.update_id])
            .returning(ProcessedUpdate.update_id)
        )
        if recorded is None:
            return "duplicate"

        # accepted -> done allowed. Can overtake in_progress
        if RANK[msg.status] <= RANK[task.status]:
            return "stale"

        task.status = msg.status
        task.error = msg.error
        if msg.status != TaskStatus.IN_PROGRESS:
            await finish_tenant(session, task)
    return "applied"
  

async def finish_tenant(session, task):
    done = task.status == TaskStatus.DONE
    tenant_id = await session.scalar(
        update(Tenant)
        .where(Tenant.id == task.tenant_id, Tenant.status == STATUS_WHILE_OPEN[task.type])
        .values(
            status=STATUS_ON_DONE[task.type] if done else TenantStatus.FAILED,
            version=Tenant.version + 1, # optimistic locking
        )
        .returning(Tenant.id)
    )
    if tenant_id is None:
        logger.warning("task %s finished but tenant %s is not open", task.id, task.tenant_id)
        
        
        
async def handle(sqs, queue_url, message):
    try:
        msg = TaskUpdate.model_validate_json(message["Body"])
        result = await apply_update(msg)
        await sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
        logger.info("update %s: %s", msg.update_id, result)
    except Exception:
        logger.exception("message %s failed, it will come back", message["MessageId"])


async def run_consumer(sqs, queue_url):
    while True:
        try:
            resp = await sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5
            )
            for message in resp.get("Messages", []):
                await handle(sqs, queue_url, message)
        except Exception:
            logger.exception("receiving updates failed, retrying")
            await asyncio.sleep(1)