from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from app.models import Session, Task, TaskType, Tenant, TenantStatus


class ApiError(Exception):
    def __init__(self, http_status, code, message, **details):
        self.http_status = http_status
        self.code = code
        self.message = message
        self.details = details
        

async def create_tenant(slug, name):
    async with Session.begin() as session:
        tenant = await session.scalar(
            insert(Tenant)
            .values(slug=slug, name=name, status=TenantStatus.PROVISIONING)
            .on_conflict_do_nothing(index_elements=[Tenant.slug])
            .returning(Tenant)
        )
        if tenant is None:
            raise ApiError(409, "tenant_already_exists", f"slug '{slug}' is taken", slug=slug)
        task = Task(tenant_id=tenant.id, type=TaskType.DEPLOY)
        session.add(task)
    return {"tenant": tenant, "task": task}