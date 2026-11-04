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
  

async def get_tenant(tenant_id):
    async with Session() as session:
        tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise ApiError(404, "tenant_not_found", f"tenant {tenant_id} not found")
    return tenant

async def get_task(task_id):
    async with Session() as session:
        task = await session.get(Task, task_id)
    if task is None:
        raise ApiError(404, "task_not_found", f"task {task_id} not found")
    return task

async def page(model, limit, offset, **filters):
    """Newest first. Fetches one extra row to know about the next page without a COUNT."""
    active = {key: value for key, value in filters.items() if value is not None}
    stmt = select(model).filter_by(**active).order_by(model.created_at.desc(), model.id.desc())
    async with Session() as session:
        rows = list(await session.scalars(stmt.limit(limit + 1).offset(offset)))
    return {"items": rows[:limit], "limit": limit, "offset": offset, "has_more": len(rows) > limit}