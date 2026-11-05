import asyncio
import uuid
from datetime import UTC, datetime

import httpx
import pytest
from sqlalchemy import func

from app.consumer import apply_update
from app.models import Session, Task, TaskType, Tenant, TenantStatus
from app.schemas import TaskUpdate
from app.sqs import queue_url, sqs_client

API = "http://app:8000/v1"
N = 30


@pytest.fixture
async def client():
    async with httpx.AsyncClient(base_url=API, timeout=30) as c:
        for _ in range(60):  # the app may still be starting
            try:
                await c.get("/tenants")
                break
            except httpx.TransportError:
                await asyncio.sleep(1)
        yield c


def new_slug():
    return f"t-{uuid.uuid4().hex[:12]}"


async def create(client):
    resp = await client.post("/tenants", json={"slug": new_slug(), "name": "Acme"})
    assert resp.status_code == 202
    return resp.json()["tenant"]


async def wait_for(client, tenant_id, status):
    for _ in range(100):
        tenant = (await client.get(f"/tenants/{tenant_id}")).json()
        if tenant["status"] == status:
            return tenant
        await asyncio.sleep(0.1)
    raise AssertionError(f"tenant is '{tenant['status']}', expected '{status}'")


async def seed_task():
    async with Session.begin() as session:
        tenant = Tenant(slug=new_slug(), name="Seed", status=TenantStatus.PROVISIONING)
        session.add(tenant)
        await session.flush()
        task = Task(tenant_id=tenant.id, type=TaskType.DEPLOY, published_at=func.now())
        session.add(task)
    return tenant.id, task.id


def update_msg(task_id, status):
    return TaskUpdate(
        update_id=f"{task_id}:{status}",
        task_id=task_id,
        status=status,
        occurred_at=datetime.now(UTC),
    )


async def state(tenant_id, task_id):
    async with Session() as session:
        tenant = await session.get(Tenant, tenant_id)
        task = await session.get(Task, task_id)
    return tenant.status, tenant.version, task.status


async def test_full_lifecycle(client):
    tenant = await create(client)
    assert tenant["status"] == "provisioning"
    tenant_id = tenant["id"]

    tenant = await wait_for(client, tenant_id, "active")
    assert tenant["version"] == 2

    resp = await client.patch(f"/tenants/{tenant_id}", json={"name": "New", "version": 2})
    assert resp.status_code == 202
    tenant = await wait_for(client, tenant_id, "active")
    assert (tenant["name"], tenant["version"]) == ("New", 4)

    assert (await client.delete(f"/tenants/{tenant_id}")).status_code == 202
    assert (await wait_for(client, tenant_id, "destroyed"))["version"] == 6

    tasks = (await client.get(f"/tenants/{tenant_id}/tasks")).json()["items"]
    assert {(t["type"], t["status"]) for t in tasks} == {
        ("deploy", "done"),
        ("update", "done"),
        ("destroy", "done"),
    }


async def test_slug_stays_reserved_after_destroy(client):
    tenant = await create(client)
    await wait_for(client, tenant["id"], "active")
    await client.delete(f"/tenants/{tenant['id']}")
    await wait_for(client, tenant["id"], "destroyed")

    resp = await client.post("/tenants", json={"slug": tenant["slug"], "name": "Again"})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "tenant_already_exists"


async def test_stale_version_is_rejected(client):
    tenant = await create(client)
    await wait_for(client, tenant["id"], "active")

    resp = await client.patch(f"/tenants/{tenant['id']}", json={"name": "X", "version": 1})
    assert resp.status_code == 409
    error = resp.json()["error"]
    assert error["code"] == "tenant_version_conflict"
    assert error["details"] == {"current_version": 2, "expected_version": 1}


async def test_patch_not_allowed_while_provisioning(client):
    tenant = await create(client)
    resp = await client.patch(f"/tenants/{tenant['id']}", json={"name": "X", "version": 1})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "tenant_update_not_allowed"


@pytest.mark.parametrize(
    "method, path, body, status, code",
    [
        ("POST", "/tenants", {"slug": "Bad_Slug", "name": "x"}, 422, "validation_error"),
        ("POST", "/tenants", {"slug": "okay", "name": "x", "a": 1}, 422, "validation_error"),
        ("GET", "/tenants?limit=101", None, 422, "validation_error"),
        ("GET", f"/tenants/{uuid.uuid4()}", None, 404, "tenant_not_found"),
        ("GET", f"/tasks/{uuid.uuid4()}", None, 404, "task_not_found"),
        ("GET", "/nope", None, 404, "not_found"),
        ("PUT", "/tenants", None, 405, "method_not_allowed"),
    ],
)
async def test_error_shape(client, method, path, body, status, code):
    resp = await client.request(method, path, json=body)
    assert resp.status_code == status
    assert resp.json()["error"]["code"] == code


async def test_pagination_and_filters(client):
    await create(client)
    await create(client)

    page = (await client.get("/tenants", params={"limit": 1})).json()
    assert len(page["items"]) == 1
    assert page["has_more"] is True

    tasks = (await client.get("/tasks", params={"type": "deploy", "limit": 5})).json()["items"]
    assert tasks
    assert all(t["type"] == "deploy" for t in tasks)


async def test_concurrent_creates_one_winner(client):
    body = {"slug": new_slug(), "name": "Race"}
    responses = await asyncio.gather(*(client.post("/tenants", json=body) for _ in range(N)))
    assert sorted(r.status_code for r in responses) == [202] + [409] * (N - 1)


async def test_concurrent_patches_one_winner(client):
    tenant = await create(client)
    await wait_for(client, tenant["id"], "active")

    url = f"/tenants/{tenant['id']}"
    body = {"name": "Race", "version": 2}
    responses = await asyncio.gather(*(client.patch(url, json=body) for _ in range(N)))
    assert sorted(r.status_code for r in responses) == [202] + [409] * (N - 1)
    codes = {r.json()["error"]["code"] for r in responses if r.status_code == 409}
    assert codes == {"tenant_version_conflict"}


async def test_concurrent_deletes_one_winner(client):
    tenant = await create(client)
    await wait_for(client, tenant["id"], "active")

    url = f"/tenants/{tenant['id']}"
    responses = await asyncio.gather(*(client.delete(url) for _ in range(N)))
    assert sorted(r.status_code for r in responses) == [202] + [409] * (N - 1)


async def test_duplicate_update_is_applied_once():
    tenant_id, task_id = await seed_task()
    msg = update_msg(task_id, "done")

    assert await apply_update(msg) == "applied"
    assert await apply_update(msg) == "duplicate"
    assert await state(tenant_id, task_id) == ("active", 2, "done")


async def test_concurrent_deliveries_apply_once():
    tenant_id, task_id = await seed_task()
    msg = update_msg(task_id, "done")

    results = await asyncio.gather(*(apply_update(msg) for _ in range(10)))
    assert sorted(results) == ["applied"] + ["duplicate"] * 9
    assert await state(tenant_id, task_id) == ("active", 2, "done")


async def test_done_can_overtake_in_progress():
    tenant_id, task_id = await seed_task()

    assert await apply_update(update_msg(task_id, "done")) == "applied"
    assert await apply_update(update_msg(task_id, "in_progress")) == "stale"
    assert await state(tenant_id, task_id) == ("active", 2, "done")


async def test_terminal_status_never_changes():
    tenant_id, task_id = await seed_task()

    assert await apply_update(update_msg(task_id, "failed")) == "applied"
    assert await apply_update(update_msg(task_id, "done")) == "stale"
    assert await state(tenant_id, task_id) == ("failed", 2, "failed")


async def test_unknown_task_is_refused():
    with pytest.raises(ValueError):
        await apply_update(update_msg(uuid.uuid4(), "done"))


async def test_poison_message_reaches_dlq():
    body = f"not json {uuid.uuid4().hex}"
    async with sqs_client() as sqs:
        updates = await queue_url(sqs, "task-updates")
        dlq = await queue_url(sqs, "task-updates-dlq")
        await sqs.send_message(QueueUrl=updates, MessageBody=body)

        for _ in range(45):
            resp = await sqs.receive_message(QueueUrl=dlq, WaitTimeSeconds=2)
            for message in resp.get("Messages", []):
                await sqs.delete_message(QueueUrl=dlq, ReceiptHandle=message["ReceiptHandle"])
                if message["Body"] == body:
                    return
    raise AssertionError("poison message never reached the DLQ")
