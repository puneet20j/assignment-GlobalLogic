import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import func, update

from app.consumer import apply_update
from app.models import (
    Session,
    Task,
    TaskStatus,
    TaskType,
    Tenant,
    TenantStatus,
)
from app.relay import task_message
from app.schemas import TaskUpdate
from app.sqs import queue_url, retry_delay, sqs_client
from app.sweeper import fail_stuck_tasks
from app.tenants import ApiError, _explain_rejection
from app.worker import parse_args
from redrive import redrive

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


async def seed_task(task_type=TaskType.DEPLOY, tenant_status=TenantStatus.PROVISIONING):
    """A tenant with one published, accepted task, bypassing the API and the worker."""
    async with Session.begin() as session:
        tenant = Tenant(slug=new_slug(), name="Seed", status=tenant_status)
        session.add(tenant)
        await session.flush()
        task = Task(tenant_id=tenant.id, type=task_type, published_at=func.now())
        session.add(task)
    return tenant.id, task.id


async def seed_tenant(status):
    """A tenant at rest in any status, with no open task."""
    async with Session.begin() as session:
        tenant = Tenant(slug=new_slug(), name="Seed", status=status)
        session.add(tenant)
    return tenant


async def task_count(client, tenant_id):
    return len((await client.get(f"/tenants/{tenant_id}/tasks")).json()["items"])


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


TRANSITIONS = {
    # status: (PATCH allowed, DELETE allowed)
    TenantStatus.PROVISIONING: (False, False),
    TenantStatus.ACTIVE: (True, True),
    TenantStatus.UPDATING: (False, False),
    TenantStatus.DESTROYING: (False, False),
    TenantStatus.DESTROYED: (False, False),
    TenantStatus.FAILED: (False, True),
}


@pytest.mark.parametrize("status", list(TRANSITIONS))
async def test_patch_guard(client, status):
    tenant = await seed_tenant(status)
    resp = await client.patch(f"/tenants/{tenant.id}", json={"name": "X", "version": 1})

    if TRANSITIONS[status][0]:
        assert resp.status_code == 202
        assert resp.json()["tenant"]["status"] == "updating"
        assert resp.json()["task"]["type"] == "update"
    else:
        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "tenant_update_not_allowed"
        assert await task_count(client, tenant.id) == 0


@pytest.mark.parametrize("status", list(TRANSITIONS))
async def test_delete_guard(client, status):
    tenant = await seed_tenant(status)
    resp = await client.delete(f"/tenants/{tenant.id}")

    if TRANSITIONS[status][1]:
        assert resp.status_code == 202
        assert resp.json()["tenant"]["status"] == "destroying"
        assert resp.json()["task"]["type"] == "destroy"
    else:
        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "tenant_update_not_allowed"
        assert await task_count(client, tenant.id) == 0


async def test_rejected_calls_create_no_task(client):
    tenant = await create(client)
    url = f"/tenants/{tenant['id']}"

    assert (await client.patch(url, json={"name": "X", "version": 1})).status_code == 409
    assert (await client.delete(url)).status_code == 409
    resp = await client.post("/tenants", json={"slug": tenant["slug"], "name": "Dup"})
    assert resp.status_code == 409

    active = await wait_for(client, tenant["id"], "active")
    assert (await client.patch(url, json={"name": "X", "version": 1})).status_code == 409
    assert active["version"] == 2
    tasks = (await client.get(f"{url}/tasks")).json()["items"]
    assert [t["type"] for t in tasks] == ["deploy"]


@pytest.mark.parametrize(
    "method, path, body, status, code",
    [
        ("POST", "/tenants", {"slug": "Bad_Slug", "name": "x"}, 422, "validation_error"),
        ("POST", "/tenants", {"slug": "okay", "name": "x", "a": 1}, 422, "validation_error"),
        ("POST", "/tenants", {"slug": "okay", "name": "a\u0000b"}, 422, "validation_error"),
        ("POST", "/tenants", {"slug": "okay", "name": "a\uffffb"}, 422, "validation_error"),
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


@pytest.mark.parametrize(
    "task_type, open_status, outcome, final_status",
    [
        (TaskType.DEPLOY, TenantStatus.PROVISIONING, "done", "active"),
        (TaskType.DEPLOY, TenantStatus.PROVISIONING, "failed", "failed"),
        (TaskType.UPDATE, TenantStatus.UPDATING, "done", "active"),
        (TaskType.UPDATE, TenantStatus.UPDATING, "failed", "failed"),
        (TaskType.DESTROY, TenantStatus.DESTROYING, "done", "destroyed"),
        (TaskType.DESTROY, TenantStatus.DESTROYING, "failed", "failed"),
    ],
)
async def test_terminal_outcome_moves_tenant(task_type, open_status, outcome, final_status):
    tenant_id, task_id = await seed_task(task_type, open_status)

    assert await apply_update(update_msg(task_id, "in_progress")) == "applied"
    assert await apply_update(update_msg(task_id, outcome)) == "applied"
    assert await state(tenant_id, task_id) == (final_status, 2, outcome)


async def test_in_progress_leaves_tenant_alone():
    tenant_id, task_id = await seed_task()

    assert await apply_update(update_msg(task_id, "in_progress")) == "applied"
    assert await state(tenant_id, task_id) == ("provisioning", 1, "in_progress")


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


async def test_published_event_carries_the_task():
    tenant_id, task_id = await seed_task()
    async with Session() as session:
        task = await session.get(Task, task_id)
        tenant = await session.get(Tenant, tenant_id)

    body = json.loads(task_message(task, tenant).model_dump_json())
    assert body["id"] == str(task_id)
    assert body["type"] == "deploy"
    assert body["tenant_id"] == str(tenant_id)
    assert body["status"] == "accepted"
    assert body["created_at"].endswith("Z")


def test_worker_flags_override_env(monkeypatch):
    monkeypatch.setenv("WORKER_FAIL_RATE", "0.5")
    monkeypatch.setenv("WORKER_REORDER", "true")

    from_env = parse_args([])
    assert (from_env.fail_rate, from_env.reorder, from_env.max_delay_ms) == (0.5, True, 2000)

    cfg = parse_args(["--fail-rate=1", "--no-reorder", "--min-delay-ms=0", "--max-delay-ms=10"])
    assert (cfg.fail_rate, cfg.reorder, cfg.min_delay_ms, cfg.max_delay_ms) == (1, False, 0, 10)


@pytest.mark.parametrize(
    "argv",
    [
        ["--fail-rate=1.5"],
        ["--duplicate-rate=-0.1"],
        ["--min-delay-ms=-1"],
        ["--min-delay-ms=900", "--max-delay-ms=100"],
    ],
)
def test_worker_rejects_bad_flags(argv):
    with pytest.raises(SystemExit):
        parse_args(argv)


def test_retry_delay_grows_and_is_capped():
    assert 0.5 <= retry_delay(0) <= 1
    assert 4 <= retry_delay(3) <= 8
    assert 15 <= retry_delay(50) <= 30


async def test_unsendable_task_fails_without_blocking_the_relay(client):
    async with Session.begin() as session:
        tenant = Tenant(slug=new_slug(), name="no\uffffgood", status=TenantStatus.PROVISIONING)
        session.add(tenant)
        await session.flush()
        task = Task(tenant_id=tenant.id, type=TaskType.DEPLOY)
        session.add(task)

    await wait_for(client, str(tenant.id), "failed")
    async with Session() as session:
        failed = await session.get(Task, task.id)
    assert failed.status == TaskStatus.FAILED
    assert failed.error == "could not be published"

    await wait_for(client, (await create(client))["id"], "active")


async def test_task_that_already_moved_on_is_not_sent_again(client):
    async with Session.begin() as session:
        tenant = Tenant(slug=new_slug(), name="Moved", status=TenantStatus.PROVISIONING)
        session.add(tenant)
        await session.flush()
        task = Task(tenant_id=tenant.id, type=TaskType.DEPLOY, status=TaskStatus.IN_PROGRESS)
        session.add(task)

    await asyncio.sleep(1)
    assert await state(tenant.id, task.id) == ("provisioning", 1, "in_progress")
    async with Session() as session:
        assert (await session.get(Task, task.id)).published_at is None


async def make_quiet(task_id, seconds=120):
    async with Session.begin() as session:
        await session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(updated_at=datetime.now(UTC) - timedelta(seconds=seconds))
        )


async def test_sweeper_fails_a_quiet_task_and_frees_the_tenant(client):
    tenant_id, task_id = await seed_task()
    await make_quiet(task_id)

    assert await fail_stuck_tasks(60) >= 1
    assert await state(tenant_id, task_id) == ("failed", 2, "failed")
    assert (await client.delete(f"/tenants/{tenant_id}")).status_code == 202


async def test_sweeper_leaves_recent_and_unpublished_tasks_alone():
    tenant_id, task_id = await seed_task()
    await fail_stuck_tasks(60)
    assert await state(tenant_id, task_id) == ("provisioning", 1, "accepted")


async def test_late_done_after_timeout_is_ignored_and_warned_about(caplog):
    tenant_id, task_id = await seed_task()
    await make_quiet(task_id)
    await fail_stuck_tasks(60)

    with caplog.at_level(logging.WARNING):
        assert await apply_update(update_msg(task_id, "done")) == "stale"

    assert "worker reported done" in caplog.text
    assert await state(tenant_id, task_id) == ("failed", 2, "failed")


async def test_redrive_brings_a_dead_lettered_update_back():
    tenant_id, task_id = await seed_task()
    async with sqs_client() as sqs:
        updates = await queue_url(sqs, "task-updates")
        dlq = await queue_url(sqs, "task-updates-dlq")
        await sqs.send_message(
            QueueUrl=dlq, MessageBody=update_msg(task_id, "done").model_dump_json()
        )
        assert await redrive(sqs, dlq, updates, limit=1) == 1

    for _ in range(100):  # the live consumer applies it
        if (await state(tenant_id, task_id))[0] == "active":
            return
        await asyncio.sleep(0.1)
    raise AssertionError("redriven update was never applied")


async def test_delete_race_message_does_not_contradict_itself():
    tenant_id, _ = await seed_task()
    async with Session.begin() as session:
        await session.execute(
            update(Tenant).where(Tenant.id == tenant_id).values(status=TenantStatus.ACTIVE)
        )

    async with Session() as session:
        with pytest.raises(ApiError) as caught:
            await _explain_rejection(session, tenant_id, None, {TenantStatus.ACTIVE})

    assert caught.value.code == "tenant_update_not_allowed"
    assert "retry" in caught.value.message
