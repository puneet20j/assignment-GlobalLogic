import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, FastAPI, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from app import tenants
from app.models import Task, TaskStatus, TaskType, Tenant, TenantStatus
from app.schemas import Page, TaskOut, TenantCreate, TenantOut, TenantUpdate, TenantWithTask
from app.tenants import ApiError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1")
Limit = Annotated[int, Query(ge=1, le=100)]
Offset = Annotated[int, Query(ge=0)]


@router.post("/tenants", status_code=202, response_model=TenantWithTask)
async def create_tenant(body: TenantCreate, response: Response):
    result = await tenants.create_tenant(body.slug, body.name)
    response.headers["Location"] = f"/v1/tenants/{result['tenant'].id}"
    return result


@router.get("/tenants", response_model=Page[TenantOut])
async def list_tenants(status: TenantStatus | None = None, limit: Limit = 20, offset: Offset = 0):
    return await tenants.page(Tenant, limit, offset, status=status)


@router.get("/tenants/{tenant_id}", response_model=TenantOut)
async def get_tenant(tenant_id: uuid.UUID):
    return await tenants.get_tenant(tenant_id)


@router.patch("/tenants/{tenant_id}", status_code=202, response_model=TenantWithTask)
async def update_tenant(tenant_id: uuid.UUID, body: TenantUpdate):
    return await tenants.update_tenant(tenant_id, body.name, body.version)


@router.delete("/tenants/{tenant_id}", status_code=202, response_model=TenantWithTask)
async def delete_tenant(tenant_id: uuid.UUID, version: Annotated[int | None, Query(ge=1)] = None):
    return await tenants.delete_tenant(tenant_id, version)


@router.get("/tenants/{tenant_id}/tasks", response_model=Page[TaskOut])
async def list_tenant_tasks(
    tenant_id: uuid.UUID,
    status: TaskStatus | None = None,
    type: TaskType | None = None,
    limit: Limit = 20,
    offset: Offset = 0,
):
    await tenants.get_tenant(tenant_id)
    return await tenants.page(Task, limit, offset, tenant_id=tenant_id, status=status, type=type)


@router.get("/tasks", response_model=Page[TaskOut])
async def list_tasks(
    tenant_id: uuid.UUID | None = None,
    status: TaskStatus | None = None,
    type: TaskType | None = None,
    limit: Limit = 20,
    offset: Offset = 0,
):
    return await tenants.page(Task, limit, offset, tenant_id=tenant_id, status=status, type=type)


@router.get("/tasks/{task_id}", response_model=TaskOut)
async def get_task(task_id: uuid.UUID):
    return await tenants.get_task(task_id)


def error_response(status, code, message, details=None):
    body = {"error": {"code": code, "message": message, "details": details or {}}}
    return JSONResponse(status_code=status, content=body)


def add_error_handlers(app: FastAPI):
    @app.exception_handler(ApiError)
    async def api_error(request: Request, exc: ApiError):
        return error_response(exc.http_status, exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        errors = [{"loc": list(e["loc"]), "message": e["msg"]} for e in exc.errors()]
        return error_response(422, "validation_error", "invalid request", {"errors": errors})

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException):
        code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "http_error")
        return error_response(exc.status_code, code, str(exc.detail))

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception):
        logger.exception("unhandled error on %s %s", request.method, request.url.path)
        return error_response(500, "internal_error", "internal server error")
