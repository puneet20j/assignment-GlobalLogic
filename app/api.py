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

@router.post("/tenants", status_code=202, response_model=TenantWithTask)
async def create_tenant(body: TenantCreate, response: Response):
    result = await tenants.create_tenant(body.slug, body.name)
    response.headers["Location"] = f"/v1/tenants/{result['tenant'].id}"
    return result


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