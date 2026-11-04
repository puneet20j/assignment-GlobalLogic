import uuid
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, StringConstraints

from app.models import TaskStatus, TaskType, TenantStatus

def format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")

Timestamp = Annotated[datetime, PlainSerializer(format_timestamp, return_type=str)]
Slug = Annotated[str, Field(pattern=r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$")]
Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]


class TenantCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: Slug
    name: Name

class TenantUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Name
    version: int = Field(ge=1)


class TenantOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    slug: str
    name: str
    status: TenantStatus
    version: int
    created_at: Timestamp
    updated_at: Timestamp

class TaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: TaskType
    tenant_id: uuid.UUID
    status: TaskStatus
    error: str | None
    created_at: Timestamp
    updated_at: Timestamp

class TenantWithTask(BaseModel):
    tenant: TenantOut
    task: TaskOut

class Page[T](BaseModel):
    items: list[T]
    limit: int
    offset: int
    has_more: bool