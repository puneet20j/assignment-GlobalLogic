import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import add_error_handlers, router
from app.relay import run_relay
from app.sqs import queue_url, sqs_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

@asynccontextmanager
async def lifespan(app):
    async with sqs_client() as sqs:
        tasks_url = await queue_url(sqs, "tasks")
        relay = asyncio.create_task(run_relay(sqs, tasks_url))
        yield
        relay.cancel()

app = FastAPI(title="Tenant Control Plane")
app.include_router(router)
add_error_handlers(app)

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}