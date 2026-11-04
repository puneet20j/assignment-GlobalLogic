import logging

from fastapi import FastAPI

from app.api import add_error_handlers, router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="Tenant Control Plane")
app.include_router(router)
add_error_handlers(app)

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}