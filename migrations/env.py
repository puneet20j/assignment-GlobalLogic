import asyncio

from alembic import context

from app.models import engine


def run(connection):
    # migrations are hand written, so no target_metadata
    context.configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()


async def main():
    async with engine.connect() as connection:
        await connection.run_sync(run)
    await engine.dispose()


asyncio.run(main())
