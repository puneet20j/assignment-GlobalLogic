"""tenants, tasks, processed_updates

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # slug is unique and tenants are never deleted, so a slug stays reserved forever
    op.execute("""
        CREATE TABLE tenants (
            id         uuid PRIMARY KEY,
            slug       varchar(28) NOT NULL UNIQUE,
            name       text NOT NULL,
            status     varchar(20) NOT NULL CHECK (status IN
                ('provisioning', 'active', 'updating', 'destroying', 'destroyed', 'failed')),
            version    integer NOT NULL DEFAULT 1,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TABLE tasks (
            id           uuid PRIMARY KEY,
            tenant_id    uuid NOT NULL REFERENCES tenants (id),
            type         varchar(20) NOT NULL CHECK (type IN ('deploy', 'update', 'destroy')),
            status       varchar(20) NOT NULL CHECK (status IN
                ('accepted', 'in_progress', 'done', 'failed')),
            error        text,
            created_at   timestamptz NOT NULL DEFAULT now(),
            updated_at   timestamptz NOT NULL DEFAULT now(),
            published_at timestamptz
        )
    """)
    op.execute("CREATE INDEX ix_tasks_tenant ON tasks (tenant_id, created_at)")
    # what the relay polls, stays tiny because published rows drop out of it
    op.execute("CREATE INDEX ix_tasks_unpublished ON tasks (created_at) WHERE published_at IS NULL")
    # safety net: the tenant status guards are what prevent a second open task
    op.execute("""
        CREATE UNIQUE INDEX uq_tasks_one_open ON tasks (tenant_id)
        WHERE status IN ('accepted', 'in_progress')
    """)
    op.execute("""
        CREATE TABLE processed_updates (
            update_id    varchar(100) PRIMARY KEY,
            task_id      uuid NOT NULL REFERENCES tasks (id),
            status       varchar(20) NOT NULL,
            processed_at timestamptz NOT NULL DEFAULT now()
        )
    """)


def downgrade():
    op.execute("DROP TABLE processed_updates")
    op.execute("DROP TABLE tasks")
    op.execute("DROP TABLE tenants")
