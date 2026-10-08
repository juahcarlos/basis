"""Run Alembic migrations with the application's async database URL."""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

import app.models
from app.config import get_settings

config = context.config

# Load configured log handlers when the Alembic ini file is present.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = app.models.Outbox.metadata


def run_migrations_offline() -> None:
    """Configure Alembic to generate SQL without opening a database connection."""
    context.configure(
        url=get_settings().database_url.get_secret_value(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run migrations on the synchronous connection supplied by SQLAlchemy."""
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Open an async connection and delegate migration work to Alembic."""
    engine = create_async_engine(
        get_settings().database_url.get_secret_value(),
        poolclass=pool.NullPool,
    )

    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await engine.dispose()


def run_migrations_online() -> None:
    """Run Alembic migrations through an async SQLAlchemy engine."""
    asyncio.run(run_async_migrations())


# Use SQL rendering only when Alembic runs without a live connection.
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()