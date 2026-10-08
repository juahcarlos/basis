"""Shared PostgreSQL fixtures for integration tests."""

import asyncio
import os
from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.db.session import create_engine, create_session_factory
from app.models.outbox import Outbox
from app.models.payment import Payment


@pytest.fixture
def settings(test_database_url: str) -> Settings:
    """Return settings pointed at the isolated test database."""
    get_settings.cache_clear()
    return get_settings()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def test_database_url() -> AsyncIterator[str]:
    """Create the dedicated test database and apply the project's Alembic migrations."""
    base_url = make_url(get_settings().database_url.get_secret_value())
    test_url = base_url.set(database="payments_test")
    connection = await asyncpg.connect(
        user=base_url.username,
        password=base_url.password,
        host=base_url.host,
        port=base_url.port or 5432,
        database="postgres",
    )

    try:
        # Drop only the dedicated test database so each pytest run starts from a clean schema.
        await connection.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = 'payments_test' AND pid <> pg_backend_pid()"
        )
        await connection.execute('DROP DATABASE IF EXISTS "payments_test"')
        await connection.execute('CREATE DATABASE "payments_test"')
    finally:
        await connection.close()

    previous_database_url = os.environ.get("DATABASE_URL")
    try:
        os.environ["DATABASE_URL"] = test_url.render_as_string(hide_password=False)
        get_settings.cache_clear()
        migration = await asyncio.create_subprocess_exec(
            "alembic",
            "upgrade",
            "head",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=os.environ.copy(),
        )
        output, _ = await migration.communicate()
        if migration.returncode != 0:
            raise RuntimeError(f"Test database migration failed: {output.decode()}")

        yield test_url.render_as_string(hide_password=False)
    finally:
        if previous_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_database_url
        get_settings.cache_clear()


@pytest_asyncio.fixture
async def test_engine(test_database_url: str) -> AsyncIterator[AsyncEngine]:
    """Provide an event-loop-scoped engine connected only to payments_test."""
    engine = create_engine(test_database_url)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def test_session_factory(
    test_engine: AsyncEngine,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Create clean sessions for database-backed tests."""
    session_factory = create_session_factory(test_engine)
    async with session_factory() as session:
        await session.execute(delete(Outbox))
        await session.execute(delete(Payment))
        await session.commit()

    yield session_factory

    async with session_factory() as session:
        await session.execute(delete(Outbox))
        await session.execute(delete(Payment))
        await session.commit()


@pytest_asyncio.fixture
async def api_client(
    test_database_url: str,
    test_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncClient]:
    """Run requests through the FastAPI app with its real lifespan and PostgreSQL."""
    from app.main import app

    get_settings.cache_clear()
    async with (
        app.router.lifespan_context(app),
        AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
        ) as client,
    ):
        yield client
