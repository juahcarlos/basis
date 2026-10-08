"""Factories for asynchronous SQLAlchemy database resources."""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_engine(database_url: str) -> AsyncEngine:
    """Create an async engine with stale-connection checks enabled."""
    # Validate pooled connections before handing them to request work.
    return create_async_engine(database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create an async session factory that keeps loaded values available after commit."""
    # Keep ORM values readable after a transaction commits and expires its session state.
    return async_sessionmaker(engine, expire_on_commit=False)
