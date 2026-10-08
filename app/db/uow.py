"""Unit of Work for transactions spanning payment and outbox repositories."""

from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.repositories.outbox import OutboxRepository
from app.repositories.payment import PaymentRepository


class UnitOfWork:
    """Create repositories around one session and control its transaction."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self.session: AsyncSession | None = None
        self.payments: PaymentRepository
        self.outbox: OutboxRepository

    async def __aenter__(self) -> "UnitOfWork":
        """Open a session and bind both repositories to it."""
        self.session = self._session_factory()
        self.payments = PaymentRepository(self.session)
        self.outbox = OutboxRepository(self.session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Roll back failed work and always close the session."""
        if self.session is None:
            # Enter may fail before a session is assigned.
            return

        # Uncommitted writes must not survive a failed unit of work.
        if exc_type is not None:
            await self.session.rollback()

        await self.session.close()

    async def commit(self) -> None:
        """Commit all changes made through this unit of work."""
        if self.session is None:
            # A commit outside the context manager has no transaction to commit.
            raise RuntimeError("UnitOfWork must be entered before commit")

        await self.session.commit()
