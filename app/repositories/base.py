"""Common asynchronous repository operations."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import Base


class BaseRepository[ModelT: Base]:
    """Provide basic persistence operations for one ORM model."""

    def __init__(self, session: AsyncSession, model: type[ModelT]) -> None:
        self.session = session
        self.model = model

    async def add(self, entity: ModelT) -> ModelT:
        """Stage an entity for insertion in the current transaction."""
        # The unit of work owns the commit boundary.
        self.session.add(entity)
        return entity

    async def get(self, entity_id: UUID) -> ModelT | None:
        """Load one entity by its primary key."""
        # Preserve the model-specific type for callers.
        return await self.session.get(self.model, entity_id)
