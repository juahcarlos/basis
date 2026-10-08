"""FastAPI dependencies for database access and payment services."""

from typing import Annotated

from fastapi import Depends, Request

from app.config import Settings, get_settings
from app.db.uow import UnitOfWork
from app.services.payment_service import PaymentService


def get_payment_service(request: Request) -> PaymentService:
    """Build the payment service with the application's session factory."""
    # Each operation opens its own unit of work from the app-scoped factory.
    session_factory = request.app.state.session_factory
    # A fresh unit of work prevents sessions from leaking across requests.
    return PaymentService(lambda: UnitOfWork(session_factory))


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
