"""FastAPI application and API-process lifespan."""

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api.v1.payments import router as payments_router
from app.config import get_settings
from app.db.session import create_engine, create_session_factory
from app.exceptions import IdempotencyConflictError, PaymentNotFoundError
from app.logging import configure_logging

logger = logging.getLogger(__name__)

# Largest request body accepted by the API, far above any valid payment request.
MAX_REQUEST_BODY_BYTES = 65536
# How long one successful readiness check is trusted.
HEALTH_CACHE_SECONDS = 5
# Serialize checks so a burst of requests opens at most one connection.
_health_lock = asyncio.Lock()
# Monotonic time until which the last successful check is trusted.
_health_ok_until = 0.0


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    """Create database resources at startup and dispose the engine at shutdown."""
    settings = get_settings()
    configure_logging(settings)
    engine = create_engine(settings.database_url.get_secret_value())
    application.state.engine = engine
    application.state.session_factory = create_session_factory(engine)

    try:
        yield
    finally:
        await engine.dispose()


async def handle_idempotency_conflict(
    request: Request,
    exception: Exception,
) -> JSONResponse:
    """Return a consistent JSON response for idempotency conflicts."""
    # Keep domain exceptions from exposing request or storage details.
    return JSONResponse(
        status_code=409,
        content={"detail": "Idempotency key was already used with different payment data"},
    )


async def handle_payment_not_found(
    request: Request,
    exception: Exception,
) -> JSONResponse:
    """Return a consistent JSON response for missing payments."""
    # Use the same public error shape for all missing IDs.
    return JSONResponse(status_code=404, content={"detail": "Payment not found"})


app = FastAPI(title="Payments API", version="1.0.0", lifespan=lifespan)
app.include_router(payments_router)
app.add_exception_handler(IdempotencyConflictError, handle_idempotency_conflict)
app.add_exception_handler(PaymentNotFoundError, handle_payment_not_found)


@app.middleware("http")
async def limit_request_body(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Reject requests whose declared body size exceeds the limit."""
    # Only a declared Content-Length can be checked before the body is read.
    content_length = request.headers.get("content-length", "0")
    # A malformed length header is ignored and left to the server to reject.
    if content_length.isdigit() and int(content_length) > MAX_REQUEST_BODY_BYTES:
        # Refuse oversized bodies before FastAPI parses them.
        return JSONResponse(status_code=413, content={"detail": "Request body too large"})

    return await call_next(request)


@app.get("/health")
async def health_check(request: Request) -> dict[str, str]:
    """Report readiness only when PostgreSQL and RabbitMQ are reachable."""
    global _health_ok_until
    async with _health_lock:
        # Serve a recent successful check without touching the dependencies again.
        if time.monotonic() < _health_ok_until:
            return {"status": "ok"}

        try:
            # A successful connection alone is insufficient; execute a lightweight query.
            async with request.app.state.engine.connect() as connection:
                await connection.execute(text("SELECT 1"))

            # Authenticate against RabbitMQ so invalid credentials also fail readiness.
            settings = get_settings()
            rabbit_connection = await aio_pika.connect(
                settings.rabbitmq_url.get_secret_value(),
                timeout=3,
            )
            await rabbit_connection.close()
        except Exception as exception:
            logger.error(
                "\n\n !!! --- ERROR --- !!! %s health_check dependency check failed e=%s\n\n",
                __file__,
                exception,
            )
            raise HTTPException(
                status_code=503,
                detail="Required dependency unavailable",
            ) from exception

        # Remember the success so repeated probes stay cheap.
        _health_ok_until = time.monotonic() + HEALTH_CACHE_SECONDS

    return {"status": "ok"}
