"""Runtime fixtures for end-to-end tests using the Compose app container."""

import asyncio
import json
import os
import socket
import sys
import threading
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import monotonic
from typing import Any
from urllib.parse import quote

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@dataclass
class E2ERuntime:
    """Addresses and captured webhook calls for the real application processes."""

    api_url: str
    api_key: str
    webhook_url: str
    vhost: str
    management: httpx.AsyncClient
    webhook_calls: list[dict[str, object]]


async def wait_until[T](
    read_value: Callable[[], Awaitable[T]],
    predicate: Callable[[T], bool],
    *,
    timeout_seconds: float,
) -> T:
    """Wait for a service observation to satisfy the supplied condition."""
    deadline = monotonic() + timeout_seconds
    last_value: object = "no observation yet"
    while monotonic() < deadline:
        value = await read_value()
        last_value = value
        if predicate(value):
            return value
        await asyncio.sleep(0.25)

    raise AssertionError(f"Condition not met before timeout; last value: {last_value!r}")


@pytest.fixture
def poll_until() -> Callable[..., Coroutine[Any, Any, Any]]:
    """Expose the shared polling helper to E2E tests."""
    return wait_until


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[object]):
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _make_webhook_handler(
    webhook_calls: list[dict[str, object]],
) -> type[BaseHTTPRequestHandler]:
    class WebhookHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length)
            status = 503 if self.path == "/fail" else 200
            webhook_calls.append(
                {
                    "path": self.path,
                    "payload": json.loads(body) if body else {},
                    "status": status,
                }
            )
            self.send_response(status)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, format: str, *args: Any) -> None:
            return

    return WebhookHandler


async def _stop_process(process: asyncio.subprocess.Process | None) -> None:
    if process is None or process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=10)
    except TimeoutError:
        process.kill()
        await process.wait()


@pytest_asyncio.fixture
async def e2e_runtime(
    request: pytest.FixtureRequest,
    test_database_url: str,
    test_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[E2ERuntime]:
    """Start API and consumer subprocesses against test PostgreSQL and an isolated vhost."""
    api_port = _free_port()
    webhook_calls: list[dict[str, object]] = []
    webhook_server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _make_webhook_handler(webhook_calls),
    )
    webhook_port = int(webhook_server.server_address[1])
    webhook_thread = threading.Thread(target=webhook_server.serve_forever, daemon=True)
    webhook_thread.start()
    api_process: asyncio.subprocess.Process | None = None
    consumer_process: asyncio.subprocess.Process | None = None
    api_output_task: asyncio.Task[bytes] | None = None
    consumer_output_task: asyncio.Task[bytes] | None = None

    rabbit_user = os.environ["RABBITMQ_DEFAULT_USER"]
    rabbit_password = os.environ["RABBITMQ_DEFAULT_PASS"]
    management = httpx.AsyncClient(
        base_url=os.environ["RABBITMQ_MANAGEMENT_URL"],
        auth=(rabbit_user, rabbit_password),
        timeout=5,
    )
    vhost = f"e2e-{os.getpid()}-{api_port}"
    encoded_vhost = quote(vhost, safe="")

    try:
        create_vhost = await management.put(f"/api/vhosts/{encoded_vhost}")
        create_vhost.raise_for_status()
        set_permissions = await management.put(
            f"/api/permissions/{encoded_vhost}/{quote(rabbit_user, safe='')}",
            json={"configure": ".*", "write": ".*", "read": ".*"},
        )
        set_permissions.raise_for_status()

        allowed_host = f"127.0.0.1:{webhook_port}"
        child_environment = os.environ.copy()
        child_environment["DATABASE_URL"] = test_database_url
        child_environment["WEBHOOK_ALLOWED_HOSTS"] = json.dumps([allowed_host])

        api_process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(api_port),
            env=child_environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert api_process.stdout is not None
        api_output_task = asyncio.create_task(api_process.stdout.read())

        async def api_is_ready() -> object:
            if api_process is not None and api_process.returncode is not None:
                raise RuntimeError("API process exited during startup")
            try:
                async with httpx.AsyncClient(timeout=1) as client:
                    response = await client.get(f"http://127.0.0.1:{api_port}/health")
                return response.status_code == 200
            except httpx.RequestError:
                return False

        await wait_until(api_is_ready, bool, timeout_seconds=30)

        consumer_environment = child_environment.copy()
        consumer_environment["RABBITMQ_URL"] = (
            f"amqp://{quote(rabbit_user, safe='')}:{quote(rabbit_password, safe='')}"
            f"@rabbitmq:5672/{encoded_vhost}"
        )
        consumer_process = await asyncio.create_subprocess_exec(
            "faststream",
            "run",
            "app.consumer:app",
            env=consumer_environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert consumer_process.stdout is not None
        consumer_output_task = asyncio.create_task(consumer_process.stdout.read())

        async def read_payment_queue() -> object:
            if consumer_process is not None and consumer_process.returncode is not None:
                raise RuntimeError("Consumer process exited during startup")
            response = await management.get(f"/api/queues/{encoded_vhost}/payments.new")
            if response.status_code == httpx.codes.NOT_FOUND:
                return False
            response.raise_for_status()
            return True

        await wait_until(read_payment_queue, bool, timeout_seconds=30)

        yield E2ERuntime(
            api_url=f"http://127.0.0.1:{api_port}",
            api_key=os.environ["API_KEY"],
            webhook_url=f"http://127.0.0.1:{webhook_port}",
            vhost=vhost,
            management=management,
            webhook_calls=webhook_calls,
        )
    finally:
        await _stop_process(consumer_process)
        await _stop_process(api_process)
        failed = any(
            getattr(request.node, name, None) is not None and getattr(request.node, name).failed
            for name in ("rep_setup", "rep_call")
        )
        if failed:
            for name, output_task in (
                ("API", api_output_task),
                ("Consumer", consumer_output_task),
            ):
                if output_task is not None:
                    output = (await output_task).decode(errors="replace")
                    if output:
                        print(f"\n{name} subprocess output:\n{output}")
        await asyncio.to_thread(webhook_server.shutdown)
        webhook_server.server_close()
        await asyncio.to_thread(webhook_thread.join)
        delete_vhost = await management.delete(f"/api/vhosts/{encoded_vhost}")
        if delete_vhost.status_code not in {httpx.codes.NO_CONTENT, httpx.codes.NOT_FOUND}:
            delete_vhost.raise_for_status()
        await management.aclose()
