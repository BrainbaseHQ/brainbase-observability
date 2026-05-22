"""Brainbase shared observability package.

Install via `pip install brainbase-observability @ git+…@v0.4.0`. Call
`init_observability(service_name=...)` once at startup, then use
`get_logger(__name__)` everywhere. Inside `except` blocks, prefer
`logger.exception("...")` over `logger.error("...")` so the traceback
is captured.

v0.4.0 — logs go to stdout only. Render Log Stream (or any other
platform-level log forwarder) is responsible for shipping them to a
backend like Datadog. There is no in-process HTTP push from this
package, which means no shipping bugs and no `GRAFANA_LOKI_*` /
`OTEL_COLLECTOR_URL` env vars to manage.

If you want APM traces, run your service under `ddtrace-run` or
`opentelemetry-instrument` — both auto-instrument FastAPI / aiohttp /
httpx / asyncpg / redis / etc. and propagate trace context across
service boundaries.

Public API:
    init_observability(service_name, *, version=None, environment=None, level="INFO")
    get_logger(name=None) -> structlog.BoundLogger
    bind_request_id(rid: str | None = None) -> str
    current_request_id() -> str | None
    bind_thread_id(tid: str | None) -> str | None
    current_thread_id() -> str | None
    bind_context(**kv) -> None
    clear_context() -> None
"""

from __future__ import annotations

from ._core import (
    bind_context,
    bind_request_id,
    bind_thread_id,
    clear_context,
    current_request_id,
    current_thread_id,
    get_logger,
    init_observability,
)

__all__ = [
    "init_observability",
    "get_logger",
    "bind_request_id",
    "current_request_id",
    "bind_thread_id",
    "current_thread_id",
    "bind_context",
    "clear_context",
]
