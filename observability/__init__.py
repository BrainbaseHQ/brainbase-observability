"""Brainbase shared observability package.

Vendor this directory verbatim into any Python service. Call
`init_observability(service_name=...)` once at startup, then use
`get_logger(__name__)` everywhere. Inside `except` blocks, prefer
`logger.exception("...")` over `logger.error("...")` so the traceback
is captured.

Logs are emitted in three places simultaneously (best-effort, fire-and-forget):
  1. stdout (always — JSON in prod, pretty in dev)
  2. Grafana Loki (if GRAFANA_LOKI_URL / USER / TOKEN are set)
  3. OTel collector (if OTEL_COLLECTOR_URL is set)

Failure in any sink is swallowed and surfaced as a one-line stderr warning;
it never raises into application code.

Public API:
    init_observability(service_name, *, version=None, environment=None, level="INFO")
    get_logger(name=None) -> structlog.BoundLogger
    bind_request_id(rid: str | None = None) -> str
    current_request_id() -> str | None
    bind_context(**kv) -> None
    clear_context() -> None
"""

from __future__ import annotations

from ._core import (
    auto_instrument,
    bind_context,
    bind_request_id,
    clear_context,
    current_request_id,
    get_logger,
    init_observability,
)

__all__ = [
    "init_observability",
    "get_logger",
    "bind_request_id",
    "current_request_id",
    "bind_context",
    "clear_context",
    "auto_instrument",
]
