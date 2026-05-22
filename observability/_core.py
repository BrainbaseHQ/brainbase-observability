"""Core observability wiring: contextvars, structlog config, stdlib bridge.

v0.4.0 — shipping layer removed. Logs go to stdout as JSON; Render Log Stream
(or any other stdout-forwarder) is responsible for getting them to a backend
like Datadog / Loki / wherever. No more in-process HTTP push from this package.

What this module gives you:
  - Async-safe context (`bind_thread_id` / `bind_request_id` / `bind_context`)
    survives `await` boundaries via contextvars.
  - structlog configured with a `_inject_context` processor so every event
    auto-carries `thread_id` / `request_id` / `service` / `environment`.
  - A stdlib `LogRecord` factory so library logs (httpx, asyncpg,
    temporalio, redis, etc.) also carry the same correlation IDs.
  - JSON-to-stdout in prod, pretty console in dev (controlled by
    `LOG_PRETTY=1` or `DEPLOYMENT_ENVIRONMENT in {dev, local}`).

What's gone (vs v0.3.x):
  - `_LokiSender` — replaced by Render Log Stream → wherever.
  - `_init_otel` / `HTTPJSONLogExporter` — same.
  - `auto_instrument()` — wasn't doing anything useful without an OTel
    provider configured. Use `ddtrace-run` or `opentelemetry-instrument`
    if you want auto-instrumentation.
  - `GRAFANA_LOKI_*` and `OTEL_COLLECTOR_URL` env vars — no longer read.
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from contextvars import ContextVar
from typing import Any

import structlog

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_initialized: bool = False
_service_name: str = "unknown-service"
_service_version: str = "unknown"
_environment: str = "unknown"
_internal_logger = logging.getLogger("observability._core")

# Async-safe context. Survives await boundaries.
# `_extra_context_var` defaults to None (NOT a shared `{}`): every reader
# converts to a dict, so we never have to defend against accidental in-place
# mutation of a shared default object.
_request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)
_thread_id_var: ContextVar[str | None] = ContextVar("thread_id", default=None)
_extra_context_var: ContextVar[dict[str, Any] | None] = ContextVar(
    "extra_context", default=None
)


# ---------------------------------------------------------------------------
# Public context API
# ---------------------------------------------------------------------------

def bind_request_id(rid: str | None = None) -> str:
    """Bind a request_id to the current async context. Returns the bound id.

    If `rid` is None or empty, a new UUID4 hex is generated.
    """
    if not rid:
        rid = uuid.uuid4().hex
    _request_id_var.set(rid)
    return rid


def current_request_id() -> str | None:
    return _request_id_var.get()


def bind_thread_id(tid: str | None) -> str | None:
    """Bind a Brainbase thread_id to the current async context. Returns the bound id.

    Unlike `bind_request_id`, this does NOT generate a synthetic value when
    `tid` is None — thread_id is a domain identifier; we either have it from
    the inbound request / job payload or we don't. Pass None to explicitly
    clear (e.g., at job teardown).
    """
    _thread_id_var.set(tid)
    return tid


def current_thread_id() -> str | None:
    return _thread_id_var.get()


def bind_context(**kv: Any) -> None:
    """Add arbitrary key/value pairs to the current context's structured log fields."""
    current = dict(_extra_context_var.get() or {})
    current.update(kv)
    _extra_context_var.set(current)


def clear_context() -> None:
    """Clear request_id, thread_id, and bound context. Call at request end."""
    _request_id_var.set(None)
    _thread_id_var.set(None)
    _extra_context_var.set(None)


# ---------------------------------------------------------------------------
# Stdlib LogRecord factory: inject contextvars onto every record
# ---------------------------------------------------------------------------

def _install_stdlib_contextvar_bridge() -> None:
    """Wrap Python's LogRecord factory so every stdlib log line carries
    Brainbase observability contextvars as record attributes.

    Without this, stdlib loggers (temporalio.activity, httpx, sqlalchemy,
    redis-py, asyncpg, anything calling `logging.getLogger(...)`) reach
    stdout as plain text with no `thread_id` / `request_id` fields. A
    downstream query like `@thread_id:508ce84f` in Datadog would miss
    every stdlib line.

    The LogRecord factory is a process-global hook (`logging.setLogRecordFactory`)
    so this enriches EVERY record regardless of which logger emitted it.
    Idempotent: a module-level flag on the `logging` module prevents
    wrapper stacking on repeated `init_observability()` calls.
    """
    if getattr(logging, "_brainbase_obs_factory_installed", False):
        return
    parent_factory = logging.getLogRecordFactory()

    def _factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = parent_factory(*args, **kwargs)
        # Don't clobber attributes a caller (or another factory) set first.
        if not hasattr(record, "thread_id"):
            tid = _thread_id_var.get()
            if tid is not None:
                record.thread_id = tid
        if not hasattr(record, "request_id"):
            rid = _request_id_var.get()
            if rid is not None:
                record.request_id = rid
        if not hasattr(record, "service"):
            record.service = _service_name
        if not hasattr(record, "environment"):
            record.environment = _environment
        return record

    logging.setLogRecordFactory(_factory)
    setattr(logging, "_brainbase_obs_factory_installed", True)


# ---------------------------------------------------------------------------
# structlog processors: inject context into every event dict
# ---------------------------------------------------------------------------

def _inject_context(_logger: Any, _method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    rid = _request_id_var.get()
    if rid:
        event_dict.setdefault("request_id", rid)
    tid = _thread_id_var.get()
    if tid:
        event_dict.setdefault("thread_id", tid)
    extras = _extra_context_var.get() or {}
    if extras:
        for k, v in extras.items():
            event_dict.setdefault(k, v)
    event_dict.setdefault("service", _service_name)
    event_dict.setdefault("service_version", _service_version)
    event_dict.setdefault("environment", _environment)
    return event_dict


def _add_severity(_logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    event_dict["level"] = method_name.upper()
    return event_dict


# ---------------------------------------------------------------------------
# init_observability — the public entry point
# ---------------------------------------------------------------------------

def init_observability(
    service_name: str,
    *,
    version: str | None = None,
    environment: str | None = None,
    level: str = "INFO",
    instrument: bool = True,  # accepted for back-compat with v0.3.x callers; ignored.
) -> None:
    """Initialize structured logging to stdout. Idempotent.

    Call ONCE at process startup, before any `get_logger(...)` calls.

    Env vars read:
      LOG_LEVEL                — overrides `level`
      SERVICE_VERSION / DEPLOYMENT_ENVIRONMENT — overrides version/environment args
      LOG_PRETTY               — "1" forces console renderer (default in non-prod)

    NO LONGER read (v0.4.0):
      GRAFANA_LOKI_URL / GRAFANA_LOKI_USER / GRAFANA_LOKI_TOKEN
      OTEL_COLLECTOR_URL
      OBSERVABILITY_DISABLE_AUTOINSTRUMENT

    Logs are written as JSON to stdout. Render Log Stream (or equivalent)
    forwards them to whatever backend you've configured at the platform
    level. There is no in-process HTTP push from this package.

    Args:
      instrument: accepted for back-compat with v0.3.x callers; ignored in
        v0.4.0+. Use `ddtrace-run` or `opentelemetry-instrument` as the
        process launcher if you want auto-instrumentation.
    """
    global _initialized, _service_name, _service_version, _environment
    del instrument  # back-compat shim — accepted but unused.

    if _initialized:
        return

    _service_name = service_name
    _service_version = os.getenv("SERVICE_VERSION") or version or "unknown"
    _environment = os.getenv("DEPLOYMENT_ENVIRONMENT") or environment or os.getenv("ENV", "unknown")

    log_level = os.getenv("LOG_LEVEL", level).upper()
    level_num = getattr(logging, log_level, logging.INFO)

    # Configure the stdlib root logger so libraries logging via `logging.getLogger`
    # also flow to stdout in a consistent shape.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level_num,
        force=True,
    )

    # Install the LogRecord factory so stdlib log records carry contextvars
    # as structured attributes.
    _install_stdlib_contextvar_bridge()

    pretty = os.getenv("LOG_PRETTY", "0") == "1" or _environment in {"dev", "local"}
    renderer: Any = (
        structlog.dev.ConsoleRenderer(colors=True)
        if pretty
        else structlog.processors.JSONRenderer()
    )

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        _add_severity,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        _inject_context,
        renderer,
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level_num),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _initialized = True

    # First log line announces the configuration so we can confirm in the
    # log destination (e.g., Datadog).
    logger = structlog.get_logger("observability")
    logger.info(
        "observability_initialized",
        service=service_name,
        version=_service_version,
        environment=_environment,
        log_level=log_level,
        sink="stdout",  # v0.4.0 — shipping is the platform's job.
    )


def get_logger(name: str | None = None) -> Any:
    """Return a structlog logger bound to the calling module.

    If `init_observability()` hasn't run yet, structlog still returns a logger
    that writes to stdout — but without context enrichment. Safe to call from
    module top-level.
    """
    if name is None:
        return structlog.get_logger()
    return structlog.get_logger(name)
