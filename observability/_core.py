"""Core observability wiring: contextvars, structlog config, OTel + Loki sinks.

This module is the single source of truth for how Brainbase Python services
emit logs. Keep it dependency-light: structlog, opentelemetry-{api,sdk}, httpx.

Design notes:
- Contextvars (not thread-local) so request_id survives `await` boundaries.
- structlog wraps the stdlib `logging` module so `logging.getLogger(...)` calls
  in third-party libs still flow through the same JSON pipeline.
- Loki + OTel sinks are async/batched and silently no-op when env is unset.
- `init_observability()` is idempotent; double-calls are safe.
"""

from __future__ import annotations

import atexit
import logging
import os
import queue
import sys
import threading
import time
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
    Loki and the OTel collector via the LoggingHandler bridge but with
    NO `thread_id` / `request_id` fields — only structlog calls flow
    through `_inject_context` and carry them. A Loki query like
    `{thread_id="..."}` would miss every stdlib line.

    The LogRecord factory is a process-global hook (`logging.setLogRecordFactory`)
    so this enriches EVERY record regardless of which logger emitted it.
    OTel's LoggingHandler serializes record attributes as OTel LogRecord
    attributes, so the fields ride straight through to whatever the
    collector forwards to (Grafana Loki, Tempo, etc).

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
# structlog processor: inject context into every event dict
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
# Loki async batched sender (best-effort, never raises into app)
# ---------------------------------------------------------------------------

class _LokiSender:
    """Background thread that batches log lines and pushes to Grafana Loki.

    Uses a dedicated thread + queue (not asyncio) so it works equally well
    in sync and async services without requiring an event loop.
    """

    def __init__(
        self,
        url: str,
        user: str,
        token: str,
        *,
        batch_size: int = 100,
        flush_interval: float = 1.0,
    ) -> None:
        self.url = url
        self.user = user
        self.token = token
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._q: queue.Queue[tuple[float, str, dict[str, str]]] = queue.Queue(maxsize=10_000)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="loki-sender", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._thread.join(timeout=5.0)
        except Exception:
            pass

    def submit(self, ts_seconds: float, line: str, labels: dict[str, str]) -> None:
        try:
            self._q.put_nowait((ts_seconds, line, labels))
        except queue.Full:
            # Drop on the floor rather than block the app.
            pass

    def _run(self) -> None:
        # Lazy-import httpx so unit tests that don't init obs don't pay the cost.
        import base64

        import httpx

        auth = base64.b64encode(f"{self.user}:{self.token}".encode("ascii")).decode("ascii")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth}",
        }
        client = httpx.Client(timeout=10.0)

        buf: list[tuple[float, str, dict[str, str]]] = []
        last_flush = time.monotonic()

        while not self._stop.is_set():
            try:
                timeout = max(0.05, self.flush_interval - (time.monotonic() - last_flush))
                try:
                    item = self._q.get(timeout=timeout)
                    buf.append(item)
                except queue.Empty:
                    pass

                now = time.monotonic()
                if buf and (len(buf) >= self.batch_size or (now - last_flush) >= self.flush_interval):
                    self._flush(client, headers, buf)
                    buf = []
                    last_flush = now
            except Exception as exc:  # noqa: BLE001
                # Never let the sender thread die.
                _internal_logger.warning("loki sender loop error: %s", exc)
                time.sleep(0.5)

        # Shutdown drain: pull every remaining item off the queue and flush
        # in one final batch so logs accumulated during the SIGTERM window
        # actually ship instead of getting dropped silently.
        while True:
            try:
                buf.append(self._q.get_nowait())
            except queue.Empty:
                break
        if buf:
            try:
                self._flush(client, headers, buf)
            except Exception:  # noqa: BLE001
                pass
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass

    def _flush(
        self,
        client: Any,
        headers: dict[str, str],
        items: list[tuple[float, str, dict[str, str]]],
    ) -> None:
        # Group by label set since Loki streams are keyed by labels.
        groups: dict[tuple, list[tuple[float, str]]] = {}
        for ts, line, labels in items:
            key = tuple(sorted(labels.items()))
            groups.setdefault(key, []).append((ts, line))

        streams = []
        for key, entries in groups.items():
            labels_dict = dict(key)
            values = [[str(int(ts * 1e9)), line] for ts, line in entries]
            streams.append({"stream": labels_dict, "values": values})

        payload = {"streams": streams}
        try:
            r = client.post(self.url, json=payload, headers=headers, timeout=10.0)
            if r.status_code >= 400:
                _internal_logger.warning(
                    "loki push %s: %s", r.status_code, r.text[:200]
                )
        except Exception as exc:  # noqa: BLE001
            _internal_logger.warning("loki push failed: %s", exc)


_loki_sender: _LokiSender | None = None
_otel_logger_provider: Any | None = None


def _loki_processor(_logger: Any, _method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """structlog processor that fans the rendered event out to Loki."""
    if _loki_sender is None:
        return event_dict
    try:
        # Render a JSON copy for Loki without mutating the event_dict that
        # later processors will see (the final renderer formats stdout).
        import json as _json

        line = _json.dumps(event_dict, default=str, separators=(",", ":"))
        labels = {
            "service": str(event_dict.get("service", _service_name)),
            "level": str(event_dict.get("level", "INFO")).lower(),
            "environment": str(event_dict.get("environment", _environment)),
        }
        _loki_sender.submit(time.time(), line, labels)
    except Exception as exc:  # noqa: BLE001
        _internal_logger.warning("loki processor error: %s", exc)
    return event_dict


# Maps structlog method names → OTel SeverityNumber values (per OTel spec).
_OTEL_SEVERITY: dict[str, int] = {
    "debug": 5,
    "info": 9,
    "warning": 13,
    "warn": 13,
    "error": 17,
    "critical": 21,
    "exception": 17,
}


def _otel_processor(
    _logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Forward every structlog event to the OTel collector.

    The stdlib `LoggingHandler` attached in `_init_otel` only sees calls made
    via `logging.getLogger(...).<level>(...)`. Because structlog's
    `PrintLoggerFactory` writes via `print()`, structlog events bypass the
    stdlib logging tree entirely. This processor closes that gap by emitting
    one OTel LogRecord per structlog event, in addition to the existing Loki
    + stdout sinks.
    """
    if _otel_logger_provider is None:
        return event_dict
    try:
        import json as _json

        # LogRecord is the API-level class exported from `opentelemetry._logs`
        # (the SDK package re-exports ReadableLogRecord / ReadWriteLogRecord
        # but NOT LogRecord itself — that path raises ImportError at runtime).
        from opentelemetry._logs import LogRecord

        body = _json.dumps(event_dict, default=str, separators=(",", ":"))
        severity = _OTEL_SEVERITY.get(method_name.lower(), 9)
        otel_logger = _otel_logger_provider.get_logger("brainbase-observability")
        otel_logger.emit(
            LogRecord(
                timestamp=int(time.time() * 1e9),
                severity_number=severity,
                severity_text=method_name.upper(),
                body=body,
                attributes={
                    "service": event_dict.get("service", _service_name),
                    "request_id": event_dict.get("request_id"),
                    "environment": event_dict.get("environment", _environment),
                },
            )
        )
    except Exception as exc:  # noqa: BLE001
        _internal_logger.warning("otel processor error: %s", exc)
    return event_dict


# ---------------------------------------------------------------------------
# OTel logs init (HTTPJSONLogExporter — borrowed from kafka-vm-proxy, fixed)
# ---------------------------------------------------------------------------

def _init_otel(collector_url: str) -> None:
    """Wire OTel logs export to the custom HTTP/JSON collector.

    Best-effort: any failure leaves the app running with structlog→stdout still working.

    Note: imports under `opentelemetry._logs` / `opentelemetry.sdk._logs` use
    the underscore-prefixed paths intentionally. As of opentelemetry-sdk 1.42
    the public modules at `opentelemetry.logs` / `opentelemetry.sdk.logs` do
    NOT exist; the logs SDK is shipped under the experimental namespace.
    """
    global _otel_logger_provider
    try:
        from opentelemetry import _logs as otel_logs
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import Resource

        from ._otel_exporter import HTTPJSONLogExporter

        resource = Resource.create(
            {
                "service.name": _service_name,
                "service.version": _service_version,
                "deployment.environment": _environment,
            }
        )
        provider = LoggerProvider(resource=resource)
        exporter = HTTPJSONLogExporter(endpoint=collector_url)
        # 5s shutdown timeout (was 30s): cron containers receive SIGTERM with
        # a hard kill deadline; we'd rather drop a final batch than stall the
        # process for half a minute waiting for a sluggish collector to ACK.
        provider.add_log_record_processor(
            BatchLogRecordProcessor(
                exporter,
                schedule_delay_millis=1000,
                export_timeout_millis=5000,
                max_queue_size=2048,
                max_export_batch_size=512,
            )
        )
        otel_logs.set_logger_provider(provider)
        _otel_logger_provider = provider

        # Bridge stdlib logging → OTel so any third-party `logger.error(...)`
        # also flows to the collector. Structlog events flow via
        # `_otel_processor` (see `init_observability`) because they bypass
        # stdlib's logging tree.
        handler = LoggingHandler(level=logging.NOTSET, logger_provider=provider)
        logging.getLogger().addHandler(handler)

        atexit.register(provider.shutdown)
    except Exception as exc:  # noqa: BLE001
        _internal_logger.warning("OTel init failed (continuing without): %s", exc)


# ---------------------------------------------------------------------------
# init_observability — the public entry point
# ---------------------------------------------------------------------------

def init_observability(
    service_name: str,
    *,
    version: str | None = None,
    environment: str | None = None,
    level: str = "INFO",
    instrument: bool = True,
) -> None:
    """Initialize structured logging + remote sinks. Idempotent.

    Call ONCE at process startup, before any `get_logger(...)` calls.

    Env vars read:
      LOG_LEVEL                — overrides `level`
      GRAFANA_LOKI_URL/USER/TOKEN — enables Loki sink if all 3 set
      OTEL_COLLECTOR_URL       — enables OTel logs export if set
      SERVICE_VERSION / DEPLOYMENT_ENVIRONMENT — overrides version/environment args
      LOG_PRETTY               — "1" forces console renderer (default in non-prod)
      OBSERVABILITY_DISABLE_AUTOINSTRUMENT — "1" skips auto_instrument()

    Args:
      instrument: when True (default), runs `auto_instrument()` after logging
        is configured. Library instrumentations (httpx, sqlalchemy, asyncpg,
        redis, aiohttp, requests) are activated if their corresponding
        `opentelemetry-instrumentation-*` packages are installed; missing
        packages are silently skipped.
    """
    global _initialized, _service_name, _service_version, _environment, _loki_sender

    if _initialized:
        return

    _service_name = service_name
    _service_version = os.getenv("SERVICE_VERSION") or version or "unknown"
    _environment = os.getenv("DEPLOYMENT_ENVIRONMENT") or environment or os.getenv("ENV", "unknown")

    log_level = os.getenv("LOG_LEVEL", level).upper()
    level_num = getattr(logging, log_level, logging.INFO)

    # Configure the stdlib root logger so libraries logging via `logging.getLogger`
    # also flow through structlog's pipeline.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level_num,
        force=True,
    )

    # Install the LogRecord factory BEFORE _init_otel attaches its
    # LoggingHandler — otherwise stdlib records would already be emitted
    # without thread_id / request_id attributes during the brief window.
    _install_stdlib_contextvar_bridge()

    pretty = os.getenv("LOG_PRETTY", "0") == "1" or _environment in {"dev", "local"}
    renderer: Any = (
        structlog.dev.ConsoleRenderer(colors=True)
        if pretty
        else structlog.processors.JSONRenderer()
    )

    # Loki sink (only if all three env vars are present).
    loki_url = os.getenv("GRAFANA_LOKI_URL")
    loki_user = os.getenv("GRAFANA_LOKI_USER")
    loki_token = os.getenv("GRAFANA_LOKI_TOKEN")
    if loki_url and loki_user and loki_token:
        try:
            _loki_sender = _LokiSender(loki_url, loki_user, loki_token)
            _loki_sender.start()
            atexit.register(_loki_sender.stop)
        except Exception as exc:  # noqa: BLE001
            _internal_logger.warning("Loki sender init failed: %s", exc)
            _loki_sender = None

    # OTel collector sink.
    otel_url = os.getenv("OTEL_COLLECTOR_URL")
    if otel_url:
        _init_otel(otel_url)

    # `format_exc_info` renders the traceback as a plain string the
    # ConsoleRenderer can prefix to stdout and the JSONRenderer can put under
    # an "exception" key. (Greptile flagged this as deprecated; verified
    # against structlog 25.5.0 — still supported, emits no warning.)
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        _add_severity,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        _inject_context,
    ]
    if _loki_sender is not None:
        processors.append(_loki_processor)
    if otel_url:
        processors.append(_otel_processor)
    processors.append(renderer)

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level_num),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _initialized = True

    activated: list[str] = []
    if instrument and os.getenv("OBSERVABILITY_DISABLE_AUTOINSTRUMENT", "0") != "1":
        activated = auto_instrument()

    # First log line announces the configuration so we can confirm in Grafana.
    logger = structlog.get_logger("observability")
    logger.info(
        "observability_initialized",
        service=service_name,
        version=_service_version,
        environment=_environment,
        log_level=log_level,
        loki_enabled=_loki_sender is not None,
        otel_enabled=bool(otel_url),
        instrumentations=activated,
    )


# ---------------------------------------------------------------------------
# auto_instrument — activate OTel instrumentations for installed libraries
# ---------------------------------------------------------------------------

# Each entry: (instrumentation_pkg_path, Instrumentor class name, library_name_for_logging)
_INSTRUMENTORS: list[tuple[str, str, str]] = [
    ("opentelemetry.instrumentation.httpx", "HTTPXClientInstrumentor", "httpx"),
    ("opentelemetry.instrumentation.requests", "RequestsInstrumentor", "requests"),
    ("opentelemetry.instrumentation.aiohttp_client", "AioHttpClientInstrumentor", "aiohttp_client"),
    ("opentelemetry.instrumentation.aiohttp_server", "AioHttpServerInstrumentor", "aiohttp_server"),
    ("opentelemetry.instrumentation.sqlalchemy", "SQLAlchemyInstrumentor", "sqlalchemy"),
    ("opentelemetry.instrumentation.asyncpg", "AsyncPGInstrumentor", "asyncpg"),
    ("opentelemetry.instrumentation.psycopg2", "Psycopg2Instrumentor", "psycopg2"),
    ("opentelemetry.instrumentation.psycopg", "PsycopgInstrumentor", "psycopg"),
    ("opentelemetry.instrumentation.redis", "RedisInstrumentor", "redis"),
    ("opentelemetry.instrumentation.boto3sqs", "Boto3SQSInstrumentor", "boto3sqs"),
]


def auto_instrument() -> list[str]:
    """Activate every installed OTel instrumentation. Returns list of activated names.

    Each instrumentation is best-effort: import errors and runtime errors are
    caught and logged at WARNING. Missing packages produce no output.

    Note: FastAPI and aiohttp app instrumentations require the app object and
    must be activated separately via `install_fastapi(app)` / `install_aiohttp(app)`.
    """
    activated: list[str] = []
    for pkg, cls_name, label in _INSTRUMENTORS:
        try:
            module = __import__(pkg, fromlist=[cls_name])
            instrumentor_cls = getattr(module, cls_name)
            instrumentor_cls().instrument()
            activated.append(label)
        except ImportError:
            # Package not installed — silently skip.
            continue
        except Exception as exc:  # noqa: BLE001
            _internal_logger.warning(
                "auto_instrument: %s failed (%s); continuing", label, exc
            )
    return activated


def get_logger(name: str | None = None) -> Any:
    """Return a structlog logger bound to the calling module.

    If `init_observability()` hasn't run yet, structlog still returns a logger
    that writes to stdout — but without context enrichment. Safe to call from
    module top-level.
    """
    if name is None:
        return structlog.get_logger()
    return structlog.get_logger(name)
