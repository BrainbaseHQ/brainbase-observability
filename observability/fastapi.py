"""FastAPI integration: request middleware + global exception handler.

Usage in your service's main.py:

    from fastapi import FastAPI
    from observability import init_observability
    from observability.fastapi import install_fastapi

    init_observability(service_name="kafka-llm-service")
    app = FastAPI()
    install_fastapi(app)

What it does:
- Reads/generates X-Request-ID and binds to contextvars so every log line
  in the request's lifecycle (including async) carries it.
- Logs request start + end with method, path, status, duration_ms.
- Catches uncaught exceptions, logs them with full traceback, returns 500
  with a JSON body containing the request_id so support can correlate.
- Sets X-Request-ID on every response so clients can include it in tickets.
"""

from __future__ import annotations

import time
from typing import Any

from ._core import (
    bind_context,
    bind_request_id,
    bind_thread_id,
    clear_context,
    current_request_id,
    get_logger,
)

_logger = get_logger("observability.fastapi")


def install_fastapi(app: Any, *, instrument: bool = True) -> None:
    """Attach request middleware + global exception handler to a FastAPI app.

    If `instrument` is True and `opentelemetry-instrumentation-fastapi` is
    installed, also activates FastAPI auto-instrumentation on the app.
    """
    try:
        from fastapi import Request
        from fastapi.responses import JSONResponse
        from starlette.middleware.base import BaseHTTPMiddleware
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "install_fastapi() requires fastapi to be installed"
        ) from exc

    if instrument:
        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            FastAPIInstrumentor().instrument_app(app)
        except ImportError:
            pass
        except Exception as exc:  # noqa: BLE001
            _logger.warning("FastAPI auto-instrumentation failed: %s", exc)

    class _ObservabilityMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next: Any) -> Any:
            incoming = request.headers.get("X-Request-ID") or request.headers.get("traceparent")
            rid = bind_request_id(incoming)

            # thread_id: prefer explicit X-Thread-ID header (set by upstream
            # observability-aware services), fall back to the path param if
            # the route declares one (e.g. /threads/{thread_id}/...). path
            # params aren't populated until routing resolves, which happens
            # inside call_next; we set whatever the header gives us upfront
            # and re-bind from path params just before the downstream call
            # actually runs by hooking via state.
            header_tid = request.headers.get("X-Thread-ID")
            if header_tid:
                bind_thread_id(header_tid)

            bind_context(
                http_method=request.method,
                http_path=request.url.path,
                client_ip=getattr(request.client, "host", None),
            )
            start = time.perf_counter()
            _logger.info("http_request_start")
            status_code = 500
            try:
                response = await call_next(request)
                status_code = response.status_code
                response.headers["X-Request-ID"] = rid
                tid = request.path_params.get("thread_id") if hasattr(request, "path_params") else None
                if tid:
                    response.headers["X-Thread-ID"] = str(tid)
                return response
            # No `except Exception` here: route-raised exceptions are caught by
            # Starlette's ExceptionMiddleware (which sits inside call_next) and
            # routed to `_on_unhandled` below, so they never reach this frame.
            # An except here would double-log when middleware-level errors do
            # surface, with no benefit over the handler's own logging.
            finally:
                duration_ms = (time.perf_counter() - start) * 1000.0
                _logger.info(
                    "http_request_end",
                    status=status_code,
                    duration_ms=round(duration_ms, 2),
                )
                clear_context()

    app.add_middleware(_ObservabilityMiddleware)

    @app.exception_handler(Exception)
    async def _on_unhandled(_request: Any, exc: Exception) -> Any:  # noqa: D401
        rid = current_request_id()
        # Pass `exc_info` explicitly rather than calling `traceback.format_exc()`:
        # the latter relies on `sys.exc_info()` being populated, which is a
        # Starlette implementation detail. structlog's `dict_tracebacks`
        # processor renders the explicit tuple into structured fields.
        _logger.exception(
            "unhandled_exception",
            exception_type=type(exc).__name__,
            exception_message=str(exc),
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_server_error",
                "request_id": rid,
                "type": type(exc).__name__,
            },
        )
