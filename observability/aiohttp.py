"""aiohttp integration: server middleware + global error handler.

Usage:

    from aiohttp import web
    from observability import init_observability
    from observability.aiohttp import install_aiohttp

    init_observability(service_name="kafka-connectors")
    app = web.Application()
    install_aiohttp(app)
    ...

Reads/generates X-Request-ID on every inbound request, binds it to the
contextvar, and logs request start + end with timing. Uncaught exceptions
inside handlers are logged with full traceback before the framework's
default 500 handler runs.
"""

from __future__ import annotations

import time
import traceback
from typing import Any

from ._core import (
    bind_context,
    bind_request_id,
    bind_thread_id,
    clear_context,
    current_request_id,
    get_logger,
)

_logger = get_logger("observability.aiohttp")


def install_aiohttp(app: Any, *, instrument: bool = False) -> None:
    """Attach observability middleware + error handler to an aiohttp app.

    By default this function does NOT activate AioHttpServerInstrumentor.
    `init_observability()` → `auto_instrument()` already activates the
    aiohttp_server instrumentor globally if its package is installed, and
    activating it a second time wraps the already-wrapped handler — producing
    duplicate OTel spans per inbound request. Pass `instrument=True` only if
    you've called `init_observability(..., instrument=False)` and intend this
    function to be the sole activator.
    """
    try:
        from aiohttp import web
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "install_aiohttp() requires aiohttp to be installed"
        ) from exc

    if instrument:
        try:
            from opentelemetry.instrumentation.aiohttp_server import (
                AioHttpServerInstrumentor,
            )

            AioHttpServerInstrumentor().instrument()
        except ImportError:
            pass
        except Exception as exc:  # noqa: BLE001
            _logger.warning("aiohttp auto-instrumentation failed: %s", exc)

    @web.middleware
    async def observability_middleware(request: Any, handler: Any) -> Any:
        incoming = request.headers.get("X-Request-ID") or request.headers.get("traceparent")
        rid = bind_request_id(incoming)
        header_tid = request.headers.get("X-Thread-ID")
        if header_tid:
            bind_thread_id(header_tid)
        bind_context(
            http_method=request.method,
            http_path=request.path,
            client_ip=request.remote,
        )
        start = time.perf_counter()
        _logger.info("http_request_start")
        status_code = 500
        try:
            response = await handler(request)
            status_code = getattr(response, "status", 200)
            try:
                response.headers["X-Request-ID"] = rid
            except Exception:  # noqa: BLE001
                pass
            return response
        except web.HTTPException as http_exc:
            status_code = http_exc.status
            try:
                http_exc.headers["X-Request-ID"] = rid
            except Exception:  # noqa: BLE001
                pass
            raise
        except Exception:
            _logger.exception(
                "http_request_unhandled_exception",
                traceback=traceback.format_exc(),
            )
            raise
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            _logger.info(
                "http_request_end",
                status=status_code,
                duration_ms=round(duration_ms, 2),
            )
            clear_context()

    app.middlewares.append(observability_middleware)
