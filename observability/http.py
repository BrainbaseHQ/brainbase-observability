"""httpx client factory that auto-propagates request_id and thread_id.

Use this everywhere you'd otherwise reach for `httpx.AsyncClient(...)`:

    from observability.http import get_async_client

    async with get_async_client() as client:
        r = await client.get(url)

Outbound requests get X-Request-ID and X-Thread-ID set from the current
contextvars, so downstream services can stitch their logs to yours.

Important: httpx requires `AsyncClient` event hooks to be coroutine functions
(it does ``await hook(req)`` internally). Awaiting a sync `def` that returns
``None`` raises ``TypeError: object NoneType can't be used in 'await'
expression`` on the FIRST outbound call. v0.3.0–v0.3.2 had this bug; v0.3.3+
uses async hooks for AsyncClient and sync hooks for the sync Client.
"""

from __future__ import annotations

from typing import Any

from ._core import current_request_id, current_thread_id, get_logger

_logger = get_logger("observability.http")


def _inject_correlation_headers(request: Any) -> None:
    rid = current_request_id()
    if rid and "X-Request-ID" not in request.headers:
        request.headers["X-Request-ID"] = rid
    tid = current_thread_id()
    if tid and "X-Thread-ID" not in request.headers:
        request.headers["X-Thread-ID"] = tid


def _log_outbound(response: Any) -> None:
    try:
        _logger.debug(
            "http_outbound",
            method=response.request.method,
            url=str(response.request.url),
            status=response.status_code,
        )
    except Exception:  # noqa: BLE001
        pass


def _on_request_sync(request: Any) -> None:
    _inject_correlation_headers(request)


def _on_response_sync(response: Any) -> None:
    _log_outbound(response)


async def _on_request_async(request: Any) -> None:
    _inject_correlation_headers(request)


async def _on_response_async(response: Any) -> None:
    _log_outbound(response)


def get_async_client(**kwargs: Any) -> Any:
    """Return a configured httpx.AsyncClient with correlation-id propagation.

    Async event hooks are required by httpx for AsyncClient.
    """
    import httpx

    event_hooks = kwargs.pop("event_hooks", {})
    event_hooks.setdefault("request", []).append(_on_request_async)
    event_hooks.setdefault("response", []).append(_on_response_async)
    return httpx.AsyncClient(event_hooks=event_hooks, **kwargs)


def get_sync_client(**kwargs: Any) -> Any:
    """Return a configured httpx.Client with correlation-id propagation.

    Sync event hooks are required by httpx for the sync Client.
    """
    import httpx

    event_hooks = kwargs.pop("event_hooks", {})
    event_hooks.setdefault("request", []).append(_on_request_sync)
    event_hooks.setdefault("response", []).append(_on_response_sync)
    return httpx.Client(event_hooks=event_hooks, **kwargs)
