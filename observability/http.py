"""httpx client factory that auto-propagates the current request_id.

Use this everywhere you'd otherwise reach for `httpx.AsyncClient(...)`:

    from observability.http import get_async_client

    async with get_async_client() as client:
        r = await client.get(url)

Outbound requests get X-Request-ID set from the current contextvar, so
downstream services can stitch their logs to yours.
"""

from __future__ import annotations

from typing import Any

from ._core import current_request_id, get_logger

_logger = get_logger("observability.http")


def _on_request(request: Any) -> None:
    rid = current_request_id()
    if rid and "X-Request-ID" not in request.headers:
        request.headers["X-Request-ID"] = rid


def _on_response(response: Any) -> None:
    # Lightweight outbound trace at debug level. Don't log bodies.
    try:
        _logger.debug(
            "http_outbound",
            method=response.request.method,
            url=str(response.request.url),
            status=response.status_code,
        )
    except Exception:  # noqa: BLE001
        pass


def get_async_client(**kwargs: Any) -> Any:
    """Return a configured httpx.AsyncClient with request-id propagation."""
    import httpx

    event_hooks = kwargs.pop("event_hooks", {})
    event_hooks.setdefault("request", []).append(_on_request)
    event_hooks.setdefault("response", []).append(_on_response)
    return httpx.AsyncClient(event_hooks=event_hooks, **kwargs)


def get_sync_client(**kwargs: Any) -> Any:
    """Return a configured httpx.Client with request-id propagation."""
    import httpx

    event_hooks = kwargs.pop("event_hooks", {})
    event_hooks.setdefault("request", []).append(_on_request)
    event_hooks.setdefault("response", []).append(_on_response)
    return httpx.Client(event_hooks=event_hooks, **kwargs)
