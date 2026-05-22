"""Regression tests for observability.http event hooks.

v0.3.0–v0.3.2 attached sync `def` hooks to `httpx.AsyncClient`, which awaits
hooks internally. `await None` (the return of a sync def) raised
`TypeError: object NoneType can't be used in 'await' expression` on every
outbound request, breaking get_async_client across every consumer repo.

This file pins the contract that:
  - get_async_client's hooks are coroutine functions
  - get_sync_client's hooks are regular functions
  - both factories actually issue a request without raising
"""

from __future__ import annotations

import asyncio
import inspect

import httpx
import pytest

from observability import bind_request_id, bind_thread_id, clear_context
from observability.http import (
    _on_request_async,
    _on_request_sync,
    _on_response_async,
    _on_response_sync,
    get_async_client,
    get_sync_client,
)


def test_async_hooks_are_coroutine_functions() -> None:
    assert inspect.iscoroutinefunction(_on_request_async)
    assert inspect.iscoroutinefunction(_on_response_async)


def test_sync_hooks_are_not_coroutine_functions() -> None:
    assert not inspect.iscoroutinefunction(_on_request_sync)
    assert not inspect.iscoroutinefunction(_on_response_sync)


def test_async_client_registers_async_hooks() -> None:
    client = get_async_client()
    try:
        for hook in client.event_hooks["request"]:
            assert inspect.iscoroutinefunction(hook), (
                f"AsyncClient request hook {hook!r} is not a coroutine — "
                "would raise 'NoneType can't be used in await expression'."
            )
        for hook in client.event_hooks["response"]:
            assert inspect.iscoroutinefunction(hook)
    finally:
        asyncio.run(client.aclose())


def test_sync_client_registers_sync_hooks() -> None:
    with get_sync_client() as client:
        for hook in client.event_hooks["request"]:
            assert not inspect.iscoroutinefunction(hook)
        for hook in client.event_hooks["response"]:
            assert not inspect.iscoroutinefunction(hook)


def _make_mock_transport() -> httpx.MockTransport:
    captured: dict[str, httpx.Headers] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = request.headers
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    transport.captured = captured  # type: ignore[attr-defined]
    return transport


def test_async_client_actually_makes_request_without_typeerror() -> None:
    """The real regression test — instantiate AsyncClient, fire a request, no TypeError."""

    async def _run() -> httpx.Headers:
        transport = _make_mock_transport()
        async with get_async_client(transport=transport) as client:
            bind_request_id("rid-test-async")
            bind_thread_id("tid-test-async")
            resp = await client.get("https://example.com/x")
            assert resp.status_code == 200
        return transport.captured["headers"]

    headers = asyncio.run(_run())
    assert headers["X-Request-ID"] == "rid-test-async"
    assert headers["X-Thread-ID"] == "tid-test-async"


def test_sync_client_actually_makes_request_without_typeerror() -> None:
    transport = _make_mock_transport()
    with get_sync_client(transport=transport) as client:
        bind_request_id("rid-test-sync")
        bind_thread_id("tid-test-sync")
        resp = client.get("https://example.com/x")
        assert resp.status_code == 200
    headers = transport.captured["headers"]
    assert headers["X-Request-ID"] == "rid-test-sync"
    assert headers["X-Thread-ID"] == "tid-test-sync"


def test_async_client_does_not_overwrite_explicit_headers() -> None:
    async def _run() -> httpx.Headers:
        transport = _make_mock_transport()
        async with get_async_client(transport=transport) as client:
            bind_request_id("rid-context")
            resp = await client.get(
                "https://example.com/x",
                headers={"X-Request-ID": "rid-explicit"},
            )
            assert resp.status_code == 200
        return transport.captured["headers"]

    headers = asyncio.run(_run())
    assert headers["X-Request-ID"] == "rid-explicit"


def test_async_client_omits_headers_when_context_unset() -> None:
    """Hooks must not crash, and must not set headers, when contextvars are unset."""

    async def _run() -> httpx.Headers:
        clear_context()
        transport = _make_mock_transport()
        async with get_async_client(transport=transport) as client:
            # Simulate a code path that runs outside the FastAPI/aiohttp
            # middleware (e.g., a one-off script): no rid/tid bound.
            resp = await client.get("https://example.com/x")
            assert resp.status_code == 200
        return transport.captured["headers"]

    headers = asyncio.run(_run())
    assert "X-Request-ID" not in headers
    assert "X-Thread-ID" not in headers


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
