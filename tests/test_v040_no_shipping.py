"""v0.4.0 regression tests — no shipping layer.

These tests pin the v0.4.0 contract: `init_observability()` writes JSON to
stdout and does NOT spin up the in-process Loki / OTel HTTP push. If any
future change accidentally re-introduces the shipping layer (which would
bring back the v0.3.x sync-hook bug class), these tests fail.
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
from contextlib import redirect_stdout

import pytest

import observability
from observability import bind_thread_id, clear_context, get_logger, init_observability


def _reset_init_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force-reset module state so init_observability runs fresh per test."""
    import observability._core as core

    monkeypatch.setattr(core, "_initialized", False)
    # Also wipe the LogRecord factory flag so the bridge reinstalls cleanly.
    if hasattr(logging, "_brainbase_obs_factory_installed"):
        monkeypatch.delattr(logging, "_brainbase_obs_factory_installed", raising=False)


def test_init_does_not_import_loki_or_otel_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sanity: in v0.4.0 the package should not even import the OTel logs SDK."""
    # Strip any prior import so we can detect re-entry.
    for mod in list(sys.modules):
        if mod.startswith("opentelemetry."):
            sys.modules.pop(mod, None)

    _reset_init_state(monkeypatch)
    init_observability(service_name="test-v040-no-shipping")

    # No OTel logs SDK should have been imported by init_observability.
    assert "opentelemetry._logs" not in sys.modules
    assert "opentelemetry.sdk._logs" not in sys.modules


def test_init_ignores_loki_and_otel_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """v0.3.x env vars are no-ops in v0.4.0; setting them must not crash or attempt shipping."""
    monkeypatch.setenv("GRAFANA_LOKI_URL", "https://loki.example.com/loki/api/v1/push")
    monkeypatch.setenv("GRAFANA_LOKI_USER", "fake")
    monkeypatch.setenv("GRAFANA_LOKI_TOKEN", "fake")
    monkeypatch.setenv("OTEL_COLLECTOR_URL", "https://otel.example.com")

    _reset_init_state(monkeypatch)
    init_observability(service_name="test-v040-env-vars-ignored")

    # No sender, no provider, no in-process shipping state.
    import observability._core as core

    assert not hasattr(core, "_loki_sender") or core.__dict__.get("_loki_sender") is None
    assert not hasattr(core, "_otel_logger_provider") or core.__dict__.get("_otel_logger_provider") is None


def test_log_line_contains_thread_id_as_json_field(monkeypatch: pytest.MonkeyPatch) -> None:
    """The whole point of v0.4.0: structured fields land on stdout for the platform to forward."""
    _reset_init_state(monkeypatch)
    init_observability(service_name="test-v040-stdout")

    clear_context()
    bind_thread_id("test-thread-id-abc123")

    buf = io.StringIO()
    logger = get_logger(__name__)
    with redirect_stdout(buf):
        logger.info("hello_event", custom_field="custom_value")

    out = buf.getvalue()
    # JSON renderer fires in non-pretty environments. Find the JSON object.
    lines = [ln for ln in out.splitlines() if ln.strip().startswith("{")]
    assert lines, f"expected a JSON log line on stdout, got: {out!r}"
    payload = json.loads(lines[-1])
    assert payload["event"] == "hello_event"
    assert payload["thread_id"] == "test-thread-id-abc123"
    assert payload["service"] == "test-v040-stdout"
    assert payload["custom_field"] == "custom_value"
    assert payload["level"] == "INFO"


def test_back_compat_kwarg_instrument_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    """v0.3.x callers may pass `instrument=...`; v0.4.0 accepts and ignores it."""
    _reset_init_state(monkeypatch)
    # Both True and False must be accepted without TypeError.
    init_observability(service_name="test-back-compat", instrument=True)
    # Idempotent second call (already initialized) with the OTHER value:
    init_observability(service_name="test-back-compat-2", instrument=False)


def test_public_api_still_exports_back_compat_names() -> None:
    """Existing services import `bind_thread_id`, `init_observability`, etc.

    v0.4.0 removes `auto_instrument` from the public surface (no callers
    import it directly), but everything else must remain importable.
    """
    expected = {
        "init_observability",
        "get_logger",
        "bind_request_id",
        "current_request_id",
        "bind_thread_id",
        "current_thread_id",
        "bind_context",
        "clear_context",
    }
    assert expected.issubset(set(observability.__all__))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
