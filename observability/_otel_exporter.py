"""Custom OTel HTTP/JSON LogExporter.

Borrowed and simplified from kafka-vm-proxy/tracing.py. Sends OTLP logs as
JSON over HTTP to a collector that speaks the OTel JSON wire format.

This exporter is deliberately tolerant: any failure (network, parsing,
malformed batches) is caught and returns FAILURE without raising into the
SDK, so the application never crashes because a collector is down.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Sequence

try:
    import httpx
    from opentelemetry.sdk._logs import LogData
    from opentelemetry.sdk._logs.export import LogExporter, LogExportResult
except ImportError:  # pragma: no cover - opentelemetry is optional at import time
    httpx = None  # type: ignore[assignment]
    LogData = object  # type: ignore[misc,assignment]
    LogExporter = object  # type: ignore[misc,assignment]

    class LogExportResult:  # type: ignore[no-redef]
        SUCCESS = 0
        FAILURE = 1


_logger = logging.getLogger("observability._otel_exporter")


class HTTPJSONLogExporter(LogExporter):  # type: ignore[misc]
    """POST OTLP logs as JSON to an HTTP collector."""

    def __init__(self, endpoint: str, timeout_seconds: float = 10.0) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._timeout = timeout_seconds
        self._client = httpx.Client(timeout=timeout_seconds) if httpx is not None else None

    def export(self, batch: Sequence[Any]) -> int:
        if self._client is None or not batch:
            return LogExportResult.SUCCESS
        try:
            payload = self._serialize(batch)
            r = self._client.post(
                self._endpoint,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self._timeout,
            )
            if r.status_code >= 400:
                _logger.warning("otel export %s: %s", r.status_code, r.text[:200])
                return LogExportResult.FAILURE
            return LogExportResult.SUCCESS
        except Exception as exc:  # noqa: BLE001
            _logger.warning("otel export failed: %s", exc)
            return LogExportResult.FAILURE

    def shutdown(self) -> None:
        try:
            if self._client is not None:
                self._client.close()
        except Exception:  # noqa: BLE001
            pass

    def force_flush(self, timeout_millis: float = 10_000) -> bool:  # noqa: ARG002
        return True

    @staticmethod
    def _serialize(batch: Sequence[Any]) -> dict[str, Any]:
        """Render a batch of LogData as OTLP/JSON.

        Minimal-but-valid: groups by resource + scope and emits LogRecord
        entries with severity, timestamp, body, and attributes.
        """
        resource_logs: dict[tuple, dict[str, Any]] = {}

        for item in batch:
            try:
                lr = item.log_record
                resource_attrs = dict(getattr(item.log_record.resource, "attributes", {}) or {})
                scope_name = getattr(item.instrumentation_scope, "name", "observability") or "observability"
                key = (json.dumps(resource_attrs, sort_keys=True, default=str), scope_name)

                rl = resource_logs.setdefault(
                    key,
                    {
                        "resource": {"attributes": [{"key": k, "value": _av(v)} for k, v in resource_attrs.items()]},
                        "scopeLogs": [{"scope": {"name": scope_name}, "logRecords": []}],
                    },
                )

                record: dict[str, Any] = {
                    "timeUnixNano": str(getattr(lr, "timestamp", 0) or 0),
                    "severityNumber": int(getattr(lr, "severity_number", 0) or 0),
                    "severityText": str(getattr(lr, "severity_text", "") or ""),
                    "body": _av(getattr(lr, "body", "")),
                }
                attrs = dict(getattr(lr, "attributes", {}) or {})
                if attrs:
                    record["attributes"] = [{"key": k, "value": _av(v)} for k, v in attrs.items()]
                rl["scopeLogs"][0]["logRecords"].append(record)
            except Exception as exc:  # noqa: BLE001
                _logger.warning("otel serialize skipped a record: %s", exc)
                continue

        return {"resourceLogs": list(resource_logs.values())}


def _av(value: Any) -> dict[str, Any]:
    """Render a Python value into an OTLP AnyValue JSON shape."""
    if value is None:
        return {"stringValue": ""}
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    return {"stringValue": str(value)}
