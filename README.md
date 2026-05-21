# brainbase-observability

Shared observability package for Brainbase Python services. Provides:

- **structlog**-based structured JSON logging with async-safe contextvars
- **Grafana Loki** push exporter (background thread, batched, fail-closed)
- **OpenTelemetry** logs export with a custom HTTP/JSON exporter
- **FastAPI** middleware for request_id propagation + global exception capture
- **aiohttp** middleware (same surface)
- **httpx** client factories that auto-propagate `X-Request-ID` outbound
- **cron** decorator (`@run_cron_entrypoint`) for one-shot scripts
- **auto_instrument()** that activates any installed OTel instrumentation packages

## Install

Pin to a specific version (recommended):

```
brainbase-observability @ git+https://github.com/BrainbaseHQ/brainbase-observability.git@v0.1.0
```

Floating versions (NOT recommended for production):

```
brainbase-observability @ git+https://github.com/BrainbaseHQ/brainbase-observability.git@main
```

The package's runtime deps are kept minimal: `structlog`, `opentelemetry-api`,
`opentelemetry-sdk`, `httpx`. Library auto-instrumentations are declared as
**optional**: each consuming service adds the `opentelemetry-instrumentation-*`
packages it actually needs (httpx, asyncpg, redis, etc.) in its own
requirements. `init_observability()` will auto-activate every installed
instrumentation; missing ones are silently skipped.

## Quick start — FastAPI

```python
from fastapi import FastAPI
from observability import init_observability, get_logger
from observability.fastapi import install_fastapi

init_observability(service_name="kafka-llm-service")
logger = get_logger(__name__)

app = FastAPI()
install_fastapi(app)
```

## Quick start — aiohttp

```python
from aiohttp import web
from observability import init_observability, get_logger
from observability.aiohttp import install_aiohttp

init_observability(service_name="kafka-connectors")
logger = get_logger(__name__)

app = web.Application()
install_aiohttp(app)
```

## Quick start — cron / one-shot script

```python
from observability import init_observability
from observability.cron import run_cron_entrypoint

init_observability(service_name="kafka-logs")

@run_cron_entrypoint
def main():
    # do the thing
    ...

if __name__ == "__main__":
    main()
```

## Inside `except` blocks

Always use `logger.exception(...)` (not `logger.error(...)`) so the traceback
ships to Loki + OTel:

```python
try:
    do_thing()
except SomeError:
    logger.exception("do_thing_failed", thing_id=thing_id)
    raise
```

## Outbound HTTP

Use the helper factories so `X-Request-ID` propagates to downstream services:

```python
from observability.http import get_async_client

async with get_async_client() as client:
    r = await client.get(url)
```

## Env vars

| Var | Effect |
|---|---|
| `LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` / `ERROR`. Default `INFO`. |
| `LOG_PRETTY` | `1` forces console renderer. Default: console in dev, JSON in prod. |
| `SERVICE_VERSION` | Tag on every log line. |
| `DEPLOYMENT_ENVIRONMENT` | `prod` / `preview` / `staging` / `dev`. |
| `GRAFANA_LOKI_URL` | Loki push URL (e.g. `https://logs-prod-021.grafana.net/loki/api/v1/push`). |
| `GRAFANA_LOKI_USER` | Loki tenant/user. |
| `GRAFANA_LOKI_TOKEN` | Loki API token. |
| `OTEL_COLLECTOR_URL` | OTel HTTP/JSON logs endpoint. |
| `OBSERVABILITY_DISABLE_AUTOINSTRUMENT` | `1` skips `auto_instrument()` during init. |

Loki + OTel sinks no-op silently if the corresponding env vars are not set, so
the package is safe to enable in dev with no extra setup.

## Failure model

Every remote sink is best-effort. Network, parsing, and serialization errors
are caught and surfaced as one-line stderr warnings. They never block the
application or raise into request handlers.

## Release process

1. Bump `version` in `pyproject.toml`
2. Commit on `main`
3. Tag `vX.Y.Z` and push
4. Each consuming service updates its dep pin in a small follow-up PR

Semver: breaking API changes bump major. Adding sinks, instrumentations, or
helpers bumps minor. Bug fixes bump patch.
