"""Cron-style entrypoint helpers.

For services that run as one-shot scripts (Render crons, k8s Jobs, etc).
Wraps the main() function so any uncaught exception is logged with a full
traceback before the process exits non-zero.

Usage in your cron's logs.py:

    from observability import init_observability
    from observability.cron import run_cron_entrypoint

    init_observability(service_name="kafka_logs", environment="prod")

    @run_cron_entrypoint
    def main() -> None:
        ...

    if __name__ == "__main__":
        main()
"""

from __future__ import annotations

import functools
import sys
import time
from typing import Any, Callable, TypeVar

from ._core import bind_context, bind_request_id, clear_context, get_logger

_F = TypeVar("_F", bound=Callable[..., Any])
_logger = get_logger("observability.cron")


def run_cron_entrypoint(func: _F) -> _F:
    """Decorator: log start/end, catch uncaught exceptions, exit non-zero on failure."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        run_id = bind_request_id()
        bind_context(cron_function=func.__name__)
        start = time.perf_counter()
        _logger.info("cron_run_start", run_id=run_id)
        try:
            result = func(*args, **kwargs)
            duration_ms = (time.perf_counter() - start) * 1000.0
            _logger.info("cron_run_end", duration_ms=round(duration_ms, 2), status="ok")
            return result
        except SystemExit as exc:
            duration_ms = (time.perf_counter() - start) * 1000.0
            # `sys.exit(0)` and `sys.exit()` both indicate clean success.
            # `sys.exit(<non-zero>)` and `sys.exit(<string>)` indicate failure.
            # Don't blanket-log every SystemExit as a warning — the common case
            # for crons is a successful `sys.exit(0)` at the end of main().
            code = exc.code
            is_success = code is None or code == 0
            if is_success:
                _logger.info(
                    "cron_run_end",
                    duration_ms=round(duration_ms, 2),
                    exit_code=code,
                    status="ok",
                )
            else:
                _logger.warning(
                    "cron_run_failed_exit",
                    duration_ms=round(duration_ms, 2),
                    exit_code=code,
                    status="error",
                )
            raise
        except Exception:
            duration_ms = (time.perf_counter() - start) * 1000.0
            _logger.exception("cron_run_failed", duration_ms=round(duration_ms, 2))
            sys.exit(1)
        finally:
            clear_context()

    return wrapper  # type: ignore[return-value]
