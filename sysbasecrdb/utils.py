"""Shared utilities: retry decorator, table filtering, CSV escaping."""

import fnmatch
import functools
import time
import logging
from typing import Any, Callable, List, Tuple

logger = logging.getLogger("sysbasecrdb.utils")


def retry(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    exceptions: Tuple[type, ...] = (Exception,),
) -> Callable:
    """Decorator that retries a function on transient failures with exponential backoff."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        sleep_time = backoff_factor ** (attempt - 1)
                        logger.warning(
                            "Attempt %d/%d for %s failed: %s — retrying in %.1fs",
                            attempt,
                            max_attempts,
                            func.__name__,
                            exc,
                            sleep_time,
                        )
                        time.sleep(sleep_time)
            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator


def filter_tables(
    all_tables: List[str], include: List[str], exclude: List[str]
) -> List[str]:
    """Filter table list by include/exclude glob patterns."""
    result = []
    for table in all_tables:
        included = any(fnmatch.fnmatch(table, pat) for pat in include)
        excluded = any(fnmatch.fnmatch(table, pat) for pat in exclude)
        if included and not excluded:
            result.append(table)
    return result


def escape_csv_value(value: Any, delimiter: str, null_marker: str) -> str:
    """Escape a value for CSV output, handling NULLs, delimiters, and newlines."""
    if value is None:
        return null_marker
    s = str(value)
    if delimiter in s or "\n" in s or "\r" in s or '"' in s:
        s = '"' + s.replace('"', '""') + '"'
    return s


def quote_ident(name: str) -> str:
    """Quote an identifier for CockroachDB (lowercase, double-quoted)."""
    return f'"{name.lower()}"'
