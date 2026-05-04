"""Shared utilities for the orchestra translation pipeline.

Ported from wkmigrate with modifications.  Covers common patterns such as
case conversion, task-key normalisation, timeout parsing, and retry-policy
extraction.
"""

from __future__ import annotations

import re
from typing import Any

from orchestra.models.adf_ast import AdfPolicy

# ---------------------------------------------------------------------------
# Default ADF timeout (12 hours) used when a timeout string cannot be parsed.
# ---------------------------------------------------------------------------
DEFAULT_TIMEOUT_SECONDS = 43_200

# ---------------------------------------------------------------------------
# Case conversion
# ---------------------------------------------------------------------------

_CAMEL_RE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_RE_2 = re.compile(r"([a-z0-9])([A-Z])")


def camel_to_snake(name: str) -> str:
    """Converts a camelCase or PascalCase string to snake_case.

    Used when loading ADF definitions that use camelCase (e.g. REST/portal
    export) so downstream code, which expects snake_case keys, works unchanged.

    Args:
        name: Identifier in camelCase or PascalCase.

    Returns:
        Same identifier in snake_case.
    """
    substituted = _CAMEL_RE_1.sub(r"\1_\2", name)
    return _CAMEL_RE_2.sub(r"\1_\2", substituted).lower()


def recursive_camel_to_snake(obj: Any) -> Any:
    """Recursively convert all dict keys from camelCase to snake_case.

    Leaves list order and non-dict values unchanged.  Creates new dicts/lists
    (no in-place mutation).

    Args:
        obj: Nested structure of dicts, lists, and primitives (e.g. ADF JSON).

    Returns:
        New structure with dict keys in snake_case.
    """
    if isinstance(obj, dict):
        return {camel_to_snake(k): recursive_camel_to_snake(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [recursive_camel_to_snake(item) for item in obj]
    return obj


# ---------------------------------------------------------------------------
# Task-key normalisation
# ---------------------------------------------------------------------------

_TASK_KEY_RE = re.compile(r"[^a-z0-9_]")


def normalize_task_key(name: str) -> str:
    """Sanitises a display name for use as a Databricks task key.

    Replaces spaces and special characters with underscores, lowercases the
    result, and collapses runs of underscores.

    Args:
        name: Original activity or pipeline name.

    Returns:
        Cleaned task key string.
    """
    lowered = name.strip().lower()
    replaced = _TASK_KEY_RE.sub("_", lowered)
    # Collapse consecutive underscores and strip leading/trailing ones.
    collapsed = re.sub(r"_+", "_", replaced).strip("_")
    return collapsed


# ---------------------------------------------------------------------------
# Timeout parsing
# ---------------------------------------------------------------------------

_TIMEOUT_PATTERN = re.compile(r"^(?:(\d+)\.)?((\d{1,2}):(\d{2}):(\d{2}))$")


def parse_timeout(timeout_str: str | None) -> int | None:
    """Parses an ADF timeout string into total seconds.

    Supports ADF timeout formats including:
    - ``"0.12:00:00"`` (12 hours)
    - ``"1.00:00:00"`` (1 day)
    - ``"00:10:00"`` (10 minutes, no day prefix)

    Returns ``None`` when *timeout_str* is ``None``.  Returns
    ``DEFAULT_TIMEOUT_SECONDS`` when the string cannot be parsed or
    represents a non-positive duration.

    Args:
        timeout_str: Timeout string from the ADF activity policy, or ``None``.

    Returns:
        Total seconds, ``DEFAULT_TIMEOUT_SECONDS`` on parse failure, or ``None``
        when no timeout is specified.
    """
    if timeout_str is None:
        return None

    match = _TIMEOUT_PATTERN.match(timeout_str)
    if not match:
        return DEFAULT_TIMEOUT_SECONDS

    days = int(match.group(1)) if match.group(1) is not None else 0
    hours = int(match.group(3))
    minutes = int(match.group(4))
    seconds = int(match.group(5))

    total = days * 86_400 + hours * 3_600 + minutes * 60 + seconds
    if total <= 0:
        return DEFAULT_TIMEOUT_SECONDS
    return total


# ---------------------------------------------------------------------------
# Retry policy extraction
# ---------------------------------------------------------------------------


def parse_retry_policy(policy: AdfPolicy | None) -> tuple[int | None, int | None]:
    """Extracts retry count and interval from an ADF policy.

    Args:
        policy: Parsed ``AdfPolicy``, or ``None``.

    Returns:
        Tuple of ``(max_retries, retry_interval_seconds)``.  Either or both
        values may be ``None`` when the policy does not specify them.
    """
    if policy is None:
        return None, None

    retries = policy.retry
    interval = policy.retry_interval_in_seconds
    return retries, interval
