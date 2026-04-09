"""Translate ADF WebActivity activities to Databricks WebActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, TranslationContext
from orchestra.models.ir import WebActivity as WebActivityIR
from orchestra.translator.activity_translators._resolve import resolve_dict_values, resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate a WebActivity.

    Extracts URL, HTTP method, headers, body, and authentication config.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`WebActivity` IR node.
    """
    tp = activity.type_properties or {}

    url = resolve_field(tp.get("url", ""), context)
    method = tp.get("method", "GET")
    headers = resolve_dict_values(tp.get("headers"), context) or None
    body = tp.get("body")
    authentication = tp.get("authentication")
    disable_cert_validation = tp.get("disableCertValidation", False)
    http_request_timeout = tp.get("httpRequestTimeout")

    # Convert ADF timeout format to seconds if present
    timeout_seconds: int | None = None
    if http_request_timeout and isinstance(http_request_timeout, str):
        timeout_seconds = _parse_timeout_to_seconds(http_request_timeout)

    return WebActivityIR(
        **base_kwargs,
        url=url,
        method=method,
        body=body,
        headers=headers,
        authentication=authentication,
        disable_cert_validation=disable_cert_validation,
        http_request_timeout_seconds=timeout_seconds,
    )


def _parse_timeout_to_seconds(timeout_str: str) -> int | None:
    """Parse an ADF timeout string to seconds.

    Args:
        timeout_str: Timeout in ``"d.hh:mm:ss"`` or ``"hh:mm:ss"`` format.

    Returns:
        Total seconds, or ``None`` if the format is unrecognised.
    """
    try:
        parts = timeout_str.split(".")
        if len(parts) == 2:
            days = int(parts[0])
            time_part = parts[1]
        else:
            days = 0
            time_part = parts[0]
        time_parts = time_part.split(":")
        hours = int(time_parts[0]) if len(time_parts) > 0 else 0
        minutes = int(time_parts[1]) if len(time_parts) > 1 else 0
        seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
        return days * 86400 + hours * 3600 + minutes * 60 + seconds
    except (ValueError, IndexError):
        return None
