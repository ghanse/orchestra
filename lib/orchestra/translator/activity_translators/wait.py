"""Translate ADF Wait activities to Databricks WaitActivity IR.

Wait is a simple leaf activity that pauses execution for a specified
number of seconds.
"""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, TranslationContext, WaitActivity


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate a Wait activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`WaitActivity` IR node.
    """
    tp = activity.type_properties or {}

    wait_time_raw = tp.get("waitTimeInSeconds", 0)
    try:
        wait_time_seconds = int(wait_time_raw)
    except (TypeError, ValueError):
        wait_time_seconds = 0

    return WaitActivity(
        **base_kwargs,
        wait_time_seconds=wait_time_seconds,
    )
