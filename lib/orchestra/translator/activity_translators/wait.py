"""Translate ADF Wait activities to Databricks WaitActivity IR.

Wait is a simple leaf activity that pauses execution for a specified
number of seconds.
"""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, TranslationContext, WaitActivity
from orchestra.translator.activity_translators._resolve import resolve_field_int


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

    wait_time_seconds = resolve_field_int(tp.get("waitTimeInSeconds", 0), context, default=0)

    return WaitActivity(
        **base_kwargs,
        wait_time_seconds=wait_time_seconds,
    )
