"""Translate ADF ForEach activities to Databricks ForEachActivity IR.

ForEach is a control-flow container that threads context through inner
activity translation.  It returns a ``(Activity, TranslationContext)`` tuple
so the engine can propagate any context changes from inner activities.
"""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, ForEachActivity, TranslationContext


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
    *,
    translate_activities_fn: Any = None,
) -> tuple[Activity, TranslationContext]:
    """Translate a ForEach activity with recursive inner translation.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.
        translate_activities_fn: Callback to translate inner activities.
            Signature: ``(activities, context, definitions) -> (list[Activity], TranslationContext)``.

    Returns:
        Tuple of ``(ForEachActivity, updated_context)``.
    """
    tp = activity.type_properties or {}

    items_expression = ""
    items_raw = tp.get("items")
    if isinstance(items_raw, dict) and items_raw.get("type") == "Expression":
        items_expression = items_raw.get("value", "")
    elif isinstance(items_raw, str):
        items_expression = items_raw

    is_sequential = tp.get("isSequential", False)
    batch_count_raw = tp.get("batchCount")
    batch_count = int(batch_count_raw) if batch_count_raw is not None else (1 if is_sequential else 20)

    # Translate inner activities
    inner_activities: list[Activity] = []
    inner_context = context
    child_adf_activities = activity.activities or []

    if translate_activities_fn and child_adf_activities:
        inner_prefix = context.task_key_prefix + activity.name + "__" if context.task_key_prefix else activity.name + "__"
        child_context = TranslationContext(
            activity_cache=context.activity_cache,
            registry=context.registry,
            variable_cache=context.variable_cache,
        )
        inner_activities, inner_context = translate_activities_fn(child_adf_activities, child_context, definitions)

    foreach_activity = ForEachActivity(
        **base_kwargs,
        items_expression=items_expression,
        child_activity=inner_activities[0] if len(inner_activities) == 1 else inner_activities[0] if inner_activities else Activity(name="noop", task_key="noop"),
        concurrency=batch_count,
    )

    return foreach_activity, context
