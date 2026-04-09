"""Translate ADF Switch activities to Databricks SwitchActivity IR.

Switch is a control-flow container that evaluates an expression and routes
to one of N case branches (or a default branch).  It threads context through
each branch translation.  Returns a ``(Activity, TranslationContext)`` tuple.
"""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, SwitchActivity, SwitchCase, TranslationContext


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
    *,
    translate_activities_fn: Any = None,
) -> tuple[Activity, TranslationContext]:
    """Translate a Switch activity with recursive branch translation.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.
        translate_activities_fn: Callback to translate branch activities.
            Signature: ``(activities, context, definitions) -> (list[Activity], TranslationContext)``.

    Returns:
        Tuple of ``(SwitchActivity, updated_context)``.
    """
    tp = activity.type_properties or {}

    # Extract the switch expression from typeProperties.on.value
    on_raw = tp.get("on", {})
    if isinstance(on_raw, dict):
        on_expression = on_raw.get("value", "")
    elif isinstance(on_raw, str):
        on_expression = on_raw
    else:
        on_expression = str(on_raw) if on_raw else ""

    # Translate each case branch
    cases: list[SwitchCase] = []
    raw_cases = tp.get("cases", [])
    for raw_case in raw_cases:
        case_value = raw_case.get("value", "")
        case_activities_raw = raw_case.get("activities", [])

        # Parse raw activity dicts into AdfActivity nodes if needed
        case_adf_activities = _ensure_adf_activities(case_activities_raw)

        case_translated: list[Activity] = []
        if translate_activities_fn and case_adf_activities:
            case_translated, _ = translate_activities_fn(
                case_adf_activities, context, definitions,
            )

        cases.append(SwitchCase(value=case_value, activities=case_translated))

    # Translate default branch
    default_activities: list[Activity] = []
    default_raw = tp.get("defaultActivities", [])
    default_adf_activities = _ensure_adf_activities(default_raw)
    if translate_activities_fn and default_adf_activities:
        default_activities, _ = translate_activities_fn(
            default_adf_activities, context, definitions,
        )

    switch_activity = SwitchActivity(
        **base_kwargs,
        on_expression=on_expression,
        cases=cases,
        default_activities=default_activities,
    )

    return switch_activity, context


def _ensure_adf_activities(raw_activities: list[Any]) -> list[AdfActivity]:
    """Ensure a list of activities are AdfActivity instances.

    The ADF loader may have already parsed child activities from
    ``typeProperties`` into :class:`AdfActivity` objects (via
    ``_parse_activity``).  If they are still raw dicts, we convert them here.

    Args:
        raw_activities: List that may contain AdfActivity instances or raw dicts.

    Returns:
        List of AdfActivity instances.
    """
    from orchestra.parser.adf_loader import _parse_activity

    result: list[AdfActivity] = []
    for item in raw_activities:
        if isinstance(item, AdfActivity):
            result.append(item)
        elif isinstance(item, dict):
            result.append(_parse_activity(item))
        # Skip anything else
    return result
