"""Translate ADF Filter activities to Databricks FilterActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, FilterActivity, TranslationContext
from orchestra.parser.expression_parser import parse_expression


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate a Filter activity.

    Extracts the items expression and condition expression from ADF
    typeProperties.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`FilterActivity` IR node.
    """
    tp = activity.type_properties or {}

    items_raw = tp.get("items", {})
    items_value = items_raw.get("value", "") if isinstance(items_raw, dict) else str(items_raw)

    condition_raw = tp.get("condition", {})
    condition_value = condition_raw.get("value", "") if isinstance(condition_raw, dict) else str(condition_raw)

    # Try deterministic expression translation
    items_expression = parse_expression(items_value, context)
    if items_expression is None:
        items_expression = items_value

    condition_expression = parse_expression(condition_value, context)
    if condition_expression is None:
        condition_expression = condition_value

    return FilterActivity(
        **base_kwargs,
        items_expression=items_expression,
        condition_expression=condition_expression,
    )
