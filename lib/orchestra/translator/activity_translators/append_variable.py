"""Translate ADF AppendVariable activities to Databricks AppendVariableActivity IR.

AppendVariable threads context by registering the variable mapping in the
translation context, so that downstream ``@variables('name')`` references
resolve to the correct task key.
"""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, AppendVariableActivity, TranslationContext
from orchestra.parser.expression_parser import parse_expression


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> tuple[Activity, TranslationContext]:
    """Translate an AppendVariable activity and register the variable in context.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        Tuple of ``(AppendVariableActivity, updated_context)`` where the context
        now maps the variable name to this activity's task key.
    """
    tp = activity.type_properties or {}

    variable_name = tp.get("variableName", "")
    value_raw = tp.get("value", "")

    # Try deterministic expression translation
    append_value = parse_expression(value_raw, context)
    if append_value is None:
        # Fall back to raw string representation
        append_value = repr(value_raw) if isinstance(value_raw, str) else str(value_raw)

    append_var_activity = AppendVariableActivity(
        **base_kwargs,
        variable_name=variable_name,
        append_value=append_value,
    )

    # Register variable -> task_key mapping in context
    new_context = context.with_variable(variable_name, base_kwargs["task_key"])

    return append_var_activity, new_context
