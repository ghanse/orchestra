"""Translate ADF SetVariable activities to Databricks SetVariableActivity IR.

SetVariable threads context by registering the variable mapping in the
translation context, so that downstream ``@variables('name')`` references
resolve to the correct task key.
"""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, SetVariableActivity, TranslationContext
from orchestra.parser.expression_parser import parse_expression


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> tuple[Activity, TranslationContext]:
    """Translate a SetVariable activity and register the variable in context.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        Tuple of ``(SetVariableActivity, updated_context)`` where the context
        now maps the variable name to this activity's task key.
    """
    tp = activity.type_properties or {}

    variable_name = tp.get("variableName", "")
    value_raw = tp.get("value", "")

    # Try deterministic expression translation
    variable_value = parse_expression(value_raw, context)
    if variable_value is None:
        # Fall back to raw string representation
        variable_value = repr(value_raw) if isinstance(value_raw, str) else str(value_raw)

    set_var_activity = SetVariableActivity(
        **base_kwargs,
        variable_name=variable_name,
        variable_value=variable_value,
    )

    # Register variable -> task_key mapping in context
    new_context = context.with_variable(variable_name, base_kwargs["task_key"])

    return set_var_activity, new_context
