"""Translate ADF IfCondition activities to Databricks IfConditionActivity IR.

IfCondition is a control-flow container that threads context through both
branch translations.  Returns a ``(Activity, TranslationContext)`` tuple.
"""

from __future__ import annotations

import re
from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, IfConditionActivity, TranslationContext

# Common ADF comparison functions
_COMPARISON_RE = re.compile(
    r"""(equals|greater|greaterOrEquals|less|lessOrEquals|not|and|or|contains|startsWith|endsWith|empty)\s*\((.+)\)""",
    re.IGNORECASE | re.DOTALL,
)


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
    *,
    translate_activities_fn: Any = None,
) -> tuple[Activity, TranslationContext]:
    """Translate an IfCondition activity with recursive branch translation.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.
        translate_activities_fn: Callback to translate branch activities.
            Signature: ``(activities, context, definitions) -> (list[Activity], TranslationContext)``.

    Returns:
        Tuple of ``(IfConditionActivity, updated_context)``.
    """
    tp = activity.type_properties or {}

    # Parse expression
    expression_raw = tp.get("expression", {})
    op, left, right = _parse_condition(expression_raw)

    # Translate true branch
    if_true_activities: list[Activity] = []
    if_true_adf = activity.if_true_activities or []
    if translate_activities_fn and if_true_adf:
        if_true_activities, _ = translate_activities_fn(if_true_adf, context, definitions)

    # Translate false branch
    if_false_activities: list[Activity] = []
    if_false_adf = activity.if_false_activities or []
    if translate_activities_fn and if_false_adf:
        if_false_activities, _ = translate_activities_fn(if_false_adf, context, definitions)

    if_activity = IfConditionActivity(
        **base_kwargs,
        op=op,
        left=left,
        right=right,
        if_true_activities=if_true_activities,
        if_false_activities=if_false_activities,
    )

    return if_activity, context


def _parse_condition(expression: dict[str, Any] | str) -> tuple[str, str, str]:
    """Parse an ADF IfCondition expression into ``(op, left, right)``.

    ADF expressions use a ``{"type": "Expression", "value": "@equals(a, b)"}``
    format.  This function extracts the comparison operator and operands.

    Args:
        expression: Raw ADF expression dict or string.

    Returns:
        Tuple of ``(operator_name, left_operand, right_operand)``.
        For unary operators, *right* is an empty string.
    """
    expr_str = ""
    if isinstance(expression, dict):
        expr_str = expression.get("value", "")
    elif isinstance(expression, str):
        expr_str = expression

    # Strip leading @
    if expr_str.startswith("@"):
        expr_str = expr_str[1:]

    m = _COMPARISON_RE.match(expr_str.strip())
    if m:
        op = m.group(1)
        args_str = m.group(2).strip()
        args = _split_args(args_str)
        left = args[0] if len(args) > 0 else ""
        right = args[1] if len(args) > 1 else ""
        return op, left, right

    # Fallback: return the whole expression as left with unknown op
    return "unknown", expr_str, ""


def _split_args(args_str: str) -> list[str]:
    """Split function arguments respecting nested parentheses and quotes.

    Args:
        args_str: Comma-separated argument string.

    Returns:
        List of argument strings, stripped of leading/trailing whitespace.
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    in_quote = False

    for ch in args_str:
        if ch == "'" and depth == 0:
            in_quote = not in_quote
            current.append(ch)
        elif in_quote:
            current.append(ch)
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)

    if current:
        parts.append("".join(current).strip())

    return parts
