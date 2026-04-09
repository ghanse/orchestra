"""Translate ADF IfCondition activities to Databricks IfConditionActivity IR.

IfCondition is a control-flow container that threads context through both
branch translations.  Returns a ``(Activity, TranslationContext)`` tuple.

The ADF expression is parsed into a structured ``(op, left, right)`` triple.
Operands that reference activity outputs are converted to Databricks task
value references (``{{tasks.<key>.values.<field>}}``).

References:
- https://docs.databricks.com/aws/en/jobs/conditional-tasks
"""

from __future__ import annotations

import re
from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, IfConditionActivity, TranslationContext

# ---------------------------------------------------------------------------
# ADF comparison function -> Databricks condition_task op mapping
# ---------------------------------------------------------------------------

_OP_MAP: dict[str, str] = {
    "equals": "EQUAL_TO",
    "greater": "GREATER_THAN",
    "greaterorequals": "GREATER_THAN_OR_EQUAL",
    "less": "LESS_THAN",
    "lessorequals": "LESS_THAN_OR_EQUAL",
    "not": "NOT_EQUAL",
}

# Matches: equals(...), greater(...), etc.
_COMPARISON_RE = re.compile(
    r"(equals|greater|greaterOrEquals|less|lessOrEquals|not)\s*\((.+)\)",
    re.IGNORECASE | re.DOTALL,
)

# Matches: activity('Name').output.firstRow.col  (and variants)
_ACTIVITY_OUTPUT_RE = re.compile(
    r"activity\(\s*'([^']+)'\s*\)\.output(?:\.(.+))?",
    re.IGNORECASE,
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

    The operator is mapped to a Databricks ``condition_task`` operator
    (``EQUAL_TO``, ``GREATER_THAN``, etc.).  Operands that reference ADF
    activity outputs are converted to task value references
    (``{{tasks.<key>.values.<field>}}``).

    Args:
        expression: Raw ADF expression dict or string.

    Returns:
        Tuple of ``(databricks_op, left_operand, right_operand)``.
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
        adf_op = m.group(1).lower()
        op = _OP_MAP.get(adf_op, adf_op.upper())
        args = _split_args(m.group(2).strip())
        left = _resolve_operand(args[0]) if len(args) > 0 else ""
        right = _resolve_operand(args[1]) if len(args) > 1 else ""
        return op, left, right

    # Fallback: treat the whole expression as a truthy check
    resolved = _resolve_operand(expr_str)
    return "NOT_EQUAL", resolved, "0"


def _resolve_operand(operand: str) -> str:
    """Convert an ADF expression operand to a Databricks task value reference.

    Examples::

        activity('Lookup').output.firstRow.cnt
            -> {{tasks.Lookup.values.cnt}}

        activity('Lookup').output.value
            -> {{tasks.Lookup.values.result}}

        0  -> 0   (literal)
        'active'  -> active  (string literal)

    Args:
        operand: A single operand string from the parsed condition.

    Returns:
        A DAB dynamic value reference or literal string.
    """
    operand = operand.strip()

    # String literal in single quotes
    if operand.startswith("'") and operand.endswith("'"):
        return operand[1:-1]

    # Numeric literal
    if operand.lstrip("-").replace(".", "", 1).isdigit():
        return operand

    # Activity output reference
    m = _ACTIVITY_OUTPUT_RE.match(operand)
    if m:
        activity_name = m.group(1)
        # Sanitize to task_key
        task_key = re.sub(r"[^a-zA-Z0-9_-]", "_", activity_name)
        task_key = re.sub(r"_+", "_", task_key).strip("_") or "unnamed"

        property_path = m.group(2) or ""
        # Extract the deepest field name for the task value key
        # e.g., "firstRow.cnt" -> "cnt", "value" -> "result"
        if property_path:
            parts = property_path.split(".")
            # Skip "firstRow" — it's a Lookup wrapper, the actual value key is the column
            field = parts[-1] if parts[-1] != "firstRow" else "result"
            if field == "value":
                field = "result"
        else:
            field = "result"

        return "{{" + f"tasks.{task_key}.values.{field}" + "}}"

    # Unrecognised — return as-is
    return operand


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
