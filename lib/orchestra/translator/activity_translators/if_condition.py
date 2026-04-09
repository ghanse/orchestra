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
from orchestra.parser.expression_parser import resolve_expression

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

# Matches: not(equals(a, b))  -- negated comparison
_NOT_COMPARISON_RE = re.compile(
    r"not\s*\(\s*(equals|greater|greaterOrEquals|less|lessOrEquals)\s*\((.+)\)\s*\)",
    re.IGNORECASE | re.DOTALL,
)

# Negation map: when not() wraps a comparison, flip the operator
_NEGATE_OP_MAP: dict[str, str] = {
    "equals": "NOT_EQUAL",
    "greater": "LESS_THAN_OR_EQUAL",
    "greaterorequals": "LESS_THAN",
    "less": "GREATER_THAN_OR_EQUAL",
    "lessorequals": "GREATER_THAN",
}


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
    op, left, right = _parse_condition(expression_raw, context)

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


def _parse_condition(expression: dict[str, Any] | str, context: TranslationContext) -> tuple[str, str, str]:
    """Parse an ADF IfCondition expression into ``(op, left, right)``.

    The operator is mapped to a Databricks ``condition_task`` operator
    (``EQUAL_TO``, ``GREATER_THAN``, etc.).  Operands that reference ADF
    activity outputs are converted to task value references
    (``{{tasks.<key>.values.<field>}}``).

    Handles ``@not(equals(a, b))`` by negating the inner comparison
    operator (e.g., ``EQUAL_TO`` becomes ``NOT_EQUAL``).

    Args:
        expression: Raw ADF expression dict or string.
        context: Translation context for resolving variables.

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

    # Try not(comparison(...)) first -- negated comparison
    m_not = _NOT_COMPARISON_RE.match(expr_str.strip())
    if m_not:
        inner_op_name = m_not.group(1).lower()
        op = _NEGATE_OP_MAP.get(inner_op_name, "NOT_EQUAL")
        args = _split_args(m_not.group(2).strip())
        left = _resolve_operand(args[0], context) if len(args) > 0 else ""
        right = _resolve_operand(args[1], context) if len(args) > 1 else ""
        return op, left, right

    m = _COMPARISON_RE.match(expr_str.strip())
    if m:
        adf_op = m.group(1).lower()
        op = _OP_MAP.get(adf_op, adf_op.upper())

        # Special case: not(single_arg) with no inner comparison
        if adf_op == "not":
            inner = m.group(2).strip()
            resolved = _resolve_operand(inner, context)
            return "NOT_EQUAL", resolved, ""

        args = _split_args(m.group(2).strip())
        left = _resolve_operand(args[0], context) if len(args) > 0 else ""
        right = _resolve_operand(args[1], context) if len(args) > 1 else ""
        return op, left, right

    # Fallback: treat the whole expression as a truthy check
    resolved = _resolve_operand(expr_str, context)
    return "NOT_EQUAL", resolved, "0"


def _resolve_operand(operand: str, context: TranslationContext) -> str:
    """Convert an ADF expression operand to a Databricks task value reference.

    Uses the shared ``resolve_expression()`` for activity output, variable,
    and pipeline property references.  Also handles nested function calls
    by trying to resolve the entire operand as an expression.

    Examples::

        activity('Lookup').output.firstRow.cnt
            -> {{tasks.Lookup.values.cnt}}

        activity('Lookup').output.value
            -> {{tasks.Lookup.values.result}}

        0  -> 0   (literal)
        'active'  -> active  (string literal)
        null  -> ""  (null literal)

    Args:
        operand: A single operand string from the parsed condition.
        context: Translation context for resolving variables.

    Returns:
        A DAB dynamic value reference or literal string.
    """
    operand = operand.strip()

    # Null literal -> empty string
    if operand.lower() == "null":
        return ""

    # String literal in single quotes
    if operand.startswith("'") and operand.endswith("'"):
        return operand[1:-1]

    # Numeric literal
    if operand.lstrip("-").replace(".", "", 1).isdigit():
        return operand

    # Strip wrapping functions like int(...) to get the inner expression
    inner = _unwrap_functions(operand)

    # Try unified expression resolution with @ prefix
    result = resolve_expression("@" + inner, context)
    if result is not None and result.kind in ("dab_ref", "literal"):
        return result.value

    # If inner != operand (we unwrapped something), also try the original
    if inner != operand:
        result = resolve_expression("@" + operand, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value

    # Unrecognised -- return as-is
    return operand


def _unwrap_functions(expr: str) -> str:
    """Strip wrapping ADF functions like ``int(...)`` to expose the inner expression.

    Args:
        expr: Expression that may be wrapped in a type-casting function.

    Returns:
        The inner expression, or the original if no wrapping detected.
    """
    # Match patterns like int(...), string(...), float(...)
    m = re.match(r"(?:int|string|float|bool)\s*\((.+)\)\s*$", expr, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return expr


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
