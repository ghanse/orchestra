"""Translate ADF expressions to Python expression strings or DAB dynamic values.

Handles the most common ADF expression patterns deterministically.  Anything
that cannot be resolved is returned as ``None`` so that the caller can route
the expression to an agentic translator.

Supported patterns
------------------
- Literal values (non-expression strings) -> ``repr(value)``
- Activity output references -> ``dbutils.jobs.taskValues.get(...)``
- Pipeline run-time properties (``@pipeline().RunId``, etc.)
- Variable references (``@variables('name')``)
- Expression-type dicts (``{"type": "Expression", "value": "@..."}``)
"""

from __future__ import annotations

import re
from typing import Any

from orchestra.models.ir import TranslationContext

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_expression(value: str | dict[str, Any] | int | float | bool, context: TranslationContext) -> str | None:
    """Translate an ADF expression to a Python expression string.

    Args:
        value: The ADF expression value.  May be a plain scalar, an
            ``@``-prefixed expression string, or an ``{"type": "Expression",
            "value": "..."}`` dict.
        context: Translation context carrying variable mappings and the current
            pipeline name.

    Returns:
        A Python expression string, or ``None`` if the expression is too
        complex for deterministic translation (should be routed to the agentic
        expression translator).
    """
    # Unwrap expression-type dicts
    if isinstance(value, dict):
        if value.get("type") == "Expression" and "value" in value:
            return parse_expression(value["value"], context)
        return None

    # Numeric and boolean literals
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, bool):
        return repr(value)

    # From here on, value must be a string
    if not isinstance(value, str):
        return None

    # Non-expression strings are literal values
    if not value.startswith("@"):
        return repr(value)

    expr = value[1:]  # strip leading @

    # --- Activity output references ---
    result = _parse_activity_output(expr)
    if result is not None:
        return result

    # --- Pipeline properties ---
    result = _parse_pipeline_property(expr)
    if result is not None:
        return result

    # --- Variables ---
    result = _parse_variable(expr, context)
    if result is not None:
        return result

    # --- Concat ---
    result = _parse_concat(expr, context)
    if result is not None:
        return result

    # --- utcNow / utcnow ---
    result = _parse_utcnow(expr)
    if result is not None:
        return result

    # Unsupported expression — route to agentic
    return None


# ---------------------------------------------------------------------------
# Internal parsers
# ---------------------------------------------------------------------------

# Matches: activity('ActivityName').output.firstRow.col  (and variants)
_ACTIVITY_OUTPUT_RE = re.compile(
    r"""activity\(\s*'([^']+)'\s*\)\.output(?:\.(.+))?""",
    re.IGNORECASE,
)

# Matches: pipeline().RunId, pipeline().GroupId, pipeline().TriggerName, etc.
_PIPELINE_PROPERTY_RE = re.compile(
    r"""pipeline\(\s*\)\.(\w+)""",
    re.IGNORECASE,
)

# Matches: variables('varName')
_VARIABLE_RE = re.compile(
    r"""variables\(\s*'([^']+)'\s*\)""",
    re.IGNORECASE,
)

# Matches: concat(...)
_CONCAT_RE = re.compile(
    r"""concat\((.+)\)""",
    re.IGNORECASE | re.DOTALL,
)

# Matches: utcNow(), utcNow('format')
_UTCNOW_RE = re.compile(
    r"""utcNow\(\s*(?:'([^']*)')?\s*\)""",
    re.IGNORECASE,
)

# ADF .NET date format -> Python strftime mapping
_DATE_FORMAT_MAP: dict[str, str] = {
    "yyyy": "%Y",
    "yy": "%y",
    "MM": "%m",
    "dd": "%d",
    "HH": "%H",
    "hh": "%I",
    "mm": "%M",
    "ss": "%S",
    "fff": "%f",
    "tt": "%p",
}

# Map well-known pipeline() properties to dbutils equivalents
_PIPELINE_PROPERTY_MAP: dict[str, str] = {
    "RunId": "dbutils.jobs.getContext().tags().get('runId', '')",
    "GroupId": "dbutils.jobs.getContext().tags().get('groupId', '')",
    "TriggerName": "dbutils.jobs.getContext().tags().get('triggerName', '')",
    "TriggerType": "dbutils.jobs.getContext().tags().get('triggerType', '')",
    "TriggerId": "dbutils.jobs.getContext().tags().get('triggerId', '')",
    "TriggerTime": "dbutils.jobs.getContext().tags().get('triggerTime', '')",
    "DataFactory": "dbutils.jobs.getContext().tags().get('orgId', '')",
    "parameters": "dbutils.widgets.getAll()",
}


def _parse_activity_output(expr: str) -> str | None:
    """Parse ``activity('Name').output...`` references.

    Args:
        expr: Expression string without the leading ``@``.

    Returns:
        Python expression string, or ``None``.
    """
    m = _ACTIVITY_OUTPUT_RE.match(expr)
    if m is None:
        return None
    activity_name = m.group(1)
    return f"dbutils.jobs.taskValues.get(taskKey='{activity_name}', key='result')"


def _parse_pipeline_property(expr: str) -> str | None:
    """Parse ``pipeline().PropertyName`` references.

    Args:
        expr: Expression string without the leading ``@``.

    Returns:
        Python expression string, or ``None``.
    """
    m = _PIPELINE_PROPERTY_RE.match(expr)
    if m is None:
        return None
    prop = m.group(1)
    return _PIPELINE_PROPERTY_MAP.get(prop)


def _parse_variable(expr: str, context: TranslationContext) -> str | None:
    """Parse ``variables('name')`` references.

    Args:
        expr: Expression string without the leading ``@``.
        context: Translation context with variable-to-task-key mappings.

    Returns:
        Python expression string, or ``None``.
    """
    m = _VARIABLE_RE.match(expr)
    if m is None:
        return None
    var_name = m.group(1)
    task_key = context.get_variable_task_key(var_name)
    if task_key:
        return f"dbutils.jobs.taskValues.get(taskKey='{task_key}', key='{var_name}')"
    # Fall back to a sensible default with the pipeline-level variable task key
    return f"dbutils.jobs.taskValues.get(taskKey='__variables__', key='{var_name}')"


def _parse_utcnow(expr: str) -> str | None:
    """Parse ``utcNow()`` or ``utcNow('format')`` expressions.

    Args:
        expr: Expression string without the leading ``@``.

    Returns:
        Python expression string, or ``None``.
    """
    m = _UTCNOW_RE.match(expr)
    if m is None:
        return None
    fmt = m.group(1)
    if fmt:
        py_fmt = _convert_date_format(fmt)
        return f"__import__('datetime').datetime.utcnow().strftime('{py_fmt}')"
    return "__import__('datetime').datetime.utcnow().isoformat()"


def _convert_date_format(adf_format: str) -> str:
    """Convert an ADF .NET date format string to Python strftime format.

    Args:
        adf_format: ADF format string (e.g., ``"yyyy-MM-dd"``).

    Returns:
        Python strftime format string (e.g., ``"%Y-%m-%d"``).
    """
    result = adf_format
    # Replace longest tokens first to avoid partial matches
    for adf_token, py_token in sorted(_DATE_FORMAT_MAP.items(), key=lambda x: -len(x[0])):
        result = result.replace(adf_token, py_token)
    return result


def _parse_concat(expr: str, context: TranslationContext) -> str | None:
    """Parse ``concat(arg1, arg2, ...)`` expressions.

    This handles simple cases where arguments are string literals or nested
    single-level expressions.  Deeply nested or complex concat calls are
    returned as ``None`` for agentic handling.

    Args:
        expr: Expression string without the leading ``@``.
        context: Translation context.

    Returns:
        Python f-string or concatenation expression, or ``None``.
    """
    m = _CONCAT_RE.match(expr)
    if m is None:
        return None

    inner = m.group(1).strip()
    parts: list[str] = _split_concat_args(inner)
    if not parts:
        return None

    translated_parts: list[str] = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part.startswith("'") and part.endswith("'"):
            # String literal inside concat
            translated_parts.append(repr(part[1:-1]))
        else:
            # Try to parse as a sub-expression
            sub_result = parse_expression("@" + part, context)
            if sub_result is None:
                return None
            translated_parts.append(sub_result)

    if not translated_parts:
        return None

    # Join with string concatenation
    return " + ".join(translated_parts)


def _split_concat_args(inner: str) -> list[str]:
    """Split concat arguments respecting nested parentheses and quoted strings.

    Args:
        inner: The inner content of ``concat(...)``.

    Returns:
        List of argument strings.
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    in_quote = False

    for ch in inner:
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


# ---------------------------------------------------------------------------
# DAB Dynamic Value Reference mode
# ---------------------------------------------------------------------------

# Matches: pipeline().parameters.ParamName
_PIPELINE_PARAM_RE = re.compile(
    r"""pipeline\(\s*\)\.parameters\.(\w+)""",
    re.IGNORECASE,
)

# Map well-known pipeline() properties to DAB dynamic value references
# See https://docs.databricks.com/aws/en/jobs/dynamic-value-references
_DAB_PIPELINE_PROPERTY_MAP: dict[str, str] = {
    "RunId": "{{job.run_id}}",
    "GroupId": "{{job.run_id}}",
    "TriggerTime": "{{job.start_time.iso_datetime}}",
    "Pipeline": "{{job.name}}",
    "TriggerName": "{{job.trigger.type}}",
    "DataFactory": "{{job.run_id}}",
}


def parse_expression_for_dab(
    value: str | dict[str, Any] | int | float | bool,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> str | None:
    """Translate an ADF expression to a DAB dynamic value reference.

    This is an alternative to :func:`parse_expression` that outputs Lakeflow
    dynamic value references (``{{job.run_id}}``, ``{{job.parameters.X}}``, etc.)
    instead of Python code.  Intended for use in ``base_parameters`` of
    ``notebook_task`` definitions in the DAB YAML.

    Args:
        value: The ADF expression value.  May be a plain scalar, an
            ``@``-prefixed expression string, or an ``{"type": "Expression",
            "value": "..."}`` dict.
        variable_task_keys: Optional mapping of variable names to the task keys
            of the SetVariable activities that set them.  Used to resolve
            ``@variables('name')`` references.

    Returns:
        A DAB dynamic value reference string, or ``None`` if the expression
        cannot be mapped.
    """
    # Unwrap expression-type dicts
    if isinstance(value, dict):
        if value.get("type") == "Expression" and "value" in value:
            return parse_expression_for_dab(value["value"], variable_task_keys=variable_task_keys)
        return None

    # Non-string values have no DAB mapping
    if not isinstance(value, str):
        return None

    # Non-expression strings are literal values — no mapping needed
    if not value.startswith("@"):
        return None

    expr = value[1:]  # strip leading @

    # --- Pipeline parameters ---
    m = _PIPELINE_PARAM_RE.match(expr)
    if m is not None:
        param_name = m.group(1)
        return "{{" + f"job.parameters.{param_name}" + "}}"

    # --- Pipeline properties ---
    m = _PIPELINE_PROPERTY_RE.match(expr)
    if m is not None:
        prop = m.group(1)
        dab_ref = _DAB_PIPELINE_PROPERTY_MAP.get(prop)
        if dab_ref is not None:
            return dab_ref

    # --- Variables --- @variables('name') -> {{tasks.<setter_key>.values.<name>}}
    m = _VARIABLE_RE.match(expr)
    if m is not None:
        var_name = m.group(1)
        vtk = variable_task_keys or {}
        setter_key = vtk.get(var_name, var_name)
        return "{{" + f"tasks.{setter_key}.values.{var_name}" + "}}"

    # --- Activity output references ---
    m = _ACTIVITY_OUTPUT_RE.match(expr)
    if m is not None:
        activity_name = m.group(1)
        task_key = re.sub(r"[^a-zA-Z0-9_-]", "_", activity_name)
        task_key = re.sub(r"_+", "_", task_key).strip("_") or "unnamed"
        property_path = m.group(2) or ""
        if property_path:
            parts = property_path.split(".")
            field = parts[-1] if parts[-1] != "firstRow" else "result"
            if field == "value":
                field = "result"
        else:
            field = "result"
        return "{{" + f"tasks.{task_key}.values.{field}" + "}}"

    # No mapping found
    return None
