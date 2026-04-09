"""Translate ADF expressions to a unified ExpressionResult.

Provides a single ``resolve_expression()`` that returns an
:class:`ExpressionResult` discriminated union with kind ``"literal"``,
``"dab_ref"`` or ``"notebook_code"``.  Thin wrappers ``parse_expression()``
and ``parse_expression_for_dab()`` are kept for backward compatibility.

Supported patterns
------------------
- Literal values (non-expression strings) -> ``ExpressionResult(kind="literal")``
- ``@pipeline().RunId`` / ``.Pipeline`` / ``.TriggerTime`` / ``.GroupId`` -> ``dab_ref``
- ``@pipeline().parameters.X`` -> ``dab_ref``
- ``@activity('Name').output...`` -> ``dab_ref``
- ``@variables('name')`` -> ``dab_ref``
- ``@item()`` -> ``dab_ref``
- ``@utcNow()`` / ``@utcNow('fmt')`` -> ``notebook_code``
- ``@concat(...)`` -> ``notebook_code`` (builds Python string expression)
"""

from __future__ import annotations

import re
from typing import Any

from orchestra.models.ir import ExpressionResult, TranslationContext

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_expression(
    value: str | dict[str, Any] | int | float | bool,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """Translate an ADF expression to an :class:`ExpressionResult`.

    Args:
        value: The ADF expression value.  May be a plain scalar, an
            ``@``-prefixed expression string, or an ``{"type": "Expression",
            "value": "..."}`` dict.
        context: Translation context carrying variable mappings.
        variable_task_keys: Optional explicit mapping of variable names to
            setter task keys.  When provided these take precedence over
            ``context.variable_cache``.

    Returns:
        An :class:`ExpressionResult`, or ``None`` if the expression is too
        complex for deterministic translation.
    """
    # Unwrap expression-type dicts
    if isinstance(value, dict):
        if value.get("type") == "Expression" and "value" in value:
            return resolve_expression(value["value"], context, variable_task_keys=variable_task_keys)
        return None

    # Numeric and boolean literals
    if isinstance(value, bool):
        return ExpressionResult(kind="literal", value=str(value))
    if isinstance(value, (int, float)):
        return ExpressionResult(kind="literal", value=str(value))

    # From here on, value must be a string
    if not isinstance(value, str):
        return None

    # Non-expression strings are literal values
    if not value.startswith("@"):
        return ExpressionResult(kind="literal", value=value)

    expr = value[1:]  # strip leading @

    # --- item() ---
    if _ITEM_RE.match(expr):
        return ExpressionResult(kind="dab_ref", value="{{input}}")

    # --- Pipeline parameters ---
    result = _resolve_pipeline_param(expr)
    if result is not None:
        return result

    # --- Pipeline properties ---
    result = _resolve_pipeline_property(expr)
    if result is not None:
        return result

    # --- Activity output references ---
    result = _resolve_activity_output(expr)
    if result is not None:
        return result

    # --- Variables ---
    result = _resolve_variable(expr, context, variable_task_keys=variable_task_keys)
    if result is not None:
        return result

    # --- utcNow / utcnow ---
    result = _resolve_utcnow(expr)
    if result is not None:
        return result

    # --- Concat ---
    result = _resolve_concat(expr, context, variable_task_keys=variable_task_keys)
    if result is not None:
        return result

    # Unsupported expression
    return None


def parse_expression(value: str | dict[str, Any] | int | float | bool, context: TranslationContext) -> str | None:
    """Backward-compatible wrapper: return the resolved value for any kind, or None.

    Args:
        value: The ADF expression value.
        context: Translation context.

    Returns:
        A string value, or ``None`` for unsupported expressions.
    """
    result = resolve_expression(value, context)
    if result is None:
        return None
    return result.value


def parse_expression_for_dab(
    value: str | dict[str, Any] | int | float | bool,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> str | None:
    """Backward-compatible wrapper: return DAB dynamic value ref or None.

    Returns the resolved value only when the kind is ``"dab_ref"``.
    Returns ``None`` for ``"literal"`` (no mapping needed) and
    ``"notebook_code"`` (must not go in base_parameters).

    Args:
        value: The ADF expression value.
        variable_task_keys: Optional mapping of variable names to setter task keys.

    Returns:
        A DAB dynamic value reference string, or ``None``.
    """
    context = TranslationContext()
    result = resolve_expression(value, context, variable_task_keys=variable_task_keys)
    if result is None:
        return None
    if result.kind == "dab_ref":
        return result.value
    # literals don't need DAB mapping; notebook_code must not be placed in params
    return None


# ---------------------------------------------------------------------------
# Compiled regexes
# ---------------------------------------------------------------------------

# Matches: item()
_ITEM_RE = re.compile(r"item\(\s*\)$", re.IGNORECASE)

# Matches: activity('ActivityName').output.firstRow.col  (and variants)
_ACTIVITY_OUTPUT_RE = re.compile(
    r"""activity\(\s*'([^']+)'\s*\)\.output(?:\.(.+))?""",
    re.IGNORECASE,
)

# Matches: pipeline().parameters.ParamName
_PIPELINE_PARAM_RE = re.compile(
    r"""pipeline\(\s*\)\.parameters\.(\w+)""",
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

# Map well-known pipeline() properties to DAB dynamic value references
_DAB_PIPELINE_PROPERTY_MAP: dict[str, str] = {
    "RunId": "{{job.run_id}}",
    "GroupId": "{{job.run_id}}",
    "TriggerTime": "{{job.start_time.iso_datetime}}",
    "Pipeline": "{{job.name}}",
    "TriggerName": "{{job.trigger.type}}",
    "DataFactory": "{{job.run_id}}",
}


# ---------------------------------------------------------------------------
# Internal resolvers
# ---------------------------------------------------------------------------


def _resolve_pipeline_param(expr: str) -> ExpressionResult | None:
    """Resolve ``pipeline().parameters.X`` -> DAB ref."""
    m = _PIPELINE_PARAM_RE.match(expr)
    if m is None:
        return None
    param_name = m.group(1)
    return ExpressionResult(kind="dab_ref", value="{{" + f"job.parameters.{param_name}" + "}}")


def _resolve_pipeline_property(expr: str) -> ExpressionResult | None:
    """Resolve ``pipeline().PropertyName`` -> DAB ref."""
    m = _PIPELINE_PROPERTY_RE.match(expr)
    if m is None:
        return None
    prop = m.group(1)
    # Skip 'parameters' — handled by _resolve_pipeline_param
    if prop == "parameters":
        return None
    dab_ref = _DAB_PIPELINE_PROPERTY_MAP.get(prop)
    if dab_ref is not None:
        return ExpressionResult(kind="dab_ref", value=dab_ref)
    return None


def _resolve_activity_output(expr: str) -> ExpressionResult | None:
    """Resolve ``activity('Name').output...`` -> DAB ref."""
    m = _ACTIVITY_OUTPUT_RE.match(expr)
    if m is None:
        return None
    activity_name = m.group(1)
    # Sanitize to task_key
    task_key = re.sub(r"[^a-zA-Z0-9_-]", "_", activity_name)
    task_key = re.sub(r"_+", "_", task_key).strip("_") or "unnamed"

    property_path = m.group(2) or ""
    if property_path:
        parts = property_path.split(".")
        # Skip "firstRow" — it's a Lookup wrapper, the actual value key is the column
        field = parts[-1] if parts[-1] != "firstRow" else "result"
        if field == "value":
            field = "result"
    else:
        field = "result"

    return ExpressionResult(kind="dab_ref", value="{{" + f"tasks.{task_key}.values.{field}" + "}}")


def _resolve_variable(
    expr: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """Resolve ``variables('name')`` -> DAB ref."""
    m = _VARIABLE_RE.match(expr)
    if m is None:
        return None
    var_name = m.group(1)

    # Look up setter task key: explicit mapping takes precedence
    vtk = variable_task_keys or {}
    setter_key = vtk.get(var_name) or context.get_variable_task_key(var_name) or var_name
    return ExpressionResult(kind="dab_ref", value="{{" + f"tasks.{setter_key}.values.{var_name}" + "}}")


def _resolve_utcnow(expr: str) -> ExpressionResult | None:
    """Resolve ``utcNow()`` or ``utcNow('format')`` -> notebook_code."""
    m = _UTCNOW_RE.match(expr)
    if m is None:
        return None
    fmt = m.group(1)
    imports = ["from datetime import datetime, timezone"]
    if fmt:
        py_fmt = _convert_date_format(fmt)
        return ExpressionResult(
            kind="notebook_code",
            value=f"datetime.now(timezone.utc).strftime('{py_fmt}')",
            imports=imports,
        )
    return ExpressionResult(
        kind="notebook_code",
        value="datetime.now(timezone.utc).isoformat()",
        imports=imports,
    )


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


def _resolve_concat(
    expr: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """Resolve ``concat(arg1, arg2, ...)`` -> notebook_code.

    Concat always produces notebook_code because its arguments may include
    DAB refs that need to be read from widget parameters at runtime.
    """
    m = _CONCAT_RE.match(expr)
    if m is None:
        return None

    inner = m.group(1).strip()
    parts: list[str] = _split_concat_args(inner)
    if not parts:
        return None

    all_imports: list[str] = []
    code_parts: list[str] = []

    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part.startswith("'") and part.endswith("'"):
            # String literal inside concat
            code_parts.append(repr(part[1:-1]))
        else:
            # Try to resolve as a sub-expression
            sub_result = resolve_expression("@" + part, context, variable_task_keys=variable_task_keys)
            if sub_result is None:
                return None
            if sub_result.kind == "literal":
                code_parts.append(repr(sub_result.value))
            elif sub_result.kind == "dab_ref":
                # DAB refs will be passed as widget parameters at runtime;
                # generate code that reads the value from dbutils.widgets
                # The actual param name is derived from the ref
                code_parts.append(_dab_ref_to_widget_code(sub_result.value))
            elif sub_result.kind == "notebook_code":
                code_parts.append(f"str({sub_result.value})")
                all_imports.extend(sub_result.imports)

    if not code_parts:
        return None

    # Build Python concatenation expression
    value = " + ".join(code_parts)
    return ExpressionResult(kind="notebook_code", value=value, imports=list(dict.fromkeys(all_imports)))


def _dab_ref_to_widget_code(dab_ref: str) -> str:
    """Convert a DAB ref like ``{{tasks.X.values.Y}}`` to widget get code.

    The preparer will ensure the DAB ref is passed as a named parameter,
    so the notebook can read it via ``dbutils.widgets.get(...)``.
    """
    # Extract a reasonable param name from the DAB ref
    # {{tasks.SetRunDate.values.runDate}} -> "runDate"
    # {{job.parameters.X}} -> "X"
    # {{job.run_id}} -> "run_id"
    inner = dab_ref.strip("{}")
    param_name = inner.split(".")[-1]
    return f"dbutils.widgets.get('{param_name}')"


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
