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
- All 84 ADF expression functions (string, collection, logical, conversion,
  math, date/time) -> ``notebook_code`` or ``None`` for agentic functions
"""

from __future__ import annotations

import re
from collections.abc import Callable
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

    # --- item() and item().field ---
    if _ITEM_RE.match(expr):
        return ExpressionResult(kind="dab_ref", value="{{input}}")

    m = _ITEM_FIELD_RE.match(expr)
    if m:
        field_name = m.group(1)
        # item().field requires notebook code to parse the JSON item
        return ExpressionResult(
            kind="notebook_code",
            value=f"__import__('json').loads(dbutils.widgets.get('item'))['{field_name}']",
            imports=["import json"],
        )

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

    # --- Generic function dispatch (all 84 ADF functions) ---
    result = _resolve_function_call(expr, context, variable_task_keys=variable_task_keys)
    if result is not None:
        return result

    # Unsupported expression
    return None


# Matches @{...} interpolation tokens
_INTERPOLATION_RE = re.compile(r"@\{(.+?)\}")


def resolve_interpolated_string(
    value: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> str:
    """Resolve ``@{...}`` interpolation tokens within a string.

    ADF uses ``@{expr}`` for inline string interpolation, e.g.
    ``"prefix_@{pipeline().parameters.name}_suffix"``.  Each ``@{...}``
    token is resolved through :func:`resolve_expression` and replaced
    with its resolved value.

    If the entire string is a single ``@{...}`` token (no surrounding
    text), it is treated as a full expression and the resolved value is
    returned directly.

    Args:
        value: A string potentially containing ``@{...}`` tokens.
        context: Translation context for resolving variables.
        variable_task_keys: Optional explicit variable-name-to-task-key map.

    Returns:
        The string with all ``@{...}`` tokens replaced by resolved values.
        Tokens that cannot be resolved are left unchanged.
    """
    if not isinstance(value, str):
        return value

    # Fast path: no interpolation tokens
    if "@{" not in value:
        return value

    def _replace_match(m: re.Match[str]) -> str:
        inner_expr = m.group(1)
        result = resolve_expression("@" + inner_expr, context, variable_task_keys=variable_task_keys)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value
        # Cannot resolve — leave token unchanged
        return m.group(0)

    return _INTERPOLATION_RE.sub(_replace_match, value)


def resolve_interpolated_string_for_notebook(
    value: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> str:
    """Resolve ``@{...}`` tokens to Python f-string expressions for notebook code.

    Like :func:`resolve_interpolated_string` but produces Python expressions
    suitable for embedding in an f-string inside a generated notebook (e.g.,
    ``{dbutils.widgets.get('startDate')}``), not DAB refs.

    Args:
        value: A string containing ``@{...}`` tokens.
        context: Translation context for resolving variables.
        variable_task_keys: Optional explicit variable-name-to-task-key map.

    Returns:
        A string with ``@{...}`` tokens replaced by Python f-string expressions.
    """
    if not isinstance(value, str) or "@{" not in value:
        return value

    def _replace_match(m: re.Match[str]) -> str:
        inner_expr = m.group(1)
        result = resolve_expression("@" + inner_expr, context, variable_task_keys=variable_task_keys)
        if result is None:
            return m.group(0)
        if result.kind == "literal":
            return result.value
        if result.kind == "dab_ref":
            # Convert DAB ref to Python expression for notebook f-strings
            ref = result.value
            # {{job.parameters.X}} -> dbutils.widgets.get('X')
            param_m = re.match(r"\{\{job\.parameters\.(\w+)\}\}", ref)
            if param_m:
                return "{dbutils.widgets.get('" + param_m.group(1) + "')}"
            # {{tasks.X.values.Y}} -> dbutils.jobs.taskValues.get(taskKey='X', key='Y')
            tv_m = re.match(r"\{\{tasks\.([^.]+)\.values\.(\w+)\}\}", ref)
            if tv_m:
                return "{dbutils.jobs.taskValues.get(taskKey='" + tv_m.group(1) + "', key='" + tv_m.group(2) + "')}"
            # {{job.run_id}} -> spark.conf.get('spark.databricks.job.runId', '')
            if ref == "{{job.run_id}}":
                return "{spark.conf.get('spark.databricks.job.runId', '')}"
            # {{job.name}} -> spark.conf.get('spark.databricks.job.parentName', '')
            if ref == "{{job.name}}":
                return "{spark.conf.get('spark.databricks.job.parentName', '')}"
            # {{job.start_time.iso_datetime}}
            if ref == "{{job.start_time.iso_datetime}}":
                return "{spark.conf.get('spark.databricks.job.triggerTime', '')}"
            # {{input}} -> json.loads(dbutils.widgets.get('item'))
            if ref == "{{input}}":
                return "{dbutils.widgets.get('item')}"
            return ref
        # notebook_code — embed the Python expression directly
        return "{" + result.value + "}"

    return _INTERPOLATION_RE.sub(_replace_match, value)


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

# Matches: item().fieldName
_ITEM_FIELD_RE = re.compile(r"item\(\s*\)\.(\w+)", re.IGNORECASE)

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
    return _split_args(inner)


def _split_args(inner: str) -> list[str]:
    """Split function arguments respecting nested parentheses and quoted strings.

    Args:
        inner: The inner content between the outermost parentheses.

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
# Generic function call resolver
# ---------------------------------------------------------------------------

# Matches: functionName(...)
_FUNCTION_CALL_RE = re.compile(
    r"([a-zA-Z_]\w*)\((.*)?\)$",
    re.IGNORECASE | re.DOTALL,
)


def _resolve_function_call(
    expr: str,
    context: TranslationContext,
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> ExpressionResult | None:
    """Resolve a generic ADF function call via the dispatch table.

    Matches ``functionName(args...)``, recursively resolves each argument,
    and dispatches to the registered handler.
    """
    m = _FUNCTION_CALL_RE.match(expr)
    if m is None:
        return None

    func_name = m.group(1)
    inner = (m.group(2) or "").strip()

    handler = _FUNCTION_HANDLERS.get(func_name)
    if handler is None:
        # Also try case-insensitive lookup
        handler = _FUNCTION_HANDLERS_CI.get(func_name.lower())
    if handler is None:
        return None

    # Parse arguments
    if not inner:
        raw_args: list[str] = []
    else:
        raw_args = _split_args(inner)

    # Resolve each argument
    resolved_args: list[ExpressionResult] = []
    for raw_arg in raw_args:
        raw_arg = raw_arg.strip()
        if not raw_arg:
            continue

        # String literal (single or double quoted)
        if (raw_arg.startswith("'") and raw_arg.endswith("'")) or (
            raw_arg.startswith('"') and raw_arg.endswith('"')
        ):
            resolved_args.append(ExpressionResult(kind="literal", value=raw_arg[1:-1]))
        # Numeric literal
        elif _is_numeric(raw_arg):
            resolved_args.append(ExpressionResult(kind="literal", value=raw_arg))
        # Boolean literal
        elif raw_arg.lower() in ("true", "false"):
            resolved_args.append(
                ExpressionResult(kind="literal", value="True" if raw_arg.lower() == "true" else "False")
            )
        # Null literal
        elif raw_arg.lower() == "null":
            resolved_args.append(ExpressionResult(kind="literal", value="None"))
        else:
            # Try to resolve as a sub-expression (add @ prefix if needed)
            sub_expr = raw_arg if raw_arg.startswith("@") else "@" + raw_arg
            sub_result = resolve_expression(sub_expr, context, variable_task_keys=variable_task_keys)
            if sub_result is None:
                return None  # If any argument is unresolvable, bail out
            resolved_args.append(sub_result)

    return handler(resolved_args)


def _is_numeric(s: str) -> bool:
    """Check if a string is a numeric literal."""
    try:
        float(s)
        return True
    except ValueError:
        return False


def _arg_to_code(arg: ExpressionResult) -> str:
    """Convert a resolved argument to a Python code snippet."""
    if arg.kind == "literal":
        # Check if it's a Python keyword/value that should not be quoted
        if arg.value in ("True", "False", "None") or _is_numeric(arg.value):
            return arg.value
        return repr(arg.value)
    elif arg.kind == "dab_ref":
        return _dab_ref_to_widget_code(arg.value)
    elif arg.kind == "notebook_code":
        return arg.value
    return repr(arg.value)


def _collect_imports(*args: ExpressionResult) -> list[str]:
    """Collect unique imports from resolved arguments."""
    imports: list[str] = []
    for arg in args:
        imports.extend(arg.imports)
    return list(dict.fromkeys(imports))


# ---------------------------------------------------------------------------
# String function handlers
# ---------------------------------------------------------------------------


def _handle_concat(args: list[ExpressionResult]) -> ExpressionResult | None:
    """concat(a, b, ...) -> str(a) + str(b) + ..."""
    if not args:
        return None
    parts = [f"str({_arg_to_code(a)})" for a in args]
    return ExpressionResult(
        kind="notebook_code",
        value=" + ".join(parts),
        imports=_collect_imports(*args),
    )


def _handle_ends_with(args: list[ExpressionResult]) -> ExpressionResult | None:
    """endsWith(text, search) -> str(text).endswith(str(search))"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])}).endswith(str({_arg_to_code(args[1])}))",
        imports=_collect_imports(*args),
    )


def _handle_guid(args: list[ExpressionResult]) -> ExpressionResult | None:
    """guid() -> str(uuid4()) or guid('N') -> no-dash variant."""
    if len(args) == 0:
        return ExpressionResult(
            kind="notebook_code",
            value="str(__import__('uuid').uuid4())",
        )
    if len(args) == 1 and args[0].kind == "literal" and args[0].value == "N":
        return ExpressionResult(
            kind="notebook_code",
            value="str(__import__('uuid').uuid4()).replace('-', '')",
        )
    # Default: no format spec
    return ExpressionResult(
        kind="notebook_code",
        value="str(__import__('uuid').uuid4())",
    )


def _handle_index_of(args: list[ExpressionResult]) -> ExpressionResult | None:
    """indexOf(text, search) -> str(text).lower().find(str(search).lower())"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])}).lower().find(str({_arg_to_code(args[1])}).lower())",
        imports=_collect_imports(*args),
    )


def _handle_last_index_of(args: list[ExpressionResult]) -> ExpressionResult | None:
    """lastIndexOf(text, search) -> str(text).lower().rfind(str(search).lower())"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])}).lower().rfind(str({_arg_to_code(args[1])}).lower())",
        imports=_collect_imports(*args),
    )


def _handle_replace(args: list[ExpressionResult]) -> ExpressionResult | None:
    """replace(text, old, new) -> str(text).replace(str(old), str(new))"""
    if len(args) != 3:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])}).replace(str({_arg_to_code(args[1])}), str({_arg_to_code(args[2])}))",
        imports=_collect_imports(*args),
    )


def _handle_split(args: list[ExpressionResult]) -> ExpressionResult | None:
    """split(text, delim) -> str(text).split(str(delim))"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])}).split(str({_arg_to_code(args[1])}))",
        imports=_collect_imports(*args),
    )


def _handle_starts_with(args: list[ExpressionResult]) -> ExpressionResult | None:
    """startsWith(text, search) -> str(text).lower().startswith(str(search).lower())"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])}).lower().startswith(str({_arg_to_code(args[1])}).lower())",
        imports=_collect_imports(*args),
    )


def _handle_substring(args: list[ExpressionResult]) -> ExpressionResult | None:
    """substring(text, start, length) -> str(text)[int(start):int(start)+int(length)]"""
    if len(args) != 3:
        return None
    s = _arg_to_code(args[0])
    start = _arg_to_code(args[1])
    length = _arg_to_code(args[2])
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({s})[int({start}):int({start})+int({length})]",
        imports=_collect_imports(*args),
    )


def _handle_to_lower(args: list[ExpressionResult]) -> ExpressionResult | None:
    """toLower(text) -> str(text).lower()"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])}).lower()",
        imports=_collect_imports(*args),
    )


def _handle_to_upper(args: list[ExpressionResult]) -> ExpressionResult | None:
    """toUpper(text) -> str(text).upper()"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])}).upper()",
        imports=_collect_imports(*args),
    )


def _handle_trim(args: list[ExpressionResult]) -> ExpressionResult | None:
    """trim(text) -> str(text).strip()"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])}).strip()",
        imports=_collect_imports(*args),
    )


# ---------------------------------------------------------------------------
# Collection function handlers
# ---------------------------------------------------------------------------


def _handle_contains(args: list[ExpressionResult]) -> ExpressionResult | None:
    """contains(collection, value) -> (value in collection)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[1])} in {_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


def _handle_empty(args: list[ExpressionResult]) -> ExpressionResult | None:
    """empty(collection) -> (len(collection) == 0)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"(len({_arg_to_code(args[0])}) == 0)",
        imports=_collect_imports(*args),
    )


def _handle_first(args: list[ExpressionResult]) -> ExpressionResult | None:
    """first(collection) -> collection[0]"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"{_arg_to_code(args[0])}[0]",
        imports=_collect_imports(*args),
    )


def _handle_intersection(args: list[ExpressionResult]) -> ExpressionResult | None:
    """intersection(c1, c2, ...) -> list(set(c1) & set(c2) & ...)"""
    if len(args) < 2:
        return None
    parts = " & ".join(f"set({_arg_to_code(a)})" for a in args)
    return ExpressionResult(
        kind="notebook_code",
        value=f"list({parts})",
        imports=_collect_imports(*args),
    )


def _handle_join(args: list[ExpressionResult]) -> ExpressionResult | None:
    """join(array, delim) -> str(delim).join(str(x) for x in array)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[1])}).join(str(x) for x in {_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


def _handle_last(args: list[ExpressionResult]) -> ExpressionResult | None:
    """last(collection) -> collection[-1]"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"{_arg_to_code(args[0])}[-1]",
        imports=_collect_imports(*args),
    )


def _handle_length(args: list[ExpressionResult]) -> ExpressionResult | None:
    """length(collection) -> len(collection)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"len({_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


def _handle_skip(args: list[ExpressionResult]) -> ExpressionResult | None:
    """skip(collection, count) -> collection[int(count):]"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"{_arg_to_code(args[0])}[int({_arg_to_code(args[1])}):]",
        imports=_collect_imports(*args),
    )


def _handle_take(args: list[ExpressionResult]) -> ExpressionResult | None:
    """take(collection, count) -> collection[:int(count)]"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"{_arg_to_code(args[0])}[:int({_arg_to_code(args[1])})]",
        imports=_collect_imports(*args),
    )


def _handle_union(args: list[ExpressionResult]) -> ExpressionResult | None:
    """union(c1, c2, ...) -> list(set(c1) | set(c2) | ...)"""
    if len(args) < 2:
        return None
    parts = " | ".join(f"set({_arg_to_code(a)})" for a in args)
    return ExpressionResult(
        kind="notebook_code",
        value=f"list({parts})",
        imports=_collect_imports(*args),
    )


# ---------------------------------------------------------------------------
# Logical function handlers
# ---------------------------------------------------------------------------


def _handle_and(args: list[ExpressionResult]) -> ExpressionResult | None:
    """and(a, b) -> (a and b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} and {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


def _handle_equals(args: list[ExpressionResult]) -> ExpressionResult | None:
    """equals(a, b) -> (a == b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} == {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


def _handle_greater(args: list[ExpressionResult]) -> ExpressionResult | None:
    """greater(a, b) -> (a > b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} > {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


def _handle_greater_or_equals(args: list[ExpressionResult]) -> ExpressionResult | None:
    """greaterOrEquals(a, b) -> (a >= b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} >= {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


def _handle_if(args: list[ExpressionResult]) -> ExpressionResult | None:
    """if(expr, trueVal, falseVal) -> (trueVal if expr else falseVal)"""
    if len(args) != 3:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[1])} if {_arg_to_code(args[0])} else {_arg_to_code(args[2])})",
        imports=_collect_imports(*args),
    )


def _handle_less(args: list[ExpressionResult]) -> ExpressionResult | None:
    """less(a, b) -> (a < b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} < {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


def _handle_less_or_equals(args: list[ExpressionResult]) -> ExpressionResult | None:
    """lessOrEquals(a, b) -> (a <= b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} <= {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


def _handle_not(args: list[ExpressionResult]) -> ExpressionResult | None:
    """not(expr) -> (not expr)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"(not {_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


def _handle_or(args: list[ExpressionResult]) -> ExpressionResult | None:
    """or(a, b) -> (a or b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} or {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


# ---------------------------------------------------------------------------
# Conversion function handlers
# ---------------------------------------------------------------------------


def _handle_array(args: list[ExpressionResult]) -> ExpressionResult | None:
    """array(value) -> [value]"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"[{_arg_to_code(args[0])}]",
        imports=_collect_imports(*args),
    )


def _handle_base64(args: list[ExpressionResult]) -> ExpressionResult | None:
    """base64(value) -> base64.b64encode(str(value).encode()).decode()"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"__import__('base64').b64encode(str({_arg_to_code(args[0])}).encode()).decode()",
        imports=_collect_imports(*args),
    )


def _handle_base64_to_binary(args: list[ExpressionResult]) -> ExpressionResult | None:
    """base64ToBinary(value) -> base64.b64decode(value)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"__import__('base64').b64decode({_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


def _handle_base64_to_string(args: list[ExpressionResult]) -> ExpressionResult | None:
    """base64ToString(value) -> base64.b64decode(value).decode()"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"__import__('base64').b64decode({_arg_to_code(args[0])}).decode()",
        imports=_collect_imports(*args),
    )


def _handle_binary(args: list[ExpressionResult]) -> ExpressionResult | None:
    """binary(value) -> str(value).encode()"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])}).encode()",
        imports=_collect_imports(*args),
    )


def _handle_bool(args: list[ExpressionResult]) -> ExpressionResult | None:
    """bool(value) -> bool(value)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"bool({_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


def _handle_coalesce(args: list[ExpressionResult]) -> ExpressionResult | None:
    """coalesce(a, b, ...) -> next((x for x in [a, b, ...] if x is not None), None)"""
    if not args:
        return None
    items = ", ".join(_arg_to_code(a) for a in args)
    return ExpressionResult(
        kind="notebook_code",
        value=f"next((x for x in [{items}] if x is not None), None)",
        imports=_collect_imports(*args),
    )


def _handle_create_array(args: list[ExpressionResult]) -> ExpressionResult | None:
    """createArray(a, b, ...) -> [a, b, ...]"""
    items = ", ".join(_arg_to_code(a) for a in args)
    return ExpressionResult(
        kind="notebook_code",
        value=f"[{items}]",
        imports=_collect_imports(*args),
    )


def _handle_agentic(_args: list[ExpressionResult]) -> ExpressionResult | None:
    """Return None for agentic functions that are too complex for deterministic translation."""
    return None


def _handle_decode_uri_component(args: list[ExpressionResult]) -> ExpressionResult | None:
    """decodeUriComponent(value) -> urllib.parse.unquote(value)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"__import__('urllib.parse', fromlist=['unquote']).unquote({_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


def _handle_encode_uri_component(args: list[ExpressionResult]) -> ExpressionResult | None:
    """encodeUriComponent(value) -> urllib.parse.quote(str(value), safe='')"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"__import__('urllib.parse', fromlist=['quote']).quote(str({_arg_to_code(args[0])}), safe='')",
        imports=_collect_imports(*args),
    )


def _handle_float(args: list[ExpressionResult]) -> ExpressionResult | None:
    """float(value) -> float(value)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"float({_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


def _handle_int(args: list[ExpressionResult]) -> ExpressionResult | None:
    """int(value) -> int(value)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"int({_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


def _handle_json(args: list[ExpressionResult]) -> ExpressionResult | None:
    """json(value) -> json.loads(value)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"__import__('json').loads({_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


def _handle_string(args: list[ExpressionResult]) -> ExpressionResult | None:
    """string(value) -> str(value)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"str({_arg_to_code(args[0])})",
        imports=_collect_imports(*args),
    )


# ---------------------------------------------------------------------------
# Math function handlers
# ---------------------------------------------------------------------------


def _handle_add(args: list[ExpressionResult]) -> ExpressionResult | None:
    """add(a, b) -> (a + b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} + {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


def _handle_div(args: list[ExpressionResult]) -> ExpressionResult | None:
    """div(a, b) -> (a // b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} // {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


def _handle_max(args: list[ExpressionResult]) -> ExpressionResult | None:
    """max(a, b, ...) -> max(a, b, ...)"""
    if not args:
        return None
    items = ", ".join(_arg_to_code(a) for a in args)
    return ExpressionResult(
        kind="notebook_code",
        value=f"max({items})",
        imports=_collect_imports(*args),
    )


def _handle_min(args: list[ExpressionResult]) -> ExpressionResult | None:
    """min(a, b, ...) -> min(a, b, ...)"""
    if not args:
        return None
    items = ", ".join(_arg_to_code(a) for a in args)
    return ExpressionResult(
        kind="notebook_code",
        value=f"min({items})",
        imports=_collect_imports(*args),
    )


def _handle_mod(args: list[ExpressionResult]) -> ExpressionResult | None:
    """mod(a, b) -> (a % b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} % {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


def _handle_mul(args: list[ExpressionResult]) -> ExpressionResult | None:
    """mul(a, b) -> (a * b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} * {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


def _handle_rand(args: list[ExpressionResult]) -> ExpressionResult | None:
    """rand(min, max) -> random.randint(min, max-1)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"__import__('random').randint({_arg_to_code(args[0])}, {_arg_to_code(args[1])} - 1)",
        imports=_collect_imports(*args),
    )


def _handle_range(args: list[ExpressionResult]) -> ExpressionResult | None:
    """range(start, count) -> list(range(start, start + count))"""
    if len(args) != 2:
        return None
    start = _arg_to_code(args[0])
    count = _arg_to_code(args[1])
    return ExpressionResult(
        kind="notebook_code",
        value=f"list(range({start}, {start} + {count}))",
        imports=_collect_imports(*args),
    )


def _handle_sub(args: list[ExpressionResult]) -> ExpressionResult | None:
    """sub(a, b) -> (a - b)"""
    if len(args) != 2:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"({_arg_to_code(args[0])} - {_arg_to_code(args[1])})",
        imports=_collect_imports(*args),
    )


# ---------------------------------------------------------------------------
# Date/time function handlers
# ---------------------------------------------------------------------------

_DATETIME_IMPORTS = ["from datetime import datetime, timezone, timedelta"]

# ADF time unit to Python timedelta keyword
_TIME_UNIT_MAP: dict[str, str] = {
    "Second": "seconds",
    "Minute": "minutes",
    "Hour": "hours",
    "Day": "days",
    "Week": "weeks",
}


def _handle_add_days(args: list[ExpressionResult]) -> ExpressionResult | None:
    """addDays(ts, days, fmt?) -> (datetime.fromisoformat(ts) + timedelta(days=days)).strftime(fmt)"""
    if len(args) < 2 or len(args) > 3:
        return None
    ts = _arg_to_code(args[0])
    days = _arg_to_code(args[1])
    fmt = _get_format_arg(args, 2)
    return ExpressionResult(
        kind="notebook_code",
        value=f"(datetime.fromisoformat({ts}) + timedelta(days={days})).strftime({fmt})",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_add_hours(args: list[ExpressionResult]) -> ExpressionResult | None:
    """addHours(ts, hours, fmt?) -> (datetime.fromisoformat(ts) + timedelta(hours=hours)).strftime(fmt)"""
    if len(args) < 2 or len(args) > 3:
        return None
    ts = _arg_to_code(args[0])
    hours = _arg_to_code(args[1])
    fmt = _get_format_arg(args, 2)
    return ExpressionResult(
        kind="notebook_code",
        value=f"(datetime.fromisoformat({ts}) + timedelta(hours={hours})).strftime({fmt})",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_add_minutes(args: list[ExpressionResult]) -> ExpressionResult | None:
    """addMinutes(ts, minutes, fmt?)"""
    if len(args) < 2 or len(args) > 3:
        return None
    ts = _arg_to_code(args[0])
    minutes = _arg_to_code(args[1])
    fmt = _get_format_arg(args, 2)
    return ExpressionResult(
        kind="notebook_code",
        value=f"(datetime.fromisoformat({ts}) + timedelta(minutes={minutes})).strftime({fmt})",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_add_seconds(args: list[ExpressionResult]) -> ExpressionResult | None:
    """addSeconds(ts, seconds, fmt?)"""
    if len(args) < 2 or len(args) > 3:
        return None
    ts = _arg_to_code(args[0])
    seconds = _arg_to_code(args[1])
    fmt = _get_format_arg(args, 2)
    return ExpressionResult(
        kind="notebook_code",
        value=f"(datetime.fromisoformat({ts}) + timedelta(seconds={seconds})).strftime({fmt})",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_add_to_time(args: list[ExpressionResult]) -> ExpressionResult | None:
    """addToTime(ts, interval, unit, fmt?) -> datetime + timedelta."""
    if len(args) < 3 or len(args) > 4:
        return None
    ts = _arg_to_code(args[0])
    interval = _arg_to_code(args[1])
    # unit is a literal string like 'Day', 'Hour', etc.
    unit_str = args[2].value if args[2].kind == "literal" else None
    if unit_str is None:
        return None
    td_kwarg = _TIME_UNIT_MAP.get(unit_str)
    if td_kwarg is None:
        return None
    fmt = _get_format_arg(args, 3)
    return ExpressionResult(
        kind="notebook_code",
        value=f"(datetime.fromisoformat({ts}) + timedelta({td_kwarg}={interval})).strftime({fmt})",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_day_of_month(args: list[ExpressionResult]) -> ExpressionResult | None:
    """dayOfMonth(ts) -> datetime.fromisoformat(ts).day"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"datetime.fromisoformat({_arg_to_code(args[0])}).day",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_day_of_week(args: list[ExpressionResult]) -> ExpressionResult | None:
    """dayOfWeek(ts) -> datetime.fromisoformat(ts).isoweekday() % 7 (ADF: 0=Sunday)"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"datetime.fromisoformat({_arg_to_code(args[0])}).isoweekday() % 7",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_day_of_year(args: list[ExpressionResult]) -> ExpressionResult | None:
    """dayOfYear(ts) -> datetime.fromisoformat(ts).timetuple().tm_yday"""
    if len(args) != 1:
        return None
    return ExpressionResult(
        kind="notebook_code",
        value=f"datetime.fromisoformat({_arg_to_code(args[0])}).timetuple().tm_yday",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_format_date_time(args: list[ExpressionResult]) -> ExpressionResult | None:
    """formatDateTime(ts, fmt?) -> datetime.fromisoformat(ts).strftime(converted_fmt)"""
    if len(args) < 1 or len(args) > 2:
        return None
    ts = _arg_to_code(args[0])
    if len(args) == 2 and args[1].kind == "literal":
        py_fmt = _convert_date_format(args[1].value)
        return ExpressionResult(
            kind="notebook_code",
            value=f"datetime.fromisoformat({ts}).strftime('{py_fmt}')",
            imports=_DATETIME_IMPORTS + _collect_imports(*args),
        )
    return ExpressionResult(
        kind="notebook_code",
        value=f"datetime.fromisoformat({ts}).isoformat()",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_get_future_time(args: list[ExpressionResult]) -> ExpressionResult | None:
    """getFutureTime(interval, unit, fmt?) -> (datetime.now(utc) + timedelta(...)).strftime(fmt)"""
    if len(args) < 2 or len(args) > 3:
        return None
    interval = _arg_to_code(args[0])
    unit_str = args[1].value if args[1].kind == "literal" else None
    if unit_str is None:
        return None
    td_kwarg = _TIME_UNIT_MAP.get(unit_str)
    if td_kwarg is None:
        return None
    fmt = _get_format_arg(args, 2)
    return ExpressionResult(
        kind="notebook_code",
        value=f"(datetime.now(timezone.utc) + timedelta({td_kwarg}={interval})).strftime({fmt})",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_get_past_time(args: list[ExpressionResult]) -> ExpressionResult | None:
    """getPastTime(interval, unit, fmt?) -> (datetime.now(utc) - timedelta(...)).strftime(fmt)"""
    if len(args) < 2 or len(args) > 3:
        return None
    interval = _arg_to_code(args[0])
    unit_str = args[1].value if args[1].kind == "literal" else None
    if unit_str is None:
        return None
    td_kwarg = _TIME_UNIT_MAP.get(unit_str)
    if td_kwarg is None:
        return None
    fmt = _get_format_arg(args, 2)
    return ExpressionResult(
        kind="notebook_code",
        value=f"(datetime.now(timezone.utc) - timedelta({td_kwarg}={interval})).strftime({fmt})",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_start_of_day(args: list[ExpressionResult]) -> ExpressionResult | None:
    """startOfDay(ts, fmt?) -> datetime.fromisoformat(ts).replace(hour=0,...).strftime(fmt)"""
    if len(args) < 1 or len(args) > 2:
        return None
    ts = _arg_to_code(args[0])
    fmt = _get_format_arg(args, 1)
    return ExpressionResult(
        kind="notebook_code",
        value=(f"datetime.fromisoformat({ts}).replace(hour=0, minute=0, second=0, microsecond=0).strftime({fmt})"),
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_start_of_hour(args: list[ExpressionResult]) -> ExpressionResult | None:
    """startOfHour(ts, fmt?) -> datetime.fromisoformat(ts).replace(minute=0,...).strftime(fmt)"""
    if len(args) < 1 or len(args) > 2:
        return None
    ts = _arg_to_code(args[0])
    fmt = _get_format_arg(args, 1)
    return ExpressionResult(
        kind="notebook_code",
        value=(f"datetime.fromisoformat({ts}).replace(minute=0, second=0, microsecond=0).strftime({fmt})"),
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_start_of_month(args: list[ExpressionResult]) -> ExpressionResult | None:
    """startOfMonth(ts, fmt?) -> datetime.fromisoformat(ts).replace(day=1,...).strftime(fmt)"""
    if len(args) < 1 or len(args) > 2:
        return None
    ts = _arg_to_code(args[0])
    fmt = _get_format_arg(args, 1)
    return ExpressionResult(
        kind="notebook_code",
        value=(
            f"datetime.fromisoformat({ts}).replace(day=1, hour=0, minute=0, second=0, microsecond=0).strftime({fmt})"
        ),
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _handle_subtract_from_time(args: list[ExpressionResult]) -> ExpressionResult | None:
    """subtractFromTime(ts, interval, unit, fmt?) -> datetime - timedelta."""
    if len(args) < 3 or len(args) > 4:
        return None
    ts = _arg_to_code(args[0])
    interval = _arg_to_code(args[1])
    unit_str = args[2].value if args[2].kind == "literal" else None
    if unit_str is None:
        return None
    td_kwarg = _TIME_UNIT_MAP.get(unit_str)
    if td_kwarg is None:
        return None
    fmt = _get_format_arg(args, 3)
    return ExpressionResult(
        kind="notebook_code",
        value=f"(datetime.fromisoformat({ts}) - timedelta({td_kwarg}={interval})).strftime({fmt})",
        imports=_DATETIME_IMPORTS + _collect_imports(*args),
    )


def _get_format_arg(args: list[ExpressionResult], idx: int) -> str:
    """Extract a format argument from the args list, converting ADF .NET format if needed.

    Returns a Python string expression suitable for embedding in generated code.
    """
    if idx < len(args) and args[idx].kind == "literal" and args[idx].value:
        py_fmt = _convert_date_format(args[idx].value)
        return repr(py_fmt)
    return "'%Y-%m-%dT%H:%M:%SZ'"


# ---------------------------------------------------------------------------
# Function dispatch table — maps ADF function names to handlers
# ---------------------------------------------------------------------------

_FUNCTION_HANDLERS: dict[str, Callable[[list[ExpressionResult]], ExpressionResult | None]] = {
    # --- String functions (12) ---
    "concat": _handle_concat,
    "endsWith": _handle_ends_with,
    "guid": _handle_guid,
    "indexOf": _handle_index_of,
    "lastIndexOf": _handle_last_index_of,
    "replace": _handle_replace,
    "split": _handle_split,
    "startsWith": _handle_starts_with,
    "substring": _handle_substring,
    "toLower": _handle_to_lower,
    "toUpper": _handle_to_upper,
    "trim": _handle_trim,
    # --- Collection functions (10) ---
    "contains": _handle_contains,
    "empty": _handle_empty,
    "first": _handle_first,
    "intersection": _handle_intersection,
    "join": _handle_join,
    "last": _handle_last,
    "length": _handle_length,
    "skip": _handle_skip,
    "take": _handle_take,
    "union": _handle_union,
    # --- Logical functions (9) ---
    "and": _handle_and,
    "equals": _handle_equals,
    "greater": _handle_greater,
    "greaterOrEquals": _handle_greater_or_equals,
    "if": _handle_if,
    "less": _handle_less,
    "lessOrEquals": _handle_less_or_equals,
    "not": _handle_not,
    "or": _handle_or,
    # --- Conversion functions (24) ---
    "array": _handle_array,
    "base64": _handle_base64,
    "base64ToBinary": _handle_base64_to_binary,
    "base64ToString": _handle_base64_to_string,
    "binary": _handle_binary,
    "bool": _handle_bool,
    "coalesce": _handle_coalesce,
    "createArray": _handle_create_array,
    "dataUri": _handle_agentic,
    "dataUriToBinary": _handle_agentic,
    "dataUriToString": _handle_agentic,
    "decodeBase64": _handle_base64_to_string,  # alias
    "decodeDataUri": _handle_agentic,
    "decodeUriComponent": _handle_decode_uri_component,
    "encodeUriComponent": _handle_encode_uri_component,
    "float": _handle_float,
    "int": _handle_int,
    "json": _handle_json,
    "string": _handle_string,
    "uriComponent": _handle_encode_uri_component,  # alias
    "uriComponentToBinary": _handle_agentic,
    "uriComponentToString": _handle_decode_uri_component,  # alias
    "xml": _handle_agentic,
    "xpath": _handle_agentic,
    # --- Math functions (9) ---
    "add": _handle_add,
    "div": _handle_div,
    "max": _handle_max,
    "min": _handle_min,
    "mod": _handle_mod,
    "mul": _handle_mul,
    "rand": _handle_rand,
    "range": _handle_range,
    "sub": _handle_sub,
    # --- Date/time functions (20) ---
    "addDays": _handle_add_days,
    "addHours": _handle_add_hours,
    "addMinutes": _handle_add_minutes,
    "addSeconds": _handle_add_seconds,
    "addToTime": _handle_add_to_time,
    "convertFromUtc": _handle_agentic,
    "convertTimeZone": _handle_agentic,
    "convertToUtc": _handle_agentic,
    "dayOfMonth": _handle_day_of_month,
    "dayOfWeek": _handle_day_of_week,
    "dayOfYear": _handle_day_of_year,
    "formatDateTime": _handle_format_date_time,
    "getFutureTime": _handle_get_future_time,
    "getPastTime": _handle_get_past_time,
    "startOfDay": _handle_start_of_day,
    "startOfHour": _handle_start_of_hour,
    "startOfMonth": _handle_start_of_month,
    "subtractFromTime": _handle_subtract_from_time,
    "ticks": _handle_agentic,
    # NOTE: utcNow is intentionally absent — handled by dedicated _resolve_utcnow
    # upstream in resolve_expression() before the generic dispatch is reached.
}

# Build case-insensitive lookup (excluding None entries)
_FUNCTION_HANDLERS_CI: dict[str, Callable[[list[ExpressionResult]], ExpressionResult | None]] = {
    k.lower(): v for k, v in _FUNCTION_HANDLERS.items() if v is not None
}
