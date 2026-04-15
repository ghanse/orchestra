"""Collect and normalize parameters for ForEach inner jobs.

Scans task dicts and their ``base_parameters`` for ADF expression references
(``@item()``, ``@pipeline().parameters.X``, ``@variables('Y')``) and:

1. Collects them into a set of parameter declarations the inner job must
   receive (e.g. ``item``, ``environment``, ``quality_threshold``).
2. Normalises raw ADF expression dicts in ``base_parameters`` and
   ``condition_task`` operands to ``{{job.parameters.*}}`` dynamic value
   references that Databricks understands.
3. Resolves simple ADF functions like ``@concat(...)`` to plain strings.
4. Builds the ``job_parameters`` pass-through map so the parent
   ``for_each_task`` forwards the right values into the inner job.
"""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Regex patterns for ADF expression references
# ---------------------------------------------------------------------------

# The leading @ is optional — ADF only puts it on the outermost expression;
# inner references like concat('x', pipeline().parameters.Y) are bare.
_ITEM_BARE_RE = re.compile(r"@?item\(\s*\)", re.IGNORECASE)
_ITEM_FIELD_RE = re.compile(r"@?item\(\s*\)\.(\w+)", re.IGNORECASE)
_PIPELINE_PARAM_RE = re.compile(r"@?pipeline\(\s*\)\.parameters\.(\w+)", re.IGNORECASE)
_PIPELINE_RUNID_RE = re.compile(r"@?pipeline\(\s*\)\.RunId", re.IGNORECASE)
_PIPELINE_TRIGGER_TIME_RE = re.compile(r"@?pipeline\(\s*\)\.TriggerTime", re.IGNORECASE)
_PIPELINE_NAME_RE = re.compile(r"@?pipeline\(\s*\)\.Pipeline", re.IGNORECASE)
_PIPELINE_GROUPID_RE = re.compile(r"@?pipeline\(\s*\)\.GroupId", re.IGNORECASE)
_ACTIVITY_OUTPUT_RE = re.compile(
    r"@?activity\(\s*'([^']+)'\s*\)\.output(?:\.firstRow)?\.(\w+)",
    re.IGNORECASE,
)
_VARIABLES_RE = re.compile(r"@?variables\(\s*'([^']+)'\s*\)", re.IGNORECASE)
# Already-resolved DAB refs
_DAB_JOB_PARAM_RE = re.compile(r"\{\{job\.parameters\.(\w+)\}\}")
# Already-resolved {{input.<field>}} refs (from translate-time resolution)
_DAB_INPUT_FIELD_RE = re.compile(r"\{\{input\.(\w+)\}\}")

# Simple @concat('literal', ref, 'literal', ...) — used for resolution
_CONCAT_RE = re.compile(r"@?concat\((.+)\)$", re.IGNORECASE | re.DOTALL)

# ADF type-conversion wrappers: @string(expr), @int(expr), @bool(expr), etc.
# These are no-ops in DAB string parameter context — strip them.
_TYPE_CAST_RE = re.compile(
    r"^@?(string|int|float|bool|decimal|json|xml|base64|binary|uriComponent|"
    r"ticks|dataUri|dataUriToBinary|dataUriToString|uriComponentToString)\((.+)\)$",
    re.IGNORECASE | re.DOTALL,
)


def collect_inner_job_params(
    tasks: list[dict[str, Any]],
    *,
    raw_ir_tasks: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Scan task dicts for parameter references and return declarations + pass-through map.

    Args:
        tasks: The inner job's DAB task dicts (may be nested via condition_task).
        raw_ir_tasks: Optional raw IR dicts (before DAB conversion) to scan
            for references in fields that are consumed during conversion
            (e.g. WebActivity ``url``, ``body``).

    Returns:
        Tuple of:
        - ``parameters``: list of ``{"name": ..., "default": ...}`` dicts for
          the inner job definition.
        - ``job_parameters``: dict mapping param name → parent expression,
          suitable for the ``run_job_task.job_parameters`` block.  ``item``
          always maps to ``"{{input}}"``, pipeline/variable params map to
          ``"{{job.parameters.<name>}}"``.
    """
    param_names: set[str] = set()
    item_field_names: set[str] = set()

    _scan_tasks(tasks, param_names, item_field_names=item_field_names)

    # Also scan raw IR dicts for references in fields consumed during conversion
    if raw_ir_tasks:
        _scan_ir_tasks(raw_ir_tasks, param_names, item_field_names=item_field_names)

    # Build parameter declarations
    parameters: list[dict[str, Any]] = []
    for name in sorted(param_names):
        param: dict[str, Any] = {"name": name}
        if name != "item":
            param["default"] = ""
        parameters.append(param)

    # Build pass-through map from parent job.
    # - "item" (bare @item()) → {{input}}  (the full iteration value)
    # - item field names (@item().field) → {{input.<field>}}
    # - pipeline params / variables → {{job.parameters.<name>}}
    job_parameters: dict[str, str] = {}
    for name in sorted(param_names):
        if name == "item":
            job_parameters[name] = "{{input}}"
        elif name in item_field_names:
            job_parameters[name] = "{{input." + name + "}}"
        else:
            job_parameters[name] = "{{job.parameters." + name + "}}"

    return parameters, job_parameters


def _scan_tasks(
    tasks: list[dict[str, Any]],
    param_names: set[str],
    *,
    item_field_names: set[str] | None = None,
) -> None:
    """Recursively scan task dicts for ADF parameter references.

    Examines ``base_parameters``, ``job_parameters``, ``condition_task``
    operands, and nested branches.

    Args:
        tasks: List of task dicts to scan.
        param_names: Accumulator set of discovered parameter names.
        item_field_names: Optional accumulator for field names from item().field refs.
    """
    for task in tasks:
        # Scan notebook_task.base_parameters
        nb_task = task.get("notebook_task", {})
        params = nb_task.get("base_parameters", {})
        for value in params.values():
            _extract_refs(value, param_names, item_field_names=item_field_names)

        # Scan run_job_task.job_parameters
        rj_task = task.get("run_job_task", {})
        for value in rj_task.get("job_parameters", {}).values():
            _extract_refs(value, param_names, item_field_names=item_field_names)

        # Scan condition_task operands and recurse into branches
        ct = task.get("condition_task", {})
        if ct:
            _extract_refs(ct.get("left", ""), param_names, item_field_names=item_field_names)
            _extract_refs(ct.get("right", ""), param_names, item_field_names=item_field_names)
            _scan_tasks(ct.get("if_true", []), param_names, item_field_names=item_field_names)
            _scan_tasks(ct.get("if_false", []), param_names, item_field_names=item_field_names)

        # Recurse into for_each_task body
        fe = task.get("for_each_task", {})
        body = fe.get("task")
        if body:
            _scan_tasks([body], param_names, item_field_names=item_field_names)


def _scan_ir_tasks(
    ir_tasks: list[dict[str, Any]],
    param_names: set[str],
    *,
    item_field_names: set[str] | None = None,
) -> None:
    """Scan raw IR task dicts for parameter references in all fields.

    Catches references in fields that ``_scan_tasks`` can't see because they
    are consumed during DAB conversion — e.g. WebActivity ``url`` and ``body``,
    NotebookActivity ``base_parameters``, and nested control-flow structures.

    Args:
        ir_tasks: Raw serialised IR task dicts.
        param_names: Accumulator set of discovered parameter names.
        item_field_names: Optional accumulator for field names from item().field refs.
    """
    kw = {"item_field_names": item_field_names}
    for ir in ir_tasks:
        # WebActivity: url, body, headers
        _extract_refs(ir.get("url", ""), param_names, **kw)
        _extract_refs(ir.get("body"), param_names, **kw)
        if isinstance(ir.get("headers"), dict):
            for v in ir["headers"].values():
                _extract_refs(v, param_names, **kw)

        # NotebookActivity: base_parameters
        bp = ir.get("base_parameters")
        if isinstance(bp, dict):
            for v in bp.values():
                _extract_refs(v, param_names, **kw)

        # SwitchActivity: on_expression, cases, default_activities
        _extract_refs(ir.get("on_expression", ""), param_names, **kw)
        for case in ir.get("cases", []):
            _scan_ir_tasks(case.get("activities", []), param_names, **kw)
        _scan_ir_tasks(ir.get("default_activities", []), param_names, **kw)

        # IfConditionActivity: expression operands, branches
        _extract_refs(ir.get("op", ""), param_names, **kw)
        _extract_refs(ir.get("left", ""), param_names, **kw)
        _extract_refs(ir.get("right", ""), param_names, **kw)
        _scan_ir_tasks(ir.get("if_true_activities", []), param_names, **kw)
        _scan_ir_tasks(ir.get("if_false_activities", []), param_names, **kw)

        # ForEachActivity: inner_activities
        _scan_ir_tasks(ir.get("inner_activities", []), param_names, **kw)


def _extract_refs(
    value: Any,
    param_names: set[str],
    *,
    item_field_names: set[str] | None = None,
) -> None:
    """Extract parameter names from a single value that may be a string or ADF expression dict."""
    text = ""
    if isinstance(value, str):
        text = value
    elif isinstance(value, dict):
        text = value.get("value", "") if value.get("type") == "Expression" else ""

    if not text:
        return

    # @pipeline().parameters.X
    for m in _PIPELINE_PARAM_RE.finditer(text):
        param_names.add(m.group(1))

    # @variables('Y')
    for m in _VARIABLES_RE.finditer(text):
        param_names.add(m.group(1))

    # @item().field → extract the field name as its own parameter
    for m in _ITEM_FIELD_RE.finditer(text):
        field_name = m.group(1)
        param_names.add(field_name)
        if item_field_names is not None:
            item_field_names.add(field_name)

    # @item() (bare, no field access) → "item"
    # Only add if there's a bare item() that isn't part of item().field
    bare_text = _ITEM_FIELD_RE.sub("", text)  # strip field refs first
    if _ITEM_BARE_RE.search(bare_text):
        param_names.add("item")

    # Already-resolved {{input.field}} refs (from translate-time resolution)
    for m in _DAB_INPUT_FIELD_RE.finditer(text):
        field_name = m.group(1)
        param_names.add(field_name)
        if item_field_names is not None:
            item_field_names.add(field_name)

    # Already-resolved DAB refs
    for m in _DAB_JOB_PARAM_RE.finditer(text):
        param_names.add(m.group(1))


# ---------------------------------------------------------------------------
# Expression normalization
# ---------------------------------------------------------------------------


def normalize_inner_task_params(tasks: list[dict[str, Any]]) -> None:
    """Normalise ADF expressions in task dicts for an inner job context.

    Rewrites raw ADF expression dicts in ``base_parameters`` and
    ``condition_task`` operands to ``{{job.parameters.*}}`` dynamic value
    references.  Resolves simple ``@concat(...)`` expressions to string
    concatenation.

    Mutates the task dicts in place.

    Args:
        tasks: The inner job's task dicts.
    """
    for task in tasks:
        nb_task = task.get("notebook_task")
        if nb_task and "base_parameters" in nb_task:
            nb_task["base_parameters"] = {k: _normalize_value(v) for k, v in nb_task["base_parameters"].items()}

        # Normalize condition_task operands
        ct = task.get("condition_task")
        if ct:
            if "left" in ct:
                ct["left"] = _normalize_value(ct["left"])
            if "right" in ct:
                ct["right"] = _normalize_value(ct["right"])
            normalize_inner_task_params(ct.get("if_true", []))
            normalize_inner_task_params(ct.get("if_false", []))

        # Recurse into for_each_task body
        fe = task.get("for_each_task", {})
        body = fe.get("task")
        if body:
            normalize_inner_task_params([body])


def _normalize_value(value: Any) -> str:
    """Normalize a single parameter value from ADF expression to DAB reference.

    Handles:
    - ``{type: Expression, value: ...}`` dicts → unwrapped and processed
    - ``@item().field`` → ``{{job.parameters.item}}``
    - ``@pipeline().parameters.X`` → ``{{job.parameters.X}}``
    - ``@variables('Y')`` → ``{{job.parameters.Y}}``
    - ``@concat('a', ref, 'b')`` → resolved to a plain string with embedded refs

    Args:
        value: A string or ``{type: Expression, value: ...}`` dict.

    Returns:
        A normalized string with ``{{job.parameters.*}}`` references.
    """
    if isinstance(value, dict) and value.get("type") == "Expression":
        text = value.get("value", "")
    elif isinstance(value, str):
        text = value
    else:
        return str(value)

    # Strip ADF type-conversion wrappers (@string, @int, etc.) first so the
    # inner expression can be resolved by subsequent steps.
    m = _TYPE_CAST_RE.match(text.strip())
    if m:
        text = m.group(2)
        # Re-add @ prefix if the inner expression is a function call or
        # reference that needs it for pattern matching.
        if not text.startswith("@") and not text.startswith("{{"):
            text = "@" + text

    # Resolve references inside the expression
    text = _replace_refs(text)

    # Then try to resolve @concat(...) to a plain string
    text = _resolve_concat(text)

    return text


def _replace_refs(text: str) -> str:
    """Replace ADF references with {{job.parameters.*}} or {{job.*}} refs.

    Order matters: field-access patterns must fire before bare patterns
    to avoid partial matches.

    Args:
        text: Expression text potentially containing ADF references.

    Returns:
        Text with references replaced.
    """
    # Already-resolved {{input.field}} (from translate-time) → {{job.parameters.<field>}}
    # Must fire before ADF-style patterns to handle pre-resolved refs.
    text = _DAB_INPUT_FIELD_RE.sub(r"{{job.parameters.\1}}", text)
    # @item().field → {{job.parameters.<field>}}
    text = _ITEM_FIELD_RE.sub(r"{{job.parameters.\1}}", text)
    # @item()  (bare, no field access) → {{job.parameters.item}}
    text = _ITEM_BARE_RE.sub("{{job.parameters.item}}", text)
    # @pipeline().parameters.X → {{job.parameters.X}}
    text = _PIPELINE_PARAM_RE.sub(r"{{job.parameters.\1}}", text)
    # @pipeline().RunId → {{job.run_id}}
    text = _PIPELINE_RUNID_RE.sub("{{job.run_id}}", text)
    # @pipeline().TriggerTime → {{job.start_time.iso_datetime}}
    text = _PIPELINE_TRIGGER_TIME_RE.sub("{{job.start_time.iso_datetime}}", text)
    # @pipeline().Pipeline → {{job.name}}
    text = _PIPELINE_NAME_RE.sub("{{job.name}}", text)
    # @pipeline().GroupId → {{job.run_id}}
    text = _PIPELINE_GROUPID_RE.sub("{{job.run_id}}", text)
    # @activity('Name').output[.firstRow].field → {{tasks.Name.values.field}}
    text = _ACTIVITY_OUTPUT_RE.sub(r"{{tasks.\1.values.\2}}", text)
    # @variables('Y') → {{job.parameters.Y}}
    text = _VARIABLES_RE.sub(r"{{job.parameters.\1}}", text)

    return text


def _resolve_concat(text: str) -> str:
    """Resolve ``@concat(arg1, arg2, ...)`` to a plain concatenated string.

    Only resolves when all arguments are string literals or already-resolved
    ``{{...}}`` references.  Returns the original text if the concat is too
    complex.

    Args:
        text: Expression that may be a @concat(...) call.

    Returns:
        Resolved string, or original text if not resolvable.
    """
    m = _CONCAT_RE.match(text.strip())
    if not m:
        return text

    args_str = m.group(1)
    parts = _split_concat_args(args_str)
    if parts is None:
        return text  # too complex to split

    resolved: list[str] = []
    for part in parts:
        part = part.strip()
        # String literal: 'value'
        if part.startswith("'") and part.endswith("'"):
            resolved.append(part[1:-1])
        # Already a {{...}} ref
        elif part.startswith("{{"):
            resolved.append(part)
        else:
            # Unrecognised argument — bail out
            return text

    return "".join(resolved)


def _split_concat_args(args_str: str) -> list[str] | None:
    """Split concat arguments respecting nested parentheses and quotes.

    Args:
        args_str: The argument string inside concat(...).

    Returns:
        List of argument strings, or None if parsing fails.
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
            if depth == 0:
                return None  # unbalanced
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)

    if current:
        parts.append("".join(current).strip())

    if depth != 0 or in_quote:
        return None

    return parts
