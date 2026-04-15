"""Translate ADF DatabricksSparkJar activities to Databricks SparkJarActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, SparkJarActivity, TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
from orchestra.translator.activity_translators._resolve import resolve_field


def _resolve_parameter(param: Any, context: TranslationContext) -> str:
    """Resolve a single ADF parameter to a DAB value string.

    Handles expression dicts, ``@expr`` style, and ``@{expr}`` interpolation.

    Args:
        param: A parameter string or ``{"type": "Expression", "value": "..."}`` dict.
        context: Translation context for variable resolution.

    Returns:
        Resolved parameter string.
    """
    # Unwrap expression dicts
    if isinstance(param, dict) and param.get("type") == "Expression":
        return _resolve_parameter(param["value"], context)

    if not isinstance(param, str):
        return str(param)

    # Try @{...} interpolation first
    if "@{" in param:
        return resolve_interpolated_string(param, context)

    # Try @expr style
    if param.startswith("@"):
        result = resolve_expression(param, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value

    return param


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate a DatabricksSparkJar activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`SparkJarActivity` IR node.
    """
    tp = activity.type_properties or {}

    main_class_name = resolve_field(tp.get("mainClassName", ""), context)
    raw_parameters = tp.get("parameters") or []
    libraries = tp.get("libraries") or []

    # Resolve each parameter through the expression parser
    parameters = [_resolve_parameter(p, context) for p in raw_parameters]

    return SparkJarActivity(
        **base_kwargs,
        main_class_name=main_class_name,
        parameters=parameters,
        libraries=libraries,
    )
