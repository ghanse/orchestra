"""Translate ADF DatabricksSparkPython activities to Databricks SparkPythonActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, SparkPythonActivity, TranslationContext


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate a DatabricksSparkPython activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`SparkPythonActivity` IR node.
    """
    tp = activity.type_properties or {}

    python_file = tp.get("pythonFile", "")
    parameters = tp.get("parameters") or []

    return SparkPythonActivity(
        **base_kwargs,
        python_file=python_file,
        parameters=parameters,
    )
