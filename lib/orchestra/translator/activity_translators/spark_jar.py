"""Translate ADF DatabricksSparkJar activities to Databricks SparkJarActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, SparkJarActivity, TranslationContext


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

    main_class_name = tp.get("mainClassName", "")
    parameters = tp.get("parameters") or []
    libraries = tp.get("libraries") or []

    return SparkJarActivity(
        **base_kwargs,
        main_class_name=main_class_name,
        parameters=parameters,
        libraries=libraries,
    )
