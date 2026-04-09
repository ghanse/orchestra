"""Translate ADF ExecutePipeline activities to Databricks ExecutePipelineActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, ExecutePipelineActivity, TranslationContext


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate an ExecutePipeline activity.

    Extracts the child pipeline reference name, parameters, and wait-on-completion flag.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing pipelines.

    Returns:
        An :class:`ExecutePipelineActivity` IR node.
    """
    tp = activity.type_properties or {}

    # Pipeline reference
    pipeline_ref = tp.get("pipeline", {})
    pipeline_name = pipeline_ref.get("referenceName", "") if isinstance(pipeline_ref, dict) else str(pipeline_ref)

    parameters = tp.get("parameters") or {}
    wait_on_completion = tp.get("waitOnCompletion", True)

    return ExecutePipelineActivity(
        **base_kwargs,
        pipeline_name=pipeline_name,
        parameters=parameters,
        wait_on_completion=wait_on_completion,
    )
