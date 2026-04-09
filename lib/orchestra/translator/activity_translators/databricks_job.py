"""Translate ADF DatabricksJob activities to Databricks RunJobActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, RunJobActivity, TranslationContext


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate a DatabricksJob activity.

    Extracts the job name or job ID from the activity type properties.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`RunJobActivity` IR node.
    """
    tp = activity.type_properties or {}

    job_name = tp.get("jobName") or tp.get("jobId")
    existing_job_id = tp.get("jobId")
    job_parameters = tp.get("jobParameters") or tp.get("baseParameters")

    return RunJobActivity(
        **base_kwargs,
        job_name=str(job_name) if job_name else activity.name,
        existing_job_id=str(existing_job_id) if existing_job_id else None,
        job_parameters=job_parameters,
    )
