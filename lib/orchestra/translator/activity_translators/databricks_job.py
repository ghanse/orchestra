"""Translate ADF DatabricksJob activities to Databricks RunJobActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, RunJobActivity, TranslationContext
from orchestra.translator.activity_translators._resolve import resolve_dict_values, resolve_field


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

    job_name_raw = tp.get("jobName") or tp.get("jobId")
    job_name = resolve_field(job_name_raw, context) if job_name_raw else activity.name
    existing_job_id = tp.get("jobId")
    job_parameters = resolve_dict_values(tp.get("jobParameters") or tp.get("baseParameters"), context) or None

    return RunJobActivity(
        **base_kwargs,
        job_name=job_name,
        existing_job_id=str(existing_job_id) if existing_job_id else None,
        job_parameters=job_parameters,
    )
