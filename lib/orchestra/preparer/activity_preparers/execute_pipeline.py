"""Preparer for ExecutePipelineActivity -> run_job_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields
from orchestra.utils import normalize_task_key

if TYPE_CHECKING:
    from orchestra.models.ir import ExecutePipelineActivity


def prepare(activity: ExecutePipelineActivity) -> PreparedActivity:
    """Convert an ExecutePipelineActivity into a DAB run_job_task definition.

    The referenced pipeline is expected to exist as another job in the same
    bundle.  The ``run_job_task`` uses a resource reference so that DAB
    resolves the job ID at deploy time.

    Args:
        activity: The translated execute-pipeline activity from the IR.

    Returns:
        A PreparedActivity containing the run_job_task dict.
    """
    task = _build_common_task_fields(activity)
    resource_key = normalize_task_key(activity.pipeline_name)
    task["run_job_task"] = {
        "job_id": f"${{resources.jobs.{resource_key}.id}}",
    }
    if activity.parameters:
        task["run_job_task"]["job_parameters"] = {k: str(v) for k, v in activity.parameters.items()}
    return PreparedActivity(task=task)
