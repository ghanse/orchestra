"""Preparer for RunJobActivity -> run_job_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import RunJobActivity


def prepare(activity: RunJobActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a RunJobActivity into a DAB run_job_task definition.

    If the activity specifies an ``existing_job_id``, it is used directly.
    Otherwise the ``job_name`` is included as a resource reference.

    Args:
        activity: The translated run-job activity from the IR.

    Returns:
        A PreparedActivity containing the run_job_task dict.
    """
    task = _build_common_task_fields(activity)
    run_job: dict = {}

    if activity.existing_job_id:
        run_job["job_id"] = activity.existing_job_id
    elif activity.job_name:
        run_job["job_id"] = f"${{resources.jobs.{activity.job_name}.id}}"

    if activity.job_parameters:
        run_job["job_parameters"] = {k: str(v) for k, v in activity.job_parameters.items()}

    task["run_job_task"] = run_job
    return PreparedActivity(task=task)
