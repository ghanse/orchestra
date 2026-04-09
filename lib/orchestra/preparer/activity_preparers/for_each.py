"""Preparer for ForEachActivity -> for_each_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields, prepare_activity

if TYPE_CHECKING:
    from orchestra.models.ir import ForEachActivity


def prepare(activity: ForEachActivity) -> PreparedActivity:
    """Convert a ForEachActivity into a DAB for_each_task definition.

    Recursively prepares the child activity and wraps it in the for_each_task
    structure supported by Databricks jobs.

    Args:
        activity: The translated for-each activity from the IR.

    Returns:
        A PreparedActivity with the for_each_task, plus any notebooks and secrets
        from the child activity.
    """
    task = _build_common_task_fields(activity)

    inner_prepared: PreparedActivity | None = None
    inner_task: dict = {}
    if activity.child_activity is not None:
        inner_prepared = prepare_activity(activity.child_activity)
        inner_task = inner_prepared.task

    concurrency = activity.concurrency if activity.concurrency is not None else 20
    task["for_each_task"] = {
        "inputs": activity.items_expression,
        "task": inner_task,
        "concurrency": concurrency,
    }

    notebooks = inner_prepared.notebooks if inner_prepared else []
    secrets = inner_prepared.secrets if inner_prepared else []
    setup_tasks = inner_prepared.setup_tasks if inner_prepared else []

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets, setup_tasks=setup_tasks)
