"""Preparer for IfConditionActivity -> condition_task dict.

Emits the structured ``condition_task`` format with ``op``, ``left``, and
``right`` fields as required by Databricks Jobs:

.. code-block:: yaml

    condition_task:
      op: GREATER_THAN
      left: "{{tasks.CheckDataExists.values.cnt}}"
      right: "0"

References:
- https://docs.databricks.com/aws/en/jobs/conditional-tasks
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields, prepare_activity

if TYPE_CHECKING:
    from orchestra.models.ir import IfConditionActivity


def prepare(activity: IfConditionActivity, *, scope: str = "") -> PreparedActivity:
    """Convert an IfConditionActivity into a DAB condition_task definition.

    Recursively prepares both the true-branch and false-branch activities.

    Args:
        activity: The translated if-condition activity from the IR.
        scope: Secret scope name passed through to child preparers.

    Returns:
        A PreparedActivity with the condition_task and aggregated artifacts
        from both branches.
    """
    task = _build_common_task_fields(activity)

    all_notebooks = []
    all_secrets = []
    all_setup_tasks = []

    if_true_tasks = []
    for child in activity.if_true_activities:
        prepared = prepare_activity(child, scope=scope)
        if_true_tasks.append(prepared.task)
        all_notebooks.extend(prepared.notebooks)
        all_secrets.extend(prepared.secrets)
        all_setup_tasks.extend(prepared.setup_tasks)

    if_false_tasks = []
    for child in activity.if_false_activities:
        prepared = prepare_activity(child, scope=scope)
        if_false_tasks.append(prepared.task)
        all_notebooks.extend(prepared.notebooks)
        all_secrets.extend(prepared.secrets)
        all_setup_tasks.extend(prepared.setup_tasks)

    task["condition_task"] = {
        "op": activity.op,
        "left": activity.left,
        "right": activity.right,
    }
    if if_true_tasks:
        task["condition_task"]["if_true"] = if_true_tasks
    if if_false_tasks:
        task["condition_task"]["if_false"] = if_false_tasks

    return PreparedActivity(
        task=task,
        notebooks=all_notebooks,
        secrets=all_secrets,
        setup_tasks=all_setup_tasks,
    )
