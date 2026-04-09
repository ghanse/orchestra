"""Preparer for IfConditionActivity -> condition_task dict."""

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

    # Build condition expression from the op/left/right fields
    condition_expr = f"{activity.left} {activity.op} {activity.right}"

    task["condition_task"] = {
        "condition_expression": condition_expr,
        "if_true": if_true_tasks,
        "if_false": if_false_tasks,
    }

    return PreparedActivity(
        task=task,
        notebooks=all_notebooks,
        secrets=all_secrets,
        setup_tasks=all_setup_tasks,
    )
