"""Preparer for IfConditionActivity -> condition_task + branch sibling tasks.

Emits the DAB-legal ``condition_task`` (only ``op``, ``left``, ``right``) and
exposes branch children as *sibling* tasks via ``extra_tasks``.  Each branch
root gains ``depends_on: [{task_key: <condition>, outcome: "true"|"false"}]``;
non-root branch tasks keep their existing intra-branch dependencies so the
chain is still gated by the branch root.

References:
- https://docs.databricks.com/aws/en/jobs/if-else
- https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orchestra.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedArtifacts,
    build_common_task_fields,
    merge_prepared_artifacts,
    prepare_activity,
)

if TYPE_CHECKING:
    from orchestra.models.ir import IfConditionActivity


def inject_outcome_dependency(tasks: list[dict[str, Any]], condition_key: str, outcome: str) -> None:
    """Gate root tasks in a branch on the condition's outcome.

    A "root" task is one with no ``depends_on`` (or with ``depends_on`` that
    references only activities outside this branch — but ADF branches are
    self-contained in practice, so we detect roots by empty ``depends_on``).

    Args:
        tasks: Tasks in one branch (mutated in place).
        condition_key: Task key of the enclosing condition task.
        outcome: ``"true"`` or ``"false"``.
    """
    branch_keys = {task.get("task_key") for task in tasks}
    for task in tasks:
        deps = task.get("depends_on") or []
        refers_to_branch_sibling = any(dep.get("task_key") in branch_keys for dep in deps)
        if not deps or not refers_to_branch_sibling:
            # Root of branch — gate on the condition outcome.
            task["depends_on"] = [{"task_key": condition_key, "outcome": outcome}]


def prepare(activity: IfConditionActivity, *, scope: str = "") -> PreparedActivity:
    """Converts an IfConditionActivity into a condition_task + flattened branches.

    The returned ``PreparedActivity.task`` is the condition evaluator.  All
    branch children are returned via ``extra_tasks`` and will be flattened
    into the job's top-level task list by :func:`prepare_workflow`.

    Args:
        activity: The translated if-condition activity from the IR.
        scope: Secret scope name passed through to child preparers.

    Returns:
        A PreparedActivity with the condition_task, sibling branch tasks,
        and aggregated artifacts from both branches.
    """
    task = build_common_task_fields(activity)
    task["condition_task"] = {
        "op": activity.op,
        "left": activity.left,
        "right": activity.right,
    }

    artifacts = PreparedArtifacts()

    if_true_tasks: list[dict[str, Any]] = []
    for child in activity.if_true_activities:
        prepared = prepare_activity(child, scope=scope)
        if_true_tasks.append(prepared.task)
        if_true_tasks.extend(prepared.extra_tasks)
        artifacts = merge_prepared_artifacts(artifacts, prepared)
    inject_outcome_dependency(if_true_tasks, activity.task_key, "true")

    if_false_tasks: list[dict[str, Any]] = []
    for child in activity.if_false_activities:
        prepared = prepare_activity(child, scope=scope)
        if_false_tasks.append(prepared.task)
        if_false_tasks.extend(prepared.extra_tasks)
        artifacts = merge_prepared_artifacts(artifacts, prepared)
    inject_outcome_dependency(if_false_tasks, activity.task_key, "false")

    return PreparedActivity(
        task=task,
        extra_tasks=if_true_tasks + if_false_tasks,
        notebooks=list(artifacts.notebooks),
        secrets=list(artifacts.secrets),
        setup_tasks=list(artifacts.setup_tasks),
        inner_workflows=list(artifacts.inner_workflows),
    )
