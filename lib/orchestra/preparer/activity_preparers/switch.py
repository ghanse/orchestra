"""Preparer for SwitchActivity -> chained condition_tasks + flattened branches.

Each case becomes a ``condition_task`` that tests equality between the switch
expression and the case value.  Condition tasks are chained via
``depends_on[...].outcome: "false"`` so the next case only evaluates when the
prior case did not match.  Case-branch children are emitted as sibling tasks
that depend on the case's condition with ``outcome: "true"``.  The default
branch hangs off the last case's ``outcome: "false"``.

References:
- https://docs.databricks.com/aws/en/jobs/if-else
- https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
from orchestra.preparer.activity_preparers.if_condition import _inject_outcome_dependency
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields, prepare_activity

if TYPE_CHECKING:
    from orchestra.models.ir import SwitchActivity


def _sanitize_key(value: str) -> str:
    """Sanitize a case value for use in a task key.

    Args:
        value: Raw case value string.

    Returns:
        Alphanumeric + underscore string safe for task keys.
    """
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized or "empty"


def _resolve_on_expression(on_expr: str) -> str:
    """Resolve the switch ``on`` expression to a DAB dynamic value ref.

    Args:
        on_expr: Raw ADF on-expression string.

    Returns:
        Resolved DAB ref string, or the original expression if unresolvable.
    """
    context = TranslationContext()
    if "@{" in on_expr:
        return resolve_interpolated_string(on_expr, context)
    if on_expr.startswith("@"):
        result = resolve_expression(on_expr, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value
    return on_expr


def prepare(activity: SwitchActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a SwitchActivity into a chain of flattened condition tasks.

    Produces one condition task per case, linked together by ``outcome:
    "false"`` dependencies so cases are evaluated in order.  Each case's
    children are returned as sibling tasks gated on that case's
    ``outcome: "true"``; the default branch is gated on the last case's
    ``outcome: "false"``.

    Args:
        activity: The translated switch activity from the IR.
        scope: Secret scope name passed through to child preparers.

    Returns:
        A PreparedActivity with the first condition as ``task`` and all
        subsequent conditions + case bodies as ``extra_tasks``.
    """
    all_notebooks = []
    all_secrets = []
    all_setup_tasks = []
    all_inner_workflows = []
    extra_tasks: list[dict[str, Any]] = []

    resolved_expr = _resolve_on_expression(activity.on_expression)

    # Degenerate case: no cases at all → fire the default (if any) unconditionally.
    if not activity.cases:
        task = _build_common_task_fields(activity)
        task["condition_task"] = {"op": "EQUAL_TO", "left": "true", "right": "true"}

        default_tasks: list[dict[str, Any]] = []
        for child in activity.default_activities:
            prepared = prepare_activity(child, scope=scope)
            default_tasks.append(prepared.task)
            default_tasks.extend(prepared.extra_tasks)
            all_notebooks.extend(prepared.notebooks)
            all_secrets.extend(prepared.secrets)
            all_setup_tasks.extend(prepared.setup_tasks)
            all_inner_workflows.extend(prepared.inner_workflows)
        _inject_outcome_dependency(default_tasks, activity.task_key, "true")

        return PreparedActivity(
            task=task,
            extra_tasks=default_tasks,
            notebooks=all_notebooks,
            secrets=all_secrets,
            setup_tasks=all_setup_tasks,
            inner_workflows=all_inner_workflows,
        )

    # Build one condition task per case, chained via outcome="false" deps.
    # Every case (including the first) is named ``<activity>_case_<value>``
    # for clarity in the rendered job graph.  The first case carries the
    # original Switch's depends_on edges; ``prepare_workflow`` rewrites any
    # downstream task that referenced the bare ``<activity>`` key to point
    # at the renamed first case.
    case_keys: list[str] = []
    for index, case in enumerate(activity.cases):
        is_first = index == 0
        case_key = f"{activity.task_key}_case_{_sanitize_key(case.value)}"
        case_keys.append(case_key)

        condition_task: dict[str, Any] = {
            "task_key": case_key,
            "condition_task": {"op": "EQUAL_TO", "left": resolved_expr, "right": case.value},
        }
        if is_first:
            # First condition takes the original activity's depends_on, timeouts,
            # retries, etc. — same baseline as before, just under the new key.
            base = _build_common_task_fields(activity)
            base.pop("task_key", None)
            condition_task.update(base)
        else:
            # Subsequent conditions fire when the prior case did not match.
            condition_task["depends_on"] = [{"task_key": case_keys[index - 1], "outcome": "false"}]

        case_branch_tasks: list[dict[str, Any]] = []
        for child in case.activities:
            prepared = prepare_activity(child, scope=scope)
            case_branch_tasks.append(prepared.task)
            case_branch_tasks.extend(prepared.extra_tasks)
            all_notebooks.extend(prepared.notebooks)
            all_secrets.extend(prepared.secrets)
            all_setup_tasks.extend(prepared.setup_tasks)
            all_inner_workflows.extend(prepared.inner_workflows)
        _inject_outcome_dependency(case_branch_tasks, case_key, "true")

        if is_first:
            first_condition_task = condition_task
        else:
            extra_tasks.append(condition_task)
        extra_tasks.extend(case_branch_tasks)

    # Default branch fires when the last case did not match.
    branch_default_tasks: list[dict[str, Any]] = []
    for child in activity.default_activities:
        prepared = prepare_activity(child, scope=scope)
        branch_default_tasks.append(prepared.task)
        branch_default_tasks.extend(prepared.extra_tasks)
        all_notebooks.extend(prepared.notebooks)
        all_secrets.extend(prepared.secrets)
        all_setup_tasks.extend(prepared.setup_tasks)
        all_inner_workflows.extend(prepared.inner_workflows)
    if branch_default_tasks:
        _inject_outcome_dependency(branch_default_tasks, case_keys[-1], "false")
        extra_tasks.extend(branch_default_tasks)

    # Tell prepare_workflow to remap any depends_on that referenced the
    # original Switch task_key onto the renamed first case.
    remap = {activity.task_key: case_keys[0]}

    return PreparedActivity(
        task=first_condition_task,
        extra_tasks=extra_tasks,
        notebooks=all_notebooks,
        secrets=all_secrets,
        setup_tasks=all_setup_tasks,
        inner_workflows=all_inner_workflows,
        task_key_remap=remap,
    )
