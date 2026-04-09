"""Preparer for SwitchActivity -> chained condition_task dicts.

A Switch activity is converted into a chain of ``condition_task`` nodes:
one per case.  Each condition tests equality between the switch expression
and the case value.  The default branch fires when all prior conditions
fail.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.preparer.code_generator import _command_separator, _notebook_header
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields, prepare_activity

if TYPE_CHECKING:
    from orchestra.models.ir import SwitchActivity


def prepare(activity: SwitchActivity) -> PreparedActivity:
    """Convert a SwitchActivity into a chain of condition_task definitions.

    Each case becomes a ``condition_task`` that checks equality between
    the switch expression and the case value.  The default case is
    represented as the ``if_false`` branch of the final case condition,
    or as a standalone set of tasks when no cases exist.

    Recursively prepares child activities for every branch.

    Args:
        activity: The translated switch activity from the IR.

    Returns:
        A PreparedActivity with the condition_task and aggregated artifacts
        from all branches.
    """
    all_notebooks = []
    all_secrets = []
    all_setup_tasks = []

    # Prepare all case branches
    case_branches: list[dict] = []
    for case in activity.cases:
        case_tasks = []
        for child in case.activities:
            prepared = prepare_activity(child)
            case_tasks.append(prepared.task)
            all_notebooks.extend(prepared.notebooks)
            all_secrets.extend(prepared.secrets)
            all_setup_tasks.extend(prepared.setup_tasks)
        case_branches.append({
            "value": case.value,
            "tasks": case_tasks,
        })

    # Prepare default branch
    default_tasks = []
    for child in activity.default_activities:
        prepared = prepare_activity(child)
        default_tasks.append(prepared.task)
        all_notebooks.extend(prepared.notebooks)
        all_secrets.extend(prepared.secrets)
        all_setup_tasks.extend(prepared.setup_tasks)

    # Build the chained condition_task structure.
    # We nest conditions right-to-left: the last case's if_false is the
    # default branch; each prior case's if_false is the next condition.
    on_expr = activity.on_expression

    if not case_branches:
        # No cases — just run the default activities directly
        task = _build_common_task_fields(activity)
        if default_tasks:
            task["condition_task"] = {
                "condition_expression": "true",
                "if_true": default_tasks,
                "if_false": [],
            }
        else:
            task["condition_task"] = {
                "condition_expression": "true",
                "if_true": [],
                "if_false": [],
            }
        return PreparedActivity(
            task=task,
            notebooks=all_notebooks,
            secrets=all_secrets,
            setup_tasks=all_setup_tasks,
        )

    # Build from the last case backwards so we can nest if_false chains
    # Last case: if_false = default branch
    last = case_branches[-1]
    inner_condition = {
        "condition_expression": f'{on_expr} == "{last["value"]}"',
        "if_true": last["tasks"],
        "if_false": default_tasks,
    }

    # Walk backwards through remaining cases, wrapping each as if_false
    for branch in reversed(case_branches[:-1]):
        inner_condition = {
            "condition_expression": f'{on_expr} == "{branch["value"]}"',
            "if_true": branch["tasks"],
            "if_false": [{"task_key": f"{activity.task_key}__else", "condition_task": inner_condition}],
        }

    task = _build_common_task_fields(activity)
    task["condition_task"] = inner_condition

    return PreparedActivity(
        task=task,
        notebooks=all_notebooks,
        secrets=all_secrets,
        setup_tasks=all_setup_tasks,
    )
