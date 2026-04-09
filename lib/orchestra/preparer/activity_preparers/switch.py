"""Preparer for SwitchActivity -> chained condition_task dicts.

A Switch activity is converted into a chain of ``condition_task`` nodes:
one per case.  Each condition tests equality between the switch expression
and the case value using the structured ``op``/``left``/``right`` format.
The default branch fires when all prior conditions fail.

Each nested condition node is given a unique task key derived from the
case value (e.g., ``RouteByEnvironment__case_dev``) to prevent collisions.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
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

    Tries to resolve ``@variables('name')``, ``@pipeline().parameters.X``,
    and ``@{...}`` interpolation through the expression parser.

    Args:
        on_expr: Raw ADF on-expression string.

    Returns:
        Resolved DAB ref string, or the original expression if unresolvable.
    """
    # Build a minimal context -- variable resolution will use fallback
    context = TranslationContext()

    # Try @{...} interpolation
    if "@{" in on_expr:
        return resolve_interpolated_string(on_expr, context)

    # Try @expr style
    if on_expr.startswith("@"):
        result = resolve_expression(on_expr, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value

    return on_expr


def prepare(activity: SwitchActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a SwitchActivity into a chain of condition_task definitions.

    Each case becomes a ``condition_task`` that checks equality between
    the switch expression and the case value using the structured
    ``op``/``left``/``right`` format.  The default case is represented as
    the ``if_false`` branch of the final case condition, or as a standalone
    set of tasks when no cases exist.

    Task keys for nested conditions are made unique by appending the case
    value (e.g., ``RouteByEnvironment__case_staging``).

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

    # Resolve the on_expression to a DAB ref
    resolved_expr = _resolve_on_expression(activity.on_expression)

    # Prepare all case branches
    case_branches: list[dict] = []
    for case in activity.cases:
        case_tasks = []
        for child in case.activities:
            prepared = prepare_activity(child, scope=scope)
            case_tasks.append(prepared.task)
            all_notebooks.extend(prepared.notebooks)
            all_secrets.extend(prepared.secrets)
            all_setup_tasks.extend(prepared.setup_tasks)
        case_branches.append(
            {
                "value": case.value,
                "tasks": case_tasks,
            }
        )

    # Prepare default branch
    default_tasks = []
    for child in activity.default_activities:
        prepared = prepare_activity(child, scope=scope)
        default_tasks.append(prepared.task)
        all_notebooks.extend(prepared.notebooks)
        all_secrets.extend(prepared.secrets)
        all_setup_tasks.extend(prepared.setup_tasks)

    # Build the chained condition_task structure.
    # We nest conditions right-to-left: the last case's if_false is the
    # default branch; each prior case's if_false is the next condition.

    if not case_branches:
        # No cases -- just run the default activities directly
        task = _build_common_task_fields(activity)
        if default_tasks:
            task["condition_task"] = {
                "op": "EQUAL_TO",
                "left": "true",
                "right": "true",
                "if_true": default_tasks,
                "if_false": [],
            }
        else:
            task["condition_task"] = {
                "op": "EQUAL_TO",
                "left": "true",
                "right": "true",
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
        "op": "EQUAL_TO",
        "left": resolved_expr,
        "right": last["value"],
        "if_true": last["tasks"],
        "if_false": default_tasks,
    }
    # Track which case value the current inner_condition is checking
    inner_case_value = last["value"]

    # Walk backwards through remaining cases, wrapping each as if_false
    for branch in reversed(case_branches[:-1]):
        # The task_key names the nested condition (what's inside if_false)
        case_key = f"{activity.task_key}__case_{_sanitize_key(inner_case_value)}"
        inner_condition = {
            "op": "EQUAL_TO",
            "left": resolved_expr,
            "right": branch["value"],
            "if_true": branch["tasks"],
            "if_false": [{"task_key": case_key, "condition_task": inner_condition}],
        }
        inner_case_value = branch["value"]

    task = _build_common_task_fields(activity)
    task["condition_task"] = inner_condition

    return PreparedActivity(
        task=task,
        notebooks=all_notebooks,
        secrets=all_secrets,
        setup_tasks=all_setup_tasks,
    )
