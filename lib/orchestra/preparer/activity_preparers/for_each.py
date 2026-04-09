"""Preparer for ForEachActivity -> for_each_task dict.

The ForEach task in Databricks jobs iterates over an array of values and runs
an inner task for each item.  The inner task receives the current item via the
``{{input}}`` dynamic value reference.

References:
- https://docs.databricks.com/aws/en/jobs/task-values
- https://docs.databricks.com/aws/en/jobs/task-values#reference-task-values
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields, prepare_activity

if TYPE_CHECKING:
    from orchestra.models.ir import ForEachActivity


# Regex to match ADF expression: @activity('TaskName').output.value (or .output.firstRow, etc.)
_ACTIVITY_OUTPUT_RE = re.compile(
    r"@activity\(\s*'([^']+)'\s*\)\.output(?:\.(\w+))?",
    re.IGNORECASE,
)


def _resolve_for_each_inputs(items_expression: str) -> str:
    """Convert an ADF items expression to a DAB dynamic value reference.

    ADF ``@activity('LookupTableList').output.value`` maps to the Databricks
    task values reference ``{{tasks.LookupTableList.values.result}}``.

    Args:
        items_expression: The raw ADF expression for ForEach items.

    Returns:
        A DAB dynamic value reference string, or the original expression if
        it cannot be resolved.
    """
    m = _ACTIVITY_OUTPUT_RE.match(items_expression)
    if m:
        task_name = m.group(1)
        # Sanitize the task name to match the task_key
        task_key = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)
        task_key = re.sub(r"_+", "_", task_key).strip("_") or "unnamed"
        return "{{" + f"tasks.{task_key}.values.result" + "}}"
    return items_expression


def _inject_input_parameter(inner_task: dict) -> dict:
    """Add ``{{input}}`` as a base_parameter on the inner task.

    For notebook tasks, each item from the ForEach array is passed as the
    ``item`` widget parameter using the ``{{input}}`` dynamic value reference.
    The notebook can then read it with ``dbutils.widgets.get("item")``.

    Args:
        inner_task: The prepared inner task dict.

    Returns:
        The task dict with ``item`` parameter injected.
    """
    if "notebook_task" in inner_task:
        params = inner_task["notebook_task"].setdefault("base_parameters", {})
        params["item"] = "{{input}}"
    return inner_task


def prepare(activity: ForEachActivity) -> PreparedActivity:
    """Convert a ForEachActivity into a DAB for_each_task definition.

    Recursively prepares the child activity and wraps it in the for_each_task
    structure.  The ``inputs`` field references the upstream task's values via
    ``{{tasks.<key>.values.result}}``, and the inner task receives each item
    as the ``item`` widget parameter via ``{{input}}``.

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
        inner_task = _inject_input_parameter(inner_prepared.task)

    concurrency = activity.concurrency if activity.concurrency is not None else 20
    inputs = _resolve_for_each_inputs(activity.items_expression)

    task["for_each_task"] = {
        "inputs": inputs,
        "task": inner_task,
        "concurrency": concurrency,
    }

    notebooks = inner_prepared.notebooks if inner_prepared else []
    secrets = inner_prepared.secrets if inner_prepared else []
    setup_tasks = inner_prepared.setup_tasks if inner_prepared else []

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets, setup_tasks=setup_tasks)
