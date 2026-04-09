"""Preparer for AppendVariableActivity -> notebook_task with generated notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.preparer.code_generator import generate_append_variable_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import AppendVariableActivity


def prepare(activity: AppendVariableActivity, *, scope: str = "") -> PreparedActivity:
    """Convert an AppendVariableActivity into a notebook_task that appends to an array.

    The generated notebook reads the current array from a task value (or
    initialises an empty list), appends the new value, and writes the
    updated array back via ``dbutils.jobs.taskValues.set()``.

    Args:
        activity: The translated append-variable activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task and generated notebook.
    """
    notebook_name = f"{activity.task_key}.py"
    notebook_path = f"notebooks/{notebook_name}"
    content = generate_append_variable_notebook(activity)

    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
        "base_parameters": {
            "variable_name": activity.variable_name,
            "value": activity.append_value,
        },
    }

    notebooks = [
        DabNotebook(
            relative_path=notebook_path,
            content=content,
        )
    ]

    return PreparedActivity(task=task, notebooks=notebooks)
