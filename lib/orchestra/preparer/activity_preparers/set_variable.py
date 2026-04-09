"""Preparer for SetVariableActivity -> notebook_task with generated notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.preparer.code_generator import generate_set_variable_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import SetVariableActivity


def prepare(activity: SetVariableActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a SetVariableActivity into a notebook_task that sets a task value.

    Databricks jobs use ``dbutils.jobs.taskValues.set()`` as the equivalent
    of ADF pipeline variables.

    Args:
        activity: The translated set-variable activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task and generated notebook.
    """
    notebook_name = f"{activity.task_key}.py"
    notebook_path = f"notebooks/{notebook_name}"
    content = generate_set_variable_notebook(activity)

    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
        "base_parameters": {
            "variable_name": activity.variable_name,
            "value": activity.variable_value,
        },
    }

    notebooks = [
        DabNotebook(
            relative_path=notebook_path,
            content=content,
        )
    ]

    return PreparedActivity(task=task, notebooks=notebooks)
