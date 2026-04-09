"""Preparer for FilterActivity -> notebook_task with generated filter notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.preparer.code_generator import generate_filter_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import FilterActivity


def prepare(activity: FilterActivity) -> PreparedActivity:
    """Convert a FilterActivity into a notebook_task that filters an array.

    The generated notebook reads the input array, applies the filter condition,
    and writes the filtered result as a task value for downstream activities.

    Args:
        activity: The translated filter activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task and generated notebook.
    """
    notebook_name = f"{activity.task_key}.py"
    notebook_path = f"notebooks/{notebook_name}"
    content = generate_filter_notebook(activity)

    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
        "base_parameters": {
            "items_expression": activity.items_expression,
            "condition_expression": activity.condition_expression,
        },
    }

    notebooks = [
        DabNotebook(
            relative_path=notebook_path,
            content=content,
        )
    ]

    return PreparedActivity(task=task, notebooks=notebooks)
