"""Preparer for DeleteActivity -> notebook_task with generated delete notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.preparer.activity_preparers._helpers import (
    build_notebook_task_artifacts,
    resolve_param_value,
)
from orchestra.preparer.activity_preparers._naming import notebook_filename
from orchestra.preparer.code_generator import generate_delete_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import DeleteActivity


def prepare(activity: DeleteActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a DeleteActivity into a notebook_task with a generated delete notebook.

    Args:
        activity: The translated delete activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task and generated notebook.
    """
    notebook_relative_path = f"notebooks/{notebook_filename(activity.task_key, activity.name)}"
    content = generate_delete_notebook(activity)

    base_parameters = {
        "dataset_name": resolve_param_value(activity.dataset_name),
        "recursive": str(activity.recursive).lower(),
    }
    if activity.folder_path:
        base_parameters["folder_path"] = resolve_param_value(activity.folder_path)

    task = _build_common_task_fields(activity)
    task["notebook_task"], notebooks = build_notebook_task_artifacts(
        notebook_relative_path=notebook_relative_path,
        notebook_content=content,
        base_parameters=base_parameters,
    )

    return PreparedActivity(task=task, notebooks=notebooks)
