"""Preparer for WaitActivity -> notebook_task with generated sleep notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.preparer.activity_preparers._helpers import build_notebook_task_artifacts
from orchestra.preparer.activity_preparers._naming import notebook_filename
from orchestra.preparer.code_generator import generate_wait_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import WaitActivity


def prepare(activity: WaitActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a WaitActivity into a notebook_task that sleeps for N seconds.

    Args:
        activity: The translated wait activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task and generated notebook.
    """
    notebook_relative_path = f"notebooks/{notebook_filename(activity.task_key, activity.name)}"
    content = generate_wait_notebook(activity)

    task = _build_common_task_fields(activity)
    task["notebook_task"], notebooks = build_notebook_task_artifacts(
        notebook_relative_path=notebook_relative_path,
        notebook_content=content,
        base_parameters={"wait_seconds": str(activity.wait_time_seconds)},
    )

    return PreparedActivity(task=task, notebooks=notebooks)
