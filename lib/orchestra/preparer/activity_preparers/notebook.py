"""Preparer for NotebookActivity -> notebook_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import NotebookActivity


def prepare(activity: NotebookActivity) -> PreparedActivity:
    """Convert a NotebookActivity into a DAB notebook_task definition.

    Args:
        activity: The translated notebook activity from the IR.

    Returns:
        A PreparedActivity containing the notebook_task dict.
    """
    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": activity.notebook_path,
    }
    if activity.base_parameters:
        task["notebook_task"]["base_parameters"] = dict(activity.base_parameters)
    return PreparedActivity(task=task)
