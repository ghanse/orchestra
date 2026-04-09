"""Preparer for SparkPythonActivity -> spark_python_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import SparkPythonActivity


def prepare(activity: SparkPythonActivity) -> PreparedActivity:
    """Convert a SparkPythonActivity into a DAB spark_python_task definition.

    Args:
        activity: The translated Spark Python activity from the IR.

    Returns:
        A PreparedActivity containing the spark_python_task dict.
    """
    task = _build_common_task_fields(activity)
    task["spark_python_task"] = {
        "python_file": activity.python_file,
    }
    if activity.parameters:
        task["spark_python_task"]["parameters"] = list(activity.parameters)
    return PreparedActivity(task=task)
