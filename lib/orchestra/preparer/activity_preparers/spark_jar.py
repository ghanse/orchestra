"""Preparer for SparkJarActivity -> spark_jar_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import SparkJarActivity


def prepare(activity: SparkJarActivity) -> PreparedActivity:
    """Convert a SparkJarActivity into a DAB spark_jar_task definition.

    Args:
        activity: The translated Spark JAR activity from the IR.

    Returns:
        A PreparedActivity containing the spark_jar_task dict.
    """
    task = _build_common_task_fields(activity)
    task["spark_jar_task"] = {
        "main_class_name": activity.main_class_name,
    }
    if activity.parameters:
        task["spark_jar_task"]["parameters"] = list(activity.parameters)
    if activity.libraries:
        task["libraries"] = list(activity.libraries)
    return PreparedActivity(task=task)
