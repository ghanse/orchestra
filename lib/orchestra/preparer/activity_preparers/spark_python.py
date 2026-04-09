"""Preparer for SparkPythonActivity -> spark_python_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import SparkPythonActivity


def _python_placeholder(original_path: str, activity_name: str) -> str:
    """Generate placeholder Python script with download instructions.

    Args:
        original_path: The original DBFS/workspace path from ADF.
        activity_name: The ADF activity name.

    Returns:
        Placeholder Python content as a string.
    """
    return (
        f"# Placeholder for Python script referenced by activity: {activity_name}\n"
        f"# Original path: {original_path}\n"
        "#\n"
        "# Download and replace this file with the actual script:\n"
        f'#   databricks fs cp "{original_path}" src/scripts/{original_path.rsplit("/", 1)[-1]}\n'
        "#\n"
        f'raise NotImplementedError("Download script from: {original_path}")\n'
    )


def prepare(activity: SparkPythonActivity) -> PreparedActivity:
    """Convert a SparkPythonActivity into a DAB spark_python_task definition.

    Rewrites the python_file to a bundle-relative path and creates a
    placeholder script with download instructions.

    Args:
        activity: The translated Spark Python activity from the IR.

    Returns:
        A PreparedActivity containing the spark_python_task dict and placeholder file.
    """
    task = _build_common_task_fields(activity)

    # Rewrite python_file to bundle-relative path
    original_path = activity.python_file
    if original_path and ("dbfs:" in original_path or "/" in original_path):
        filename = original_path.rsplit("/", 1)[-1] if "/" in original_path else original_path
    else:
        filename = f"{activity.task_key}.py"
    script_rel_path = f"scripts/{filename}"

    # Try to download the actual Python file from DBFS
    from orchestra.preparer.workspace_downloader import download_dbfs_file

    downloaded = download_dbfs_file(original_path)
    if downloaded is not None:
        content = downloaded.decode("utf-8")
    else:
        content = _python_placeholder(original_path, activity.name)
    notebooks = [
        DabNotebook(
            relative_path=script_rel_path,
            content=content,
            language="python",
        )
    ]

    task["spark_python_task"] = {
        "python_file": f"../src/{script_rel_path}",
    }
    if activity.parameters:
        task["spark_python_task"]["parameters"] = list(activity.parameters)
    return PreparedActivity(task=task, notebooks=notebooks)
