"""Preparer for SparkJarActivity -> spark_jar_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import SparkJarActivity


def _jar_placeholder(libraries: list[dict] | None, activity_name: str) -> str:
    """Generate a placeholder file with download instructions for JAR libraries.

    Args:
        libraries: Library descriptors from the SparkJarActivity.
        activity_name: The ADF activity name.

    Returns:
        Placeholder content as a string.
    """
    lib_lines = ""
    if libraries:
        for lib in libraries:
            for key, path in lib.items():
                lib_lines += f"#   {key}: {path}\n"
    return (
        f"# Placeholder for JARs referenced by activity: {activity_name}\n"
        "#\n"
        "# Download the following libraries and place them in this directory:\n"
        f"{lib_lines}"
        "#\n"
        "# Use the Databricks CLI to upload JARs:\n"
        "#   databricks fs cp <local_jar> dbfs:/FileStore/jars/<jar_name>\n"
    )


def prepare(activity: SparkJarActivity) -> PreparedActivity:
    """Convert a SparkJarActivity into a DAB spark_jar_task definition.

    Rewrites JAR library paths to bundle-relative paths and creates
    placeholder files with download instructions.

    Args:
        activity: The translated Spark JAR activity from the IR.

    Returns:
        A PreparedActivity containing the spark_jar_task dict and placeholder files.
    """
    task = _build_common_task_fields(activity)

    # Rewrite library paths to bundle-relative and try downloading JARs
    from orchestra.preparer.workspace_downloader import download_dbfs_file

    rewritten_libraries: list[dict] = []
    notebooks: list[DabNotebook] = []
    downloaded_any = False
    if activity.libraries:
        for lib in activity.libraries:
            rewritten_lib = {}
            for key, path in lib.items():
                if isinstance(path, str) and ("dbfs:" in path or "/" in path):
                    filename = path.rsplit("/", 1)[-1] if "/" in path else path
                    rewritten_lib[key] = f"../lib/{filename}"
                    # Try to download the JAR from DBFS
                    if key == "jar":
                        jar_content = download_dbfs_file(path)
                        if jar_content is not None:
                            downloaded_any = True
                            notebooks.append(
                                DabNotebook(
                                    relative_path=f"lib/{filename}",
                                    binary_content=jar_content,
                                )
                            )
                else:
                    rewritten_lib[key] = path
            rewritten_libraries.append(rewritten_lib)

        # Create a placeholder readme if we didn't download all JARs
        if not downloaded_any:
            placeholder_content = _jar_placeholder(activity.libraries, activity.name)
            notebooks.append(
                DabNotebook(
                    relative_path=f"lib/{activity.task_key}_README.txt",
                    content=placeholder_content,
                    language="python",
                )
            )

    task["spark_jar_task"] = {
        "main_class_name": activity.main_class_name,
    }
    if activity.parameters:
        task["spark_jar_task"]["parameters"] = list(activity.parameters)
    if rewritten_libraries:
        task["libraries"] = rewritten_libraries
    elif activity.libraries:
        task["libraries"] = list(activity.libraries)

    return PreparedActivity(task=task, notebooks=notebooks)
