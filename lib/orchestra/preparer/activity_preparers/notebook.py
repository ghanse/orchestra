"""Preparer for NotebookActivity -> notebook_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.parser.expression_parser import parse_expression_for_dab
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import NotebookActivity


def _notebook_placeholder(original_path: str, activity_name: str, filename: str) -> str:
    """Generate placeholder notebook content with export instructions.

    Args:
        original_path: The original workspace path from ADF.
        activity_name: The ADF activity name.
        filename: The filename for the placeholder notebook.

    Returns:
        Placeholder notebook content as a string.
    """
    return (
        "# Databricks notebook source\n"
        "# MAGIC %md\n"
        f"# MAGIC # {activity_name}\n"
        "# MAGIC\n"
        f"# MAGIC **Source workspace path**: `{original_path}`\n"
        "# MAGIC\n"
        f"# MAGIC This notebook was referenced by ADF pipeline activity `{activity_name}`.\n"
        "# MAGIC Replace this placeholder with the actual notebook content.\n"
        "# MAGIC\n"
        "# MAGIC Export from workspace:\n"
        "# MAGIC ```\n"
        f'# MAGIC databricks workspace export "{original_path}" --format SOURCE -o src/notebooks/{filename}\n'
        "# MAGIC ```\n"
        "\n# COMMAND ----------\n\n"
        "# TODO: Replace this placeholder with the exported notebook content\n"
        "raise NotImplementedError(\n"
        f'    f"Export notebook from workspace: {original_path}"\n'
        ")\n"
    )


def _resolve_base_parameters(params: dict[str, str]) -> dict[str, str]:
    """Resolve ADF expression dicts and map to DAB dynamic value references.

    Args:
        params: Raw base_parameters dict from the NotebookActivity.

    Returns:
        Resolved dict with DAB dynamic value references where possible.
    """
    resolved: dict[str, str] = {}
    for key, value in params.items():
        # Handle expression-type dicts: {"type": "Expression", "value": "@..."}
        if isinstance(value, dict):
            if value.get("type") == "Expression" and "value" in value:
                value = value["value"]
            else:
                resolved[key] = str(value)
                continue

        # Try to map ADF expression to DAB dynamic value reference
        if isinstance(value, str) and value.startswith("@"):
            dab_ref = parse_expression_for_dab(value)
            if dab_ref is not None:
                resolved[key] = dab_ref
                continue

        resolved[key] = str(value)
    return resolved


def prepare(activity: NotebookActivity) -> PreparedActivity:
    """Convert a NotebookActivity into a DAB notebook_task definition.

    Rewrites the notebook_path to a bundle-relative path and creates a
    placeholder notebook with export instructions.

    Args:
        activity: The translated notebook activity from the IR.

    Returns:
        A PreparedActivity containing the notebook_task dict and placeholder notebook.
    """
    sanitized = activity.task_key
    notebook_rel_path = f"notebooks/{sanitized}.py"

    # Generate placeholder notebook with export instructions
    content = _notebook_placeholder(activity.notebook_path, activity.name, f"{sanitized}.py")

    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_rel_path}",
    }
    if activity.base_parameters:
        task["notebook_task"]["base_parameters"] = _resolve_base_parameters(dict(activity.base_parameters))

    notebooks = [DabNotebook(relative_path=notebook_rel_path, content=content)]
    return PreparedActivity(task=task, notebooks=notebooks)
