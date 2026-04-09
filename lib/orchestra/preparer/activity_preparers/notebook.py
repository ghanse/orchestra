"""Preparer for NotebookActivity -> notebook_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
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


def _resolve_base_parameters(
    params: dict[str, str],
    *,
    variable_task_keys: dict[str, str] | None = None,
) -> dict[str, str]:
    """Resolve ADF expression dicts and map to DAB dynamic value references.

    Uses the unified ``resolve_expression()`` to determine parameter values.
    Only ``literal`` and ``dab_ref`` kinds are placed in base_parameters.
    ``notebook_code`` kinds are excluded (they must go in the notebook body).

    Args:
        params: Raw base_parameters dict from the NotebookActivity.
        variable_task_keys: Mapping of variable names to the task keys that
            set them, used to resolve ``@variables('name')`` references.

    Returns:
        Resolved dict with DAB dynamic value references where possible.
    """
    context = TranslationContext()
    resolved: dict[str, str] = {}
    for key, value in params.items():
        result = resolve_expression(value, context, variable_task_keys=variable_task_keys)
        if result is not None:
            if result.kind in ("literal", "dab_ref"):
                resolved[key] = result.value
                continue
            # notebook_code: skip -- must not go in base_parameters
            continue

        # Fallback for unresolvable values: keep as literal string
        if isinstance(value, dict):
            if value.get("type") == "Expression" and "value" in value:
                # Keep the raw expression string for manual review
                resolved[key] = str(value["value"])
            else:
                resolved[key] = str(value)
        else:
            resolved[key] = str(value)
    return resolved


def _resolve_notebook_path(path: str) -> str:
    """Resolve any ADF expressions remaining in a notebook path.

    Args:
        path: Notebook workspace path that may contain ADF expressions.

    Returns:
        Resolved path string.
    """
    ctx = TranslationContext()
    if "@{" in path:
        return resolve_interpolated_string(path, ctx)
    if path.startswith("@"):
        result = resolve_expression(path, ctx)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value
    return path


def prepare(
    activity: NotebookActivity,
    *,
    scope: str = "",
    variable_task_keys: dict[str, str] | None = None,
) -> PreparedActivity:
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

    # Resolve any remaining ADF expressions in notebook_path
    resolved_path = _resolve_notebook_path(activity.notebook_path)

    # Try to download the actual notebook from the workspace
    from orchestra.preparer.workspace_downloader import download_notebook

    content = download_notebook(resolved_path)
    if content is None:
        # Fall back to placeholder with export instructions
        content = _notebook_placeholder(resolved_path, activity.name, f"{sanitized}.py")

    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_rel_path}",
    }
    if activity.base_parameters:
        task["notebook_task"]["base_parameters"] = _resolve_base_parameters(
            dict(activity.base_parameters),
            variable_task_keys=variable_task_keys,
        )

    notebooks = [DabNotebook(relative_path=notebook_rel_path, content=content)]
    return PreparedActivity(task=task, notebooks=notebooks)
