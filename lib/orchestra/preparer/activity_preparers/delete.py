"""Preparer for DeleteActivity -> notebook_task with generated delete notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
from orchestra.preparer.code_generator import generate_delete_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import DeleteActivity


def _resolve_param_value(value: str) -> str:
    """Resolve an ADF expression parameter value to a DAB ref.

    Args:
        value: A parameter value string that may contain ADF expressions.

    Returns:
        Resolved value string.
    """
    ctx = TranslationContext()

    # Try @{...} interpolation
    if "@{" in value:
        return resolve_interpolated_string(value, ctx)

    # Try @expr style
    if value.startswith("@"):
        result = resolve_expression(value, ctx)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value

    return value


def prepare(activity: DeleteActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a DeleteActivity into a notebook_task with a generated delete notebook.

    Args:
        activity: The translated delete activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task and generated notebook.
    """
    notebook_name = f"{activity.task_key}.py"
    notebook_path = f"notebooks/{notebook_name}"
    content = generate_delete_notebook(activity)

    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
        "base_parameters": {
            "dataset_name": _resolve_param_value(activity.dataset_name),
            "recursive": str(activity.recursive).lower(),
        },
    }
    if activity.folder_path:
        task["notebook_task"]["base_parameters"]["folder_path"] = _resolve_param_value(activity.folder_path)

    notebooks = [
        DabNotebook(
            relative_path=notebook_path,
            content=content,
        )
    ]

    return PreparedActivity(task=task, notebooks=notebooks)
