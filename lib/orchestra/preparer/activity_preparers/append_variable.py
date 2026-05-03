"""Preparer for AppendVariableActivity -> notebook_task with generated notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.preparer.code_generator import generate_append_variable_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import AppendVariableActivity


def prepare(
    activity: AppendVariableActivity,
    *,
    scope: str = "",
    variable_task_keys: dict[str, str] | None = None,
) -> PreparedActivity:
    """Convert an AppendVariableActivity into a notebook_task that appends to an array.

    The generated notebook reads the current array from the task value set by
    the most-recent prior writer of this variable, appends, and writes back.

    When ``value_kind`` is ``"literal"`` or ``"dab_ref"`` the value is passed
    via ``base_parameters``.  When ``"notebook_code"`` the code is embedded
    directly in the notebook.

    Args:
        activity: The translated append-variable activity from the IR.
        scope: Secret scope name (unused here but accepted for dispatch).
        variable_task_keys: Map of pipeline variable name → task_key of the
            most recent writer, used to populate the ``source_task_key``
            widget so the generated notebook can read the current value.

    Returns:
        A PreparedActivity with the notebook_task and generated notebook.
    """
    from orchestra.preparer.activity_preparers._naming import notebook_filename
    notebook_name = notebook_filename(activity.task_key, activity.name)
    notebook_path = f"notebooks/{notebook_name}"
    content = generate_append_variable_notebook(activity)

    task = _build_common_task_fields(activity)

    base_parameters: dict[str, str] = {
        "variable_name": activity.variable_name,
        "source_task_key": (variable_task_keys or {}).get(activity.variable_name, ""),
    }

    if activity.value_kind in ("literal", "dab_ref"):
        base_parameters["value"] = activity.append_value

    # For notebook_code embeds, thread widget refs through base_parameters
    # so every `dbutils.widgets.get()` in the generated notebook resolves.
    for widget_name, dab_ref in activity.required_parameters.items():
        base_parameters.setdefault(widget_name, dab_ref)

    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
        "base_parameters": base_parameters,
    }

    notebooks = [
        DabNotebook(
            relative_path=notebook_path,
            content=content,
        )
    ]

    return PreparedActivity(task=task, notebooks=notebooks)
