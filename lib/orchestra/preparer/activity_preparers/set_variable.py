"""Preparer for SetVariableActivity -> notebook_task with generated notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.preparer.activity_preparers._helpers import build_notebook_activity_task
from orchestra.preparer.activity_preparers._naming import notebook_filename
from orchestra.preparer.code_generator import generate_set_variable_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from orchestra.models.ir import SetVariableActivity


def prepare(activity: SetVariableActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a SetVariableActivity into a notebook_task that sets a task value.

    Databricks jobs use ``dbutils.jobs.taskValues.set()`` as the equivalent
    of ADF pipeline variables.

    When ``value_kind`` is ``"literal"`` or ``"dab_ref"``  the value is passed
    via ``base_parameters`` so the DAB runtime resolves dynamic references
    before the notebook runs.

    When ``value_kind`` is ``"notebook_code"`` the code is embedded directly
    in the generated notebook -- no Python code ever appears in
    ``base_parameters``.

    Args:
        activity: The translated set-variable activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task and generated notebook.
    """
    base_parameters: dict[str, str] = {"variable_name": activity.variable_name}
    if activity.value_kind in ("literal", "dab_ref"):
        # DAB resolves the ref at runtime before the notebook starts.
        base_parameters["value"] = activity.variable_value
    # ``notebook_code`` values are embedded directly in the body, but any
    # ``dbutils.widgets.get()`` calls still need their refs threaded through
    # base_parameters so DAB can supply the matching widget values.
    for widget_name, dab_ref in activity.required_parameters.items():
        base_parameters.setdefault(widget_name, dab_ref)

    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_set_variable_notebook(activity),
        base_parameters=base_parameters,
    )
    return PreparedActivity(task=task, notebooks=notebooks)
