"""Preparer for FilterActivity -> notebook_task with generated filter notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression
from orchestra.preparer.activity_preparers._helpers import build_notebook_activity_task
from orchestra.preparer.activity_preparers._naming import notebook_filename
from orchestra.preparer.code_generator import generate_filter_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from orchestra.models.ir import FilterActivity


def prepare(activity: FilterActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a FilterActivity into a notebook_task that filters an array.

    The condition expression is embedded in the notebook body (it typically
    involves ``item().field`` which requires runtime JSON parsing); only the
    items expression flows through ``base_parameters``.
    """
    items_result = resolve_expression(activity.items_expression, TranslationContext())
    if items_result is not None and items_result.kind in ("dab_ref", "literal"):
        items_value = items_result.value
    else:
        items_value = activity.items_expression

    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_filter_notebook(activity),
        base_parameters={"items_expression": items_value},
    )
    return PreparedActivity(task=task, notebooks=notebooks)
