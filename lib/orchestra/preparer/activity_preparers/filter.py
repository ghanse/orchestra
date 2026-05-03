"""Preparer for FilterActivity -> notebook_task with generated filter notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression
from orchestra.preparer.activity_preparers._helpers import build_notebook_task_artifacts
from orchestra.preparer.activity_preparers._naming import notebook_filename
from orchestra.preparer.code_generator import generate_filter_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import FilterActivity


def prepare(activity: FilterActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a FilterActivity into a notebook_task that filters an array.

    The generated notebook reads the input array, applies the filter condition,
    and writes the filtered result as a task value for downstream activities.

    Only DAB-resolvable values (literals and dynamic value references) are
    placed in ``base_parameters``.  The condition expression is always
    embedded in the notebook body since it typically involves ``item()``
    field access which requires runtime JSON parsing.

    Args:
        activity: The translated filter activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task and generated notebook.
    """
    notebook_relative_path = f"notebooks/{notebook_filename(activity.task_key, activity.name)}"
    content = generate_filter_notebook(activity)

    # items_expression: typically a DAB ref like ``{{tasks.X.values.result}}``.
    # condition_expression is intentionally NOT included -- it usually
    # involves ``item().field`` which requires runtime JSON parsing.
    items_result = resolve_expression(activity.items_expression, TranslationContext())
    if items_result and items_result.kind in ("dab_ref", "literal"):
        items_value = items_result.value
    else:
        items_value = activity.items_expression

    task = _build_common_task_fields(activity)
    task["notebook_task"], notebooks = build_notebook_task_artifacts(
        notebook_relative_path=notebook_relative_path,
        notebook_content=content,
        base_parameters={"items_expression": items_value},
    )

    return PreparedActivity(task=task, notebooks=notebooks)
