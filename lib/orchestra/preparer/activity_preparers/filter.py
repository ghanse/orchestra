"""Preparer for FilterActivity -> notebook_task with generated filter notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression
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
    notebook_name = f"{activity.task_key}.py"
    notebook_path = f"notebooks/{notebook_name}"
    content = generate_filter_notebook(activity)

    task = _build_common_task_fields(activity)
    params: dict[str, str] = {}

    # items_expression: typically a DAB ref like {{tasks.X.values.result}}
    ctx = TranslationContext()
    items_result = resolve_expression(activity.items_expression, ctx)
    if items_result and items_result.kind in ("dab_ref", "literal"):
        params["items_expression"] = items_result.value
    else:
        params["items_expression"] = activity.items_expression

    # condition_expression: typically involves item().field which is notebook_code
    # — do NOT put in parameters, the notebook embeds it directly
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
        "base_parameters": params,
    }

    notebooks = [
        DabNotebook(
            relative_path=notebook_path,
            content=content,
        )
    ]

    return PreparedActivity(task=task, notebooks=notebooks)
