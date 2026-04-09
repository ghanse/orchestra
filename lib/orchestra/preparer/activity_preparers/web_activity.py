"""Preparer for WebActivity -> notebook_task with generated HTTP notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook, SecretInstruction
from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
from orchestra.preparer.code_generator import generate_web_activity_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import WebActivity


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


def prepare(activity: WebActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a WebActivity into a notebook_task with a generated HTTP notebook.

    Args:
        activity: The translated web activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task, generated notebook, and secret instructions.
    """
    notebook_name = f"{activity.task_key}.py"
    notebook_path = f"notebooks/{notebook_name}"
    content = generate_web_activity_notebook(activity, scope=scope)

    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
        "base_parameters": {
            "url": _resolve_param_value(activity.url),
            "method": activity.method,
        },
    }

    notebooks = [
        DabNotebook(
            relative_path=notebook_path,
            content=content,
        )
    ]

    secrets: list[SecretInstruction] = []
    auth = activity.authentication
    if auth:
        scope_name = scope or activity.task_key
        auth_type = auth.get("type", "unknown")
        secrets.append(
            SecretInstruction(
                scope=scope_name,
                key="auth-credential",
                value_source=f"Authentication credential ({auth_type}) for web activity '{activity.name}'",
            )
        )

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets)
