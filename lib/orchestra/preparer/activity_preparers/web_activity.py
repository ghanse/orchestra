"""Preparer for WebActivity -> notebook_task with generated HTTP notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import SecretInstruction
from orchestra.preparer.activity_preparers._helpers import (
    build_notebook_task_artifacts,
    resolve_param_value,
)
from orchestra.preparer.activity_preparers._naming import notebook_filename
from orchestra.preparer.code_generator import generate_web_activity_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import WebActivity


def prepare(activity: WebActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a WebActivity into a notebook_task with a generated HTTP notebook.

    Args:
        activity: The translated web activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task, generated notebook, and secret instructions.
    """
    notebook_relative_path = f"notebooks/{notebook_filename(activity.task_key, activity.name)}"
    content = generate_web_activity_notebook(activity, scope=scope)

    task = _build_common_task_fields(activity)
    task["notebook_task"], notebooks = build_notebook_task_artifacts(
        notebook_relative_path=notebook_relative_path,
        notebook_content=content,
        base_parameters={
            "url": resolve_param_value(activity.url),
            "method": resolve_param_value(activity.method),
        },
    )

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
