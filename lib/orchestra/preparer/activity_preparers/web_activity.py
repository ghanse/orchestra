"""Preparer for WebActivity -> notebook_task with generated HTTP notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook, SecretInstruction
from orchestra.preparer.code_generator import generate_web_activity_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import WebActivity


def prepare(activity: WebActivity) -> PreparedActivity:
    """Convert a WebActivity into a notebook_task with a generated HTTP notebook.

    Args:
        activity: The translated web activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task, generated notebook, and secret instructions.
    """
    notebook_name = f"{activity.task_key}.py"
    notebook_path = f"notebooks/{notebook_name}"
    content = generate_web_activity_notebook(activity)

    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
        "base_parameters": {
            "url": activity.url,
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
        scope = f"orchestra-{activity.task_key}"
        auth_type = auth.get("type", "unknown")
        secrets.append(
            SecretInstruction(
                scope=scope,
                key="auth-credential",
                value_source=f"Authentication credential ({auth_type}) for web activity '{activity.name}'",
            )
        )

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets)
