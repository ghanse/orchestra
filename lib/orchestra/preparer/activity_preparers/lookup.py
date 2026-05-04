"""Preparer for LookupActivity -> notebook_task with generated lookup notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import SecretInstruction
from orchestra.models.source_types import JDBC_SOURCE_TYPES
from orchestra.preparer.activity_preparers._helpers import (
    build_notebook_activity_task,
    make_jdbc_secrets,
)
from orchestra.preparer.activity_preparers._naming import notebook_filename
from orchestra.preparer.code_generator import generate_lookup_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from orchestra.models.ir import LookupActivity


def prepare(activity: LookupActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a LookupActivity into a notebook_task with a generated lookup notebook."""
    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_lookup_notebook(activity, scope=scope),
        base_parameters={"first_row_only": str(activity.first_row_only).lower()},
    )

    secrets: list[SecretInstruction] = []
    source_type = activity.source_type or ""
    if source_type in JDBC_SOURCE_TYPES:
        secrets.extend(
            make_jdbc_secrets(
                scope_name=scope or activity.task_key,
                source_type=source_type,
                activity_name=activity.name,
                role="lookup",
            )
        )

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets)
