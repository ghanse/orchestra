"""Preparer for LookupActivity -> notebook_task with generated lookup notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook, SecretInstruction
from orchestra.preparer.code_generator import generate_lookup_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import LookupActivity

_DB_SOURCE_TYPES = {
    "AzureSqlSource",
    "SqlServerSource",
    "OracleSource",
    "PostgreSqlSource",
    "MySqlSource",
    "SqlSource",
    "CosmosDbSqlApiSource",
    "SqlDWSource",
}


def prepare(activity: LookupActivity) -> PreparedActivity:
    """Convert a LookupActivity into a notebook_task with a generated lookup notebook.

    Args:
        activity: The translated lookup activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task, generated notebook, and secret instructions.
    """
    notebook_name = f"{activity.task_key}.py"
    notebook_path = f"notebooks/{notebook_name}"
    content = generate_lookup_notebook(activity)

    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
        "base_parameters": {
            "first_row_only": str(activity.first_row_only).lower(),
        },
    }

    notebooks = [
        DabNotebook(
            relative_path=notebook_path,
            content=content,
        )
    ]

    secrets: list[SecretInstruction] = []
    source_type = activity.source_type or ""
    if source_type in _DB_SOURCE_TYPES:
        scope = f"orchestra-{activity.task_key}"
        secrets.append(
            SecretInstruction(
                scope=scope,
                key="jdbc-url",
                value_source=f"JDBC URL for {source_type} lookup in activity '{activity.name}'",
            )
        )
        secrets.append(
            SecretInstruction(
                scope=scope,
                key="jdbc-password",
                value_source=f"JDBC password for {source_type} lookup in activity '{activity.name}'",
            )
        )

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets)
