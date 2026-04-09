"""Preparer for CopyActivity -> notebook_task with generated copy notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook, SecretInstruction
from orchestra.preparer.code_generator import generate_copy_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import CopyActivity

# Source type strings that indicate file-based origins
_FILE_SOURCE_TYPES = {
    "BlobSource",
    "AzureBlobFSSource",
    "AzureDataLakeStoreSource",
    "AmazonS3Source",
    "FileSystemSource",
    "SftpSource",
    "HttpSource",
    "AzureBlobStorageSource",
}

# Source type strings that indicate database origins
_DB_SOURCE_TYPES = {
    "AzureSqlSource",
    "SqlServerSource",
    "OracleSource",
    "PostgreSqlSource",
    "MySqlSource",
    "SqlSource",
    "CosmosDbSqlApiSource",
    "SqlDWSource",
    "AzureSqlDatabaseSource",
}


def prepare(activity: CopyActivity) -> PreparedActivity:
    """Convert a CopyActivity into a notebook_task with a generated copy notebook.

    The ingestion strategy is determined by the source type string:
    - File-based sources use Auto Loader (``cloudFiles``).
    - Database sources use JDBC reads.
    - Unknown sources use a generic Spark read/write.

    Args:
        activity: The translated copy activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task, generated notebook, and secret instructions.
    """
    notebook_name = f"{activity.task_key}.py"
    notebook_path = f"notebooks/{notebook_name}"
    content = generate_copy_notebook(activity)

    task = _build_common_task_fields(activity)
    task["notebook_task"] = {
        "notebook_path": f"../src/{notebook_path}",
        "base_parameters": {},
    }
    if activity.source_type:
        task["notebook_task"]["base_parameters"]["source_type"] = activity.source_type
    if activity.sink_type:
        task["notebook_task"]["base_parameters"]["sink_type"] = activity.sink_type

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
                value_source=f"JDBC URL for {source_type} source in activity '{activity.name}'",
            )
        )
        secrets.append(
            SecretInstruction(
                scope=scope,
                key="jdbc-password",
                value_source=f"JDBC password for {source_type} source in activity '{activity.name}'",
            )
        )
    elif source_type in _FILE_SOURCE_TYPES:
        src_props = activity.source_properties or {}
        if src_props.get("connection_string") or src_props.get("sasUri"):
            scope = f"orchestra-{activity.task_key}"
            secrets.append(
                SecretInstruction(
                    scope=scope,
                    key="connection-string",
                    value_source=f"Connection string for {source_type} source in activity '{activity.name}'",
                )
            )

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets)
