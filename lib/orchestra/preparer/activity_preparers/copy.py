"""Preparer for CopyActivity -> notebook_task with generated copy notebook."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook, SecretInstruction, SetupTask
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
    "DelimitedTextSource",
    "JsonSource",
    "ParquetSource",
    "AvroSource",
    "OrcSource",
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


def _extract_volume_info(source_properties: dict) -> dict | None:
    """Extract storage account, container, and path from source properties for volume creation.

    Args:
        source_properties: The source_properties dict from the CopyActivity.

    Returns:
        Dict with ``storage_account``, ``container``, and ``path`` keys, or ``None``.
    """
    resolved_path = source_properties.get("resolved_path", "")
    if not resolved_path:
        return None

    # Parse abfss://container@account.dfs.core.windows.net/path
    m = re.match(r"abfss://([^@]+)@([^/]+)/?(.*)", resolved_path)
    if m:
        container = m.group(1)
        storage_account = m.group(2)
        path = m.group(3)
        return {
            "storage_account": storage_account,
            "container": container,
            "path": path,
        }
    return None


def _make_volume_name(task_key: str) -> str:
    """Create a sanitized volume name from the task key.

    Args:
        task_key: The task key to derive a volume name from.

    Returns:
        A sanitized volume name string.
    """
    name = re.sub(r"[^a-zA-Z0-9_]", "_", task_key)
    return f"vol_{name}"


def prepare(activity: CopyActivity) -> PreparedActivity:
    """Convert a CopyActivity into a notebook_task with a generated copy notebook.

    The ingestion strategy is determined by the source type string:
    - File-based sources use Auto Loader (``cloudFiles``).
    - Database sources use JDBC reads.
    - Unknown sources use a generic Spark read/write.

    For file-based sources with a resolved ABFSS path, a UC external volume
    setup task is also created.

    Args:
        activity: The translated copy activity from the IR.

    Returns:
        A PreparedActivity with the notebook_task, generated notebook, secret
        instructions, and setup tasks.
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

    # Pass through the resolved source path as a parameter
    src_props = activity.source_properties or {}
    resolved_path = src_props.get("resolved_path")
    if resolved_path:
        task["notebook_task"]["base_parameters"]["source_path"] = resolved_path

    notebooks = [
        DabNotebook(
            relative_path=notebook_path,
            content=content,
        )
    ]

    secrets: list[SecretInstruction] = []
    setup_tasks: list[SetupTask] = []
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
        if src_props.get("connection_string") or src_props.get("sasUri"):
            scope = f"orchestra-{activity.task_key}"
            secrets.append(
                SecretInstruction(
                    scope=scope,
                    key="connection-string",
                    value_source=f"Connection string for {source_type} source in activity '{activity.name}'",
                )
            )

        # Create UC external volume setup task for file-based sources
        vol_info = _extract_volume_info(src_props)
        if vol_info:
            volume_name = _make_volume_name(activity.task_key)
            abfss_base = f"abfss://{vol_info['container']}@{vol_info['storage_account']}"
            if vol_info["path"]:
                abfss_base += f"/{vol_info['path']}"
            setup_tasks.append(
                SetupTask(
                    type="volume",
                    config={
                        "volume_name": volume_name,
                        "storage_account": vol_info["storage_account"],
                        "container": vol_info["container"],
                        "path": vol_info["path"],
                        "sql": (
                            f"CREATE EXTERNAL VOLUME IF NOT EXISTS "
                            f"${{var.catalog}}.${{var.schema}}.{volume_name}\n"
                            f"LOCATION '{abfss_base}'"
                        ),
                        "volume_path": (f"/Volumes/${{var.catalog}}/${{var.schema}}/{volume_name}"),
                    },
                )
            )
            # Also set the volume path as a parameter
            task["notebook_task"]["base_parameters"]["volume_path"] = (
                f"/Volumes/${{var.catalog}}/${{var.schema}}/{volume_name}"
            )

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets, setup_tasks=setup_tasks)
