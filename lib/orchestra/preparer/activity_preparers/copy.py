"""Preparer for CopyActivity -> notebook_task with generated copy notebook."""

from __future__ import annotations

import re

from orchestra.models.dab import DabNotebook, SecretInstruction, SetupTask
from orchestra.models.ir import CopyActivity
from orchestra.preparer.code_generator import generate_copy_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields

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

    src_props = activity.source_properties or {}
    source_type = activity.source_type or ""

    # Pre-compute the UC volume path so the code generator can embed it in
    # the notebook as the default source_path instead of the raw abfss:// URL.
    volume_name: str | None = None
    volume_path: str | None = None
    abfss_url: str | None = None
    if source_type in _FILE_SOURCE_TYPES:
        vol_info = _extract_volume_info(src_props)
        if vol_info:
            volume_name = _make_volume_name(activity.task_key)
            abfss_url = f"abfss://{vol_info['container']}@{vol_info['storage_account']}"
            if vol_info["path"]:
                abfss_url += f"/{vol_info['path']}"
            volume_path = f"/Volumes/${{var.catalog}}/${{var.schema}}/{volume_name}"
            # Store the volume path in source_properties so the code generator
            # can use it as the default source_path in the Auto Loader notebook.
            src_props = {**src_props, "volume_path": volume_path}
            activity = CopyActivity(
                name=activity.name,
                task_key=activity.task_key,
                description=activity.description,
                timeout_seconds=activity.timeout_seconds,
                max_retries=activity.max_retries,
                min_retry_interval_millis=activity.min_retry_interval_millis,
                depends_on=activity.depends_on,
                cluster=activity.cluster,
                source_type=activity.source_type,
                sink_type=activity.sink_type,
                source_properties=src_props,
                sink_properties=activity.sink_properties,
                column_mapping=activity.column_mapping,
            )

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

    # Pass through the source path as a parameter: prefer the volume path,
    # fall back to the resolved abfss:// path.
    if volume_path:
        task["notebook_task"]["base_parameters"]["source_path"] = volume_path
    else:
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
        if volume_name and abfss_url:
            setup_tasks.append(
                SetupTask(
                    type="volume",
                    config={
                        "volume_name": volume_name,
                        "volume_type": "EXTERNAL",
                        "location": abfss_url,
                    },
                )
            )

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets, setup_tasks=setup_tasks)
