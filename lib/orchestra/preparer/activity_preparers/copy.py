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
    """Extract storage account, container, and folder path from a resolved ABFSS URL.

    The volume is created at the **container level** so that multiple activities
    sharing the same storage container reuse a single external volume.  The
    ``folder_path`` is preserved so callers can read from
    ``/Volumes/catalog/schema/<volume>/<folder_path>``.

    Args:
        source_properties: The source_properties dict from the CopyActivity.

    Returns:
        Dict with ``storage_account``, ``container``, ``folder_path``, and
        ``volume_name`` keys, or ``None``.
    """
    resolved_path = source_properties.get("resolved_path", "")
    if not resolved_path:
        return None

    # Parse abfss://container@account.dfs.core.windows.net/folder/path
    m = re.match(r"abfss://([^@]+)@([^/]+)/?(.*)", resolved_path)
    if not m:
        return None

    container = m.group(1)
    storage_account = m.group(2)
    folder_path = m.group(3).rstrip("/")

    # Volume name derived from the container — one volume per container.
    volume_name = re.sub(r"[^a-zA-Z0-9_]", "_", container)
    return {
        "storage_account": storage_account,
        "container": container,
        "folder_path": folder_path,
        "volume_name": volume_name,
    }


def prepare(activity: CopyActivity, *, scope: str = "") -> PreparedActivity:
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
    from orchestra.preparer.activity_preparers._naming import notebook_filename
    notebook_name = notebook_filename(activity.task_key, activity.name)
    notebook_path = f"notebooks/{notebook_name}"

    src_props = activity.source_properties or {}
    source_type = activity.source_type or ""

    # Pre-compute the UC volume path so the code generator can embed it in
    # the notebook as the default source_path instead of the raw abfss:// URL.
    #
    # Volumes are created at the *container level* so multiple Copy activities
    # that read from the same container share a single external volume.  The
    # folder path within the container becomes a subdirectory under the volume
    # mount (e.g., /Volumes/catalog/schema/adf_export/raw/customers).
    volume_name: str | None = None
    volume_source_path: str | None = None
    volume_base: str | None = None
    container_location: str | None = None
    if source_type in _FILE_SOURCE_TYPES:
        vol_info = _extract_volume_info(src_props)
        if vol_info:
            volume_name = vol_info["volume_name"]
            folder_path = vol_info["folder_path"]
            container_location = f"abfss://{vol_info['container']}@{vol_info['storage_account']}"
            volume_base = f"/Volumes/${{var.catalog}}/${{var.schema}}/{volume_name}"
            # Source path = volume root + subfolder from the dataset
            volume_source_path = f"{volume_base}/{folder_path}" if folder_path else volume_base
            # Store paths in source_properties for the code generator
            src_props = {
                **src_props,
                "volume_path": volume_source_path,
                "volume_base": volume_base,
            }
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

    content = generate_copy_notebook(activity, scope=scope)

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
    if volume_source_path:
        task["notebook_task"]["base_parameters"]["source_path"] = volume_source_path
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
        scope_name = scope or activity.task_key
        secrets.append(
            SecretInstruction(
                scope=scope_name,
                key="jdbc-url",
                value_source=f"JDBC URL for {source_type} source in activity '{activity.name}'",
            )
        )
        secrets.append(
            SecretInstruction(
                scope=scope_name,
                key="jdbc-password",
                value_source=f"JDBC password for {source_type} source in activity '{activity.name}'",
            )
        )
    elif source_type in _FILE_SOURCE_TYPES:
        if src_props.get("connection_string") or src_props.get("sasUri"):
            scope_name = scope or activity.task_key
            secrets.append(
                SecretInstruction(
                    scope=scope_name,
                    key="connection-string",
                    value_source=f"Connection string for {source_type} source in activity '{activity.name}'",
                )
            )

        # Create UC external volume setup task at the container level.
        # Multiple activities sharing the same container reuse this volume.
        if volume_name and container_location:
            setup_tasks.append(
                SetupTask(
                    type="volume",
                    config={
                        "volume_name": volume_name,
                        "volume_type": "EXTERNAL",
                        "location": container_location,
                    },
                )
            )

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets, setup_tasks=setup_tasks)
