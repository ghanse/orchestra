"""Preparer for CopyActivity -> notebook_task with generated copy notebook."""

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass

from orchestra.models.dab import SecretInstruction, SetupTask
from orchestra.models.ir import CopyActivity
from orchestra.models.source_types import FILE_SOURCE_TYPES, JDBC_SOURCE_TYPES
from orchestra.preparer.activity_preparers._helpers import (
    build_notebook_task_artifacts,
    make_jdbc_secrets,
)
from orchestra.preparer.activity_preparers._naming import notebook_filename
from orchestra.preparer.code_generator import generate_copy_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, _build_common_task_fields


_ABFSS_URL_RE = re.compile(r"abfss://([^@]+)@([^/]+)/?(.*)")
_VOLUME_NAME_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]")


@dataclass(frozen=True, slots=True)
class _VolumeBinding:
    """Resolved UC volume info derived from a file source's resolved ABFSS URL.

    Attributes:
        volume_name: UC volume name (sanitised from the container).
        container_location: ``abfss://<container>@<storage_account>`` -- used
            as the EXTERNAL LOCATION URL for the volume DDL.
        volume_base: ``/Volumes/${var.catalog}/${var.schema}/<volume>``.
        source_path: ``<volume_base>/<folder>`` (or ``<volume_base>`` when
            the dataset has no folder), used as the notebook's source_path.
    """

    volume_name: str
    container_location: str
    volume_base: str
    source_path: str


def _resolve_volume_binding(source_properties: dict) -> _VolumeBinding | None:
    """Derive UC volume info from a file source's resolved ABFSS URL.

    The volume is created at the **container level** so multiple Copy
    activities sharing the same container reuse a single external volume;
    the folder portion of the URL becomes a subdirectory under the volume.
    Returns ``None`` when the source has no resolved abfss:// path.
    """
    resolved_path = source_properties.get("resolved_path", "")
    if not resolved_path:
        return None

    match = _ABFSS_URL_RE.match(resolved_path)
    if not match:
        return None

    container, storage_account, folder_path = match.group(1), match.group(2), match.group(3).rstrip("/")
    volume_name = _VOLUME_NAME_SANITIZE_RE.sub("_", container)
    volume_base = f"/Volumes/${{var.catalog}}/${{var.schema}}/{volume_name}"
    return _VolumeBinding(
        volume_name=volume_name,
        container_location=f"abfss://{container}@{storage_account}",
        volume_base=volume_base,
        source_path=f"{volume_base}/{folder_path}" if folder_path else volume_base,
    )


def _augment_with_volume_paths(activity: CopyActivity, binding: _VolumeBinding) -> CopyActivity:
    """Return a copy of *activity* with volume paths threaded into source_properties."""
    augmented_properties = {
        **(activity.source_properties or {}),
        "volume_path": binding.source_path,
        "volume_base": binding.volume_base,
    }
    return dataclasses.replace(activity, source_properties=augmented_properties)


def prepare(activity: CopyActivity, *, scope: str = "") -> PreparedActivity:
    """Convert a CopyActivity into a notebook_task with a generated copy notebook.

    The ingestion strategy is determined by the source type string:
    - File-based sources use Auto Loader (``cloudFiles``).
    - Database sources use JDBC reads.
    - Unknown sources use a generic Spark read/write.

    For file-based sources with a resolved ABFSS path, a UC external volume
    setup task is also created.
    """
    source_type = activity.source_type or ""
    volume_binding: _VolumeBinding | None = None
    if source_type in FILE_SOURCE_TYPES:
        volume_binding = _resolve_volume_binding(activity.source_properties or {})
        if volume_binding is not None:
            activity = _augment_with_volume_paths(activity, volume_binding)

    notebook_relative_path = f"notebooks/{notebook_filename(activity.task_key, activity.name)}"
    notebook_content = generate_copy_notebook(activity, scope=scope)

    base_parameters: dict[str, str] = {}
    if activity.source_type:
        base_parameters["source_type"] = activity.source_type
    if activity.sink_type:
        base_parameters["sink_type"] = activity.sink_type
    # Prefer the resolved volume path; fall back to the raw abfss:// URL.
    source_path = (
        volume_binding.source_path
        if volume_binding is not None
        else (activity.source_properties or {}).get("resolved_path")
    )
    if source_path:
        base_parameters["source_path"] = source_path

    task = _build_common_task_fields(activity)
    task["notebook_task"], notebooks = build_notebook_task_artifacts(
        notebook_relative_path=notebook_relative_path,
        notebook_content=notebook_content,
        base_parameters=base_parameters,
    )

    scope_name = scope or activity.task_key
    secrets = _build_secrets(activity, source_type, scope_name)
    setup_tasks = _build_setup_tasks(activity, source_type, volume_binding)

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets, setup_tasks=setup_tasks)


def _build_secrets(
    activity: CopyActivity, source_type: str, scope_name: str
) -> list[SecretInstruction]:
    """Return the SecretInstructions a Copy activity needs to deploy."""
    if source_type in JDBC_SOURCE_TYPES:
        return make_jdbc_secrets(
            scope_name=scope_name,
            source_type=source_type,
            activity_name=activity.name,
            role="source",
        )
    if source_type not in FILE_SOURCE_TYPES:
        return []
    source_properties = activity.source_properties or {}
    if not (source_properties.get("connection_string") or source_properties.get("sasUri")):
        return []
    return [
        SecretInstruction(
            scope=scope_name,
            key="connection-string",
            value_source=f"Connection string for {source_type} source in activity '{activity.name}'",
        )
    ]


def _build_setup_tasks(
    activity: CopyActivity,
    source_type: str,
    volume_binding: _VolumeBinding | None,
) -> list[SetupTask]:
    """Return the SetupTasks (UC volumes etc.) a Copy activity needs."""
    if source_type not in FILE_SOURCE_TYPES or volume_binding is None:
        return []
    return [
        SetupTask(
            type="volume",
            config={
                "volume_name": volume_binding.volume_name,
                "volume_type": "EXTERNAL",
                "location": volume_binding.container_location,
            },
        )
    ]
