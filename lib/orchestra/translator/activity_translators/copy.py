"""Translate ADF Copy activities to Databricks CopyActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, CopyActivity, TranslationContext


def _resolve_source_path(activity: AdfActivity, definitions: AdfDefinitions) -> str | None:
    """Resolve the full storage path from the activity's input dataset.

    Combines the dataset's location properties (fileSystem, folderPath) with
    the linked service URL to build a full ABFSS path.

    Args:
        activity: The ADF activity AST node.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        An ``abfss://`` path string, or ``None`` if the path cannot be resolved.
    """
    if not activity.inputs:
        return None
    dataset_ref = activity.inputs[0]
    dataset = definitions.datasets.get(dataset_ref.reference_name)
    if not dataset:
        return None
    props = dataset.properties
    type_props = props.get("typeProperties", {})
    location = type_props.get("location", {})
    file_system = location.get("fileSystem", "")
    folder_path = location.get("folderPath", "")

    # Get storage account from linked service
    ls_name = props.get("linkedServiceName", {})
    if isinstance(ls_name, dict):
        ls_name = ls_name.get("referenceName", "")
    ls = definitions.linked_services.get(ls_name) if ls_name else None
    if ls:
        url = ls.properties.get("typeProperties", {}).get("url", "")
        if url:
            # url is like "https://account.dfs.core.windows.net"
            host = url.replace("https://", "").rstrip("/")
            path = f"abfss://{file_system}@{host}/{folder_path}".rstrip("/")
            return path
    return None


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate a Copy activity.

    Extracts source/sink dataset references, connection properties, and
    column mappings from the ADF type properties.  Also resolves the input
    dataset reference to extract the actual storage path.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing datasets.

    Returns:
        A :class:`CopyActivity` IR node.
    """
    tp = activity.type_properties or {}

    # Source
    source_raw = tp.get("source", {})
    source_type = source_raw.get("type")
    source_properties = {k: v for k, v in source_raw.items() if k != "type"} if source_raw else {}

    # Resolve source path from input dataset
    resolved_path = _resolve_source_path(activity, definitions)
    if resolved_path:
        source_properties["resolved_path"] = resolved_path

    # Sink
    sink_raw = tp.get("sink", {})
    sink_type = sink_raw.get("type")
    sink_properties = {k: v for k, v in sink_raw.items() if k != "type"} if sink_raw else {}

    # Column mapping
    column_mapping: list[dict[str, str]] = []
    translator_raw = tp.get("translator")
    if translator_raw and isinstance(translator_raw, dict):
        mappings = translator_raw.get("mappings", [])
        for mapping in mappings:
            source_col = mapping.get("source", {})
            sink_col = mapping.get("sink", {})
            if source_col and sink_col:
                column_mapping.append(
                    {
                        "source_name": source_col.get("name", ""),
                        "source_type": source_col.get("type", ""),
                        "sink_name": sink_col.get("name", ""),
                        "sink_type": sink_col.get("type", ""),
                    }
                )

    return CopyActivity(
        **base_kwargs,
        source_type=source_type,
        sink_type=sink_type,
        source_properties=source_properties,
        sink_properties=sink_properties,
        column_mapping=column_mapping if column_mapping else None,
    )
