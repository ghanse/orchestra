"""Translate ADF Copy activities to Databricks CopyActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, CopyActivity, TranslationContext


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate a Copy activity.

    Extracts source/sink dataset references, connection properties, and
    column mappings from the ADF type properties.

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
