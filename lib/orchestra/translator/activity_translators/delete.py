"""Translate ADF Delete activities to Databricks DeleteActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, DeleteActivity, TranslationContext
from orchestra.translator.activity_translators._resolve import resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate a Delete activity.

    Extracts dataset reference, recursive flag, and wildcard patterns.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing datasets.

    Returns:
        A :class:`DeleteActivity` IR node.
    """
    tp = activity.type_properties or {}

    # Dataset reference from inputs
    dataset_name = ""
    if activity.inputs:
        dataset_name = activity.inputs[0].reference_name

    # Properties from typeProperties
    recursive = tp.get("recursive", True)
    folder_path_raw = tp.get("dataset", {}).get("folderPath") if isinstance(tp.get("dataset"), dict) else None
    folder_path = resolve_field(folder_path_raw, context) if folder_path_raw is not None else None

    # Also check storeSettings for wildcard paths
    store_settings = tp.get("storeSettings", {})
    wildcard_folder_path_raw = store_settings.get("wildcardFolderPath")
    wildcard_folder_path = resolve_field(wildcard_folder_path_raw, context) if wildcard_folder_path_raw is not None else None

    # Build folder path from available data
    effective_folder = folder_path or wildcard_folder_path

    return DeleteActivity(
        **base_kwargs,
        dataset_name=dataset_name,
        folder_path=effective_folder,
        recursive=recursive,
    )
