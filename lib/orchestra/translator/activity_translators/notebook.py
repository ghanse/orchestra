"""Translate ADF DatabricksNotebook activities to Databricks NotebookActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, NotebookActivity, TranslationContext
from orchestra.translator.activity_translators._resolve import resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translate a DatabricksNotebook activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`NotebookActivity` IR node.
    """
    tp = activity.type_properties or {}

    notebook_path = resolve_field(tp.get("notebookPath", ""), context)
    base_parameters = tp.get("baseParameters") or {}

    return NotebookActivity(
        **base_kwargs,
        notebook_path=notebook_path,
        base_parameters=base_parameters,
    )
