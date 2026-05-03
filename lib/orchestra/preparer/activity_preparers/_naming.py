"""Shared naming helpers used by every activity preparer.

The deterministic translators keep ``task_key`` aligned with the original ADF
activity name (often PascalCase, e.g. ``BronzeIngest``).  Generated notebooks
that orchestra emits into ``src/notebooks/`` are filesystem artifacts and use
snake_case (``bronze_ingest.py``) so they read naturally in a Databricks
workspace and a Git tree.
"""

from __future__ import annotations

import re


def to_snake_case(name: str) -> str:
    """Convert PascalCase / camelCase / mixed identifiers to snake_case.

    Examples:
        ``BronzeIngest`` -> ``bronze_ingest``
        ``copySQLToBlob`` -> ``copy_sql_to_blob``
        ``ETL_Main`` -> ``etl_main``
        ``spaces and dashes-here`` -> ``spaces_and_dashes_here``
    """
    # Split between a lower/digit and an upper to handle camelCase boundaries.
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    s2 = re.sub(r"[^a-zA-Z0-9_]+", "_", s2)
    s2 = re.sub(r"_+", "_", s2).strip("_")
    return s2.lower()


def notebook_filename(task_key: str, activity_name: str | None = None) -> str:
    """Derive a snake_case ``<name>.py`` filename for a generated notebook.

    Prefer the ADF activity name (which carries human intent) when supplied;
    fall back to the task key.  Always returns lower-snake-case.
    """
    source = activity_name or task_key
    snake = to_snake_case(source)
    if not snake:
        snake = task_key.lower() or "notebook"
    return f"{snake}.py"
