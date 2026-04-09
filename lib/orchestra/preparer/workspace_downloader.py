"""Download workspace artifacts (notebooks, scripts, JARs) via Databricks SDK.

Falls back to placeholder content if the SDK is not available or the
workspace is not reachable.
"""

from __future__ import annotations

import base64
import logging

logger = logging.getLogger(__name__)


def download_notebook(workspace_path: str) -> str | None:
    """Download a notebook from Databricks workspace.

    Args:
        workspace_path: Workspace path (e.g., ``"/Shared/orchestra/transform"``).

    Returns:
        Notebook source code as a string, or ``None`` if download failed.
    """
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]

        w = WorkspaceClient()
        response = w.workspace.export(path=workspace_path, format="SOURCE")
        if response.content:
            return base64.b64decode(response.content).decode("utf-8")
    except ImportError:
        logger.info("databricks-sdk not installed; skipping notebook download for %s", workspace_path)
    except Exception as e:
        logger.warning("Failed to download notebook %s: %s", workspace_path, e)
    return None


def download_dbfs_file(dbfs_path: str) -> bytes | None:
    """Download a file from DBFS.

    Args:
        dbfs_path: DBFS path (e.g., ``"dbfs:/scripts/process.py"`` or
            ``"dbfs:/jars/app.jar"``).

    Returns:
        File content as bytes, or ``None`` if download failed.
    """
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]

        w = WorkspaceClient()
        # Strip "dbfs:" prefix for the SDK call
        path = dbfs_path.replace("dbfs:", "", 1)
        with w.dbfs.open(path, read=True) as f:
            return f.read()
    except ImportError:
        logger.info("databricks-sdk not installed; skipping DBFS download for %s", dbfs_path)
    except Exception as e:
        logger.warning("Failed to download DBFS file %s: %s", dbfs_path, e)
    return None
