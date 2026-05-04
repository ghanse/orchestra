"""Download workspace artifacts (notebooks, scripts, JARs) via Databricks SDK.

Reads ``~/.databrickscfg`` to discover available profiles.  When multiple
profiles exist, prompts the user to select one (once per session).  Falls
back to placeholder content if the SDK is not available or the workspace is
not reachable.
"""

from __future__ import annotations

import base64
import configparser
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state — resolved once per process, reused across calls
# ---------------------------------------------------------------------------

_resolved_profile: str | None = None
_profile_resolved: bool = False


def _get_databrickscfg_path() -> Path:
    """Return the path to the Databricks CLI config file."""
    override = os.environ.get("DATABRICKS_CONFIG_FILE")
    if override:
        return Path(override)
    return Path.home() / ".databrickscfg"


def _list_profiles() -> list[str]:
    """Parses ``~/.databrickscfg`` and return available profile names.

    Uses ``default_section=None`` so that the ``[DEFAULT]`` section is
    treated as a regular profile (``configparser`` normally hides it).

    Returns:
        Sorted list of profile section names.  Empty list if the file
        does not exist or cannot be parsed.
    """
    cfg_path = _get_databrickscfg_path()
    if not cfg_path.exists():
        return []

    # Use a sentinel default_section so [DEFAULT] is treated as a regular section
    config = configparser.ConfigParser(default_section="__no_default__")
    try:
        config.read(str(cfg_path), encoding="utf-8")
    except configparser.Error:
        logger.warning("Failed to parse %s", cfg_path)
        return []

    return sorted(config.sections())


def _resolve_profile() -> str | None:
    """Determines which ``~/.databrickscfg`` profile to use.

    Resolution order:

    1. ``DATABRICKS_CONFIG_PROFILE`` environment variable.
    2. If exactly one profile exists, use it.
    3. If a ``DEFAULT`` profile exists alongside others, use ``DEFAULT``.
    4. If multiple non-default profiles exist, prompt the user interactively.
    5. If no profiles exist, return ``None`` (SDK will attempt env-var auth).

    The result is cached for the lifetime of the process.

    Returns:
        Profile name string, or ``None`` to let the SDK use its own
        default resolution.
    """
    global _resolved_profile, _profile_resolved  # noqa: PLW0603

    if _profile_resolved:
        return _resolved_profile

    _profile_resolved = True

    # 1. Explicit environment variable
    env_profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if env_profile:
        logger.info("Using profile from DATABRICKS_CONFIG_PROFILE: %s", env_profile)
        _resolved_profile = env_profile
        return _resolved_profile

    profiles = _list_profiles()

    if not profiles:
        # No config file or no sections — let the SDK resolve auth itself
        _resolved_profile = None
        return _resolved_profile

    if len(profiles) == 1:
        # Only one profile — use it
        _resolved_profile = profiles[0]
        logger.info("Using sole .databrickscfg profile: %s", _resolved_profile)
        return _resolved_profile

    # Multiple profiles — check if DEFAULT exists
    if "DEFAULT" in profiles:
        _resolved_profile = "DEFAULT"
        logger.info("Multiple profiles found; using DEFAULT")
        return _resolved_profile

    # Multiple non-default profiles — prompt the user
    _resolved_profile = _prompt_for_profile(profiles)
    return _resolved_profile


def _prompt_for_profile(profiles: list[str]) -> str:
    """Interactively prompt the user to select a profile.

    Args:
        profiles: Available profile names.

    Returns:
        Selected profile name.
    """
    cfg_path = _get_databrickscfg_path()

    # Read the config to show hosts alongside profile names
    config = configparser.ConfigParser(default_section="__no_default__")
    try:
        config.read(str(cfg_path), encoding="utf-8")
    except configparser.Error:
        pass

    print(f"\nMultiple Databricks profiles found in {cfg_path}:")
    for i, name in enumerate(profiles, 1):
        host = config.get(name, "host", fallback="")
        print(f"  [{i}] {name:<30s} {host}")

    while True:
        try:
            choice = input("\nSelect a profile number (or name): ").strip()
        except (EOFError, KeyboardInterrupt):
            # Non-interactive context — fall back to first profile
            print(f"\nNon-interactive; defaulting to '{profiles[0]}'")
            return profiles[0]

        # Accept by number
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(profiles):
                selected = profiles[idx]
                print(f"Using profile: {selected}")
                return selected

        # Accept by name
        if choice in profiles:
            print(f"Using profile: {choice}")
            return choice

        print(f"Invalid selection: '{choice}'. Try again.")


def set_profile(profile: str | None) -> None:
    """Explicitly set the profile to use, bypassing auto-resolution.

    Useful when the caller (e.g. a skill) has already determined the
    correct profile from user interaction.

    Args:
        profile: Profile name, or ``None`` to reset to auto-resolution.
    """
    global _resolved_profile, _profile_resolved  # noqa: PLW0603
    _resolved_profile = profile
    _profile_resolved = profile is not None


def _get_workspace_client():
    """Return a ``WorkspaceClient`` configured with the resolved profile.

    Returns:
        A ``WorkspaceClient`` instance.

    Raises:
        ImportError: If ``databricks-sdk`` is not installed.
    """
    from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]

    profile = _resolve_profile()
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# Public download API
# ---------------------------------------------------------------------------


def download_notebook(workspace_path: str) -> str | None:
    """Download a notebook from Databricks workspace.

    Uses the resolved ``~/.databrickscfg`` profile (prompting the user
    to select one if multiple profiles exist).

    Args:
        workspace_path: Workspace path (e.g., ``"/Shared/orchestra/transform"``).

    Returns:
        Notebook source code as a string, or ``None`` if download failed.
    """
    try:
        from databricks.sdk.service.workspace import ExportFormat  # type: ignore[import-not-found]

        w = _get_workspace_client()
        response = w.workspace.export(path=workspace_path, format=ExportFormat.SOURCE)
        if response.content:
            return base64.b64decode(response.content).decode("utf-8")
    except ImportError:
        logger.info("databricks-sdk not installed; skipping notebook download for %s", workspace_path)
    except Exception as e:
        logger.warning("Failed to download notebook %s: %s", workspace_path, e)
    return None


def download_dbfs_file(dbfs_path: str) -> bytes | None:
    """Download a file from DBFS.

    Uses the resolved ``~/.databrickscfg`` profile (prompting the user
    to select one if multiple profiles exist).

    Args:
        dbfs_path: DBFS path (e.g., ``"dbfs:/scripts/process.py"`` or
            ``"dbfs:/jars/app.jar"``).

    Returns:
        File content as bytes, or ``None`` if download failed.
    """
    try:
        w = _get_workspace_client()
        # Strip "dbfs:" prefix for the SDK call
        path = dbfs_path.replace("dbfs:", "", 1)
        with w.dbfs.open(path, read=True) as f:
            return f.read()
    except ImportError:
        logger.info("databricks-sdk not installed; skipping DBFS download for %s", dbfs_path)
    except Exception as e:
        logger.warning("Failed to download DBFS file %s: %s", dbfs_path, e)
    return None
