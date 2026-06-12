"""Subprocess bridge between MCP tools and the orchestra adapter CLI.

Every MCP tool shells out to ``python -m orchestra.adapter`` — the same unified
entry point the agent skills already use — then reads back the JSON/CSV
artifacts each phase writes. This reuses the tested phase contracts instead of
re-implementing their logic, so the MCP surface stays in lockstep with the CLI.
"""

from __future__ import annotations

import csv
import io
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Phases can take a while on large factories; allow generous default headroom.
DEFAULT_TIMEOUT = int(os.environ.get("ORCHESTRA_MCP_TIMEOUT", "1800"))


@dataclass
class AdapterResult:
    """Outcome of a single ``orchestra.adapter`` invocation."""

    command: list[str]
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def as_dict(self) -> dict[str, Any]:
        """Serialise the raw process outcome for inclusion in a tool result."""
        return {
            "command": " ".join(self.command),
            "ok": self.ok,
            "returncode": self.returncode,
            "stdout": self.stdout.strip(),
            "stderr": self.stderr.strip(),
        }


def run_adapter(args: list[Any], *, cwd: str | Path | None = None, timeout: int = DEFAULT_TIMEOUT) -> AdapterResult:
    """Invoke ``python -m orchestra.adapter`` with the supplied arguments.

    Args:
        args: Adapter subcommand and flags (each item is stringified).
        cwd: Working directory for the subprocess. Defaults to the current one.
        timeout: Seconds before the subprocess is killed.

    Returns:
        The captured :class:`AdapterResult`.
    """
    command = [sys.executable, "-m", "orchestra.adapter", *[str(arg) for arg in args]]
    proc = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return AdapterResult(command=command, returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)


def read_json(path: Path) -> Any | None:
    """Return parsed JSON at *path*, or ``None`` when the file is absent/invalid."""
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def read_text(path: Path) -> str | None:
    """Return text at *path*, or ``None`` when it cannot be read."""
    try:
        return path.read_text()
    except OSError:
        return None


def parse_stdout_json(result: AdapterResult) -> Any | None:
    """Parse the adapter's stdout as JSON (used by inspect/inputs/workspace-paths)."""
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def list_tree(root: Path, *, max_entries: int = 250) -> list[str]:
    """Return repo-relative paths of files under *root* (sorted, capped)."""
    if not root.exists():
        return []
    files = sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())
    return files[:max_entries]


def summarize_inventory(output_dir: Path) -> dict[str, Any] | None:
    """Summarise ``metadata/inventory.json`` produced by the profile phase.

    The profile phase writes a ready-made ``summary`` block (pipeline/activity
    counts by strategy plus coverage); surface it directly when present.
    """
    inventory = read_json(output_dir / "metadata" / "inventory.json")
    if not isinstance(inventory, dict):
        return None

    summary = inventory.get("summary")
    if isinstance(summary, dict):
        return summary

    pipelines = inventory.get("pipelines") or []
    return {"pipeline_count": len(pipelines)}


def summarize_translation(output_dir: Path) -> dict[str, Any] | None:
    """Summarise the translation report (transient under ``.work/``)."""
    for candidate in (".work/translation_report.json", "translation_report.json"):
        report = read_json(output_dir / candidate)
        if isinstance(report, dict):
            break
    else:
        return None

    pipelines = report.get("pipelines") or []
    statuses: dict[str, int] = {}
    for pipeline in pipelines:
        for task in pipeline.get("tasks", []):
            status = str(task.get("status", "translated")).lower()
            statuses[status] = statuses.get(status, 0) + 1
    return {"pipelines": len(pipelines), "task_status_counts": statuses}


def materialize_lookup_rows(source: str) -> list[dict[str, str]]:
    """Parse a CSV file path or literal CSV string into a list of row dicts.

    Mirrors the adapter's ``materialize-lookup`` parsing so the MCP tool can
    validate input without a second subprocess hop.
    """
    text = Path(source).read_text() if Path(source).exists() else source
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]
