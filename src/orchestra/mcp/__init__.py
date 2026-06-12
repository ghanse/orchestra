"""MCP packaging for orchestra.

Exposes the orchestra migration phases (profile, translate, prepare) and the
adapter helper operations as Model Context Protocol tools. The tool layer is a
thin bridge over the existing, tested ``python -m orchestra.adapter`` entry
point so the phase contracts are reused verbatim rather than duplicated.

Run locally over stdio (for Claude Code / Claude Desktop)::

    python -m orchestra.mcp

Run as a streamable-HTTP server (for hosting on Databricks Apps / Genie Code)::

    python -m orchestra.mcp --http --port 8000

``build_server`` / ``build_http_app`` require the ``mcp`` extra
(``pip install -e .[mcp]``); they are imported lazily so :mod:`orchestra.mcp.runner`
stays usable without it.
"""

from typing import Any

__all__ = ["build_server", "build_http_app"]


def __getattr__(name: str) -> Any:
    if name in ("build_server", "build_http_app"):
        from orchestra.mcp import server

        return getattr(server, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
